import re
from threading import RLock, Condition
from types import MethodType

from zmq import PUB, SUB, SUBSCRIBE, POLLIN, DEALER, ROUTER  # pylint: disable-msg=E0611

import dcamp.types.messages.configuration as config
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import ServiceMixin
from dcamp.types.topo import TopoTreeMixin, TopoNode
from dcamp.util.functions import now_secs, now_msecs
from dcamp.types.config_file import ConfigFileMixin


class Configuration(ServiceMixin):

    # states
    STATE_SYNC = 0
    STATE_GOGO = 1

    def __init__(
            self,
            control_pipe,  # control pipe for shutting down service
            local_ep,  # this is us
            local_uuid,  # this is also us
            config_svc,  # obviously, this must be None
            parent_ep,  # from where we receive config updates/snapshots
            level,
            group,
            sos_func,  # call when our parent stops sending HUGZ
            config_state=None,  # should only be given for root level
    ):
        ServiceMixin.__init__(self, control_pipe, local_ep, local_uuid, config_svc)
        assert config_svc is None

        assert isinstance(parent_ep, (EndpntSpec, type(None)))
        self.parent = parent_ep

        assert level in ['root', 'branch', 'leaf']
        self.level = level

        self.group = group

        if self.level in ['branch', 'leaf']:
            assert isinstance(sos_func, MethodType)
        else:
            assert sos_func is None
        self.sos = sos_func

        # Access to __kvdict, __kv_seq, and __tree are protected by __kvlock. There are multiple
        # writers (both the Management and Configuration services can write at the same time) and
        # multiple readers (all services, snapshots requests, etc).
        #
        # When reading, the Lock only needs to be held while reading multiple values (i.e.
        # iterating across the entire dictionary or walking the tree). Getting individual elements
        # should not require the Lock.

        # { key : ( value, seq-num ) }
        self.__kvlock = RLock()  # reentrant lock because I'm lazy
        self.__kvdict = {}
        self.__kv_seq = -1

        # 1) tree starts empty
        # 2) as config receives /TOPO keys, add nodes to tree as topo-node
        # tree.nodes contains { EndpntSpec: TopoNode }
        # TopoNode contains (endpoint, role, group, parent, children, last-seen)
        # topo keys come from tree.get_topo_key(node)

        # topo changes SUB'ed from parent are mimicked in __tree and PUB'ed to children; topo
        # changes by local services are applied to tree and then all generated key-value
        # updates are PUB'ed to children all at once.
        self.__tree = TopoTreeMixin()

        ### Common Members

        # external callers wait on this condition until state==GOGO
        self.state_cond = Condition()
        self.state = Configuration.STATE_SYNC

        ### Root/Branch Members

        # sending hearbeats to children
        self.hug_msg = None
        self.next_hug = None

        ### Branch/Leaf Members

        # detecting parent heartbeats
        self.next_sos = None

        # receiving snapshots
        # { topic : final-seq-num }
        self.kvsync_completed = {}
        self.pending_updates = []

        # TODO: fix timing issue w.r.t. root node sync--root needs to be sync'ed before any other node.

        # receiving updates
        self.topics = []
        self.topics.append('/TOPO/root')  # must be first item sync'ed for all nodes
        if 'leaf' == self.level:
            assert self.group is not None
            self.topics.append('/CONFIG/global/')
            self.topics.append('/CONFIG/%s/' % self.group)
        elif 'branch' == self.level:
            self.topics.append('/')  # collectors get everything

        ### let's get it started

        # sockets and message counts
        (self.update_sub, self.update_pub, self.kvsync_req, self.kvsync_rep) = (None, None, None, None)
        (self.subcnt, self.pubcnt, self.hugcnt, self.reqcnt, self.repcnt) = (0, 0, 0, 0, 0)

        # populate dict with config_file values if root level; values are used later
        if 'root' == self.level:
            if isinstance(config_state, str):
                self.__init_kvdict_from_file(config_state)
            else:
                assert isinstance(config_state, dict)
                self.__init_kvdict_from_dict(config_state)

            # set GOGO state; this basically means we have all the config values
            self.__set_gogo()
            self.__init_producer_sockets()
        else:
            self.__init_consumer_sockets()

    #####
    #  BEGIN dictionary access methods

    def copy_kvdict(self):
        assert 'root' == self.level
        with self.__kvlock:
            return self.__kvdict.copy()  # shallow copy; kvdict values should not be modified
                                         # after this call

    def get(self, k, default=None):
        """ returns (value, seq-id) """
        return self.__kvdict.get(k, (default, -1))

    def __getitem__(self, k):
        (val, seq) = self.__kvdict[k]
        return val

    def __setitem__(self, k, v):
        assert 'root' == self.level, "only root level allowed to make modifications"
        self.__kvlist_store_and_pub([(k, v)])  # add to our dict and publish update

    def __delitem__(self, k):
        assert 'root' == self.level, "only root level allowed to make modifications"
        self.__kvlist_store_and_pub([(k, None)])  # remove from our dict and publish update

    def __len__(self):
        return len(self.__kvdict)

    def __iter__(self):
        return iter(self.__kvdict)

    def __kvlist_store_and_pub(self, kvlist, ignore_seq=False, skip_topo=False):
        pub_kvlist = []
        with self.__kvlock:
            for item in kvlist:
                # for convenience, list contains CONFIG messages or 3-tuples
                if isinstance(item, config.CONFIG):
                    (k, v, seq) = (item.key, item.value, item.sequence)
                else:
                    assert isinstance(item, tuple)
                    if 3 == len(item):
                        (k, v, seq) = item
                    elif 2 == len(item):
                        (k, v) = item
                        seq = None
                    else:
                        raise NotImplementedError('unknown tuple length')

                if seq is None:
                    assert not ignore_seq
                    seq = self.__kv_seq + 1

                # write to dict
                if self.__kv_write(k, v, seq, ignore_seq):
                    # if successful, pub later
                    pub_kvlist.append((k, v, seq))

        for (k, v, seq) in pub_kvlist:
            # pass all topo updates to tree; if update is actually coming from
            # tree, skip_topo should be True
            if not skip_topo and k.startswith('/TOPO'):
                self.__tree.kv_update(k, v)

            # wait until finished with sync state before sending updates
            if self._is_gogo():
                assert self.update_pub is not None
                update = config.KVPUB(k, v, seq)
                update.send(self.update_pub)
                self.__hb_sent()
                self.pubcnt += 1

    def __kv_write(self, key, value, sequence, ignore_seq):
        """ N.B.: self.__kvlock MUST be held when calling __kv_write() """

        if ignore_seq:
            # during kvsync, we allow out of sequence updates; otherwise,
            assert self._is_sync()
        else:
            # if not greater than current kv-sequence, skip this one
            if sequence <= self.__kv_seq:
                # TODO: trigger a kvsync if sequence != kv_seq + 1; leaf nodes do not get every
                #       update, so that won't work...
                self.logger.warn('kv write out of sequence (cur=%d, recvd=%d); dropping' % (
                    self.__kv_seq, sequence))
                return False

            # always set seq-num if not ignore_seq
            self.__kv_seq = sequence

        # set/delete given key-value pair
        self.__kvdict[key] = (value, sequence)
        if value is None:
            del self.__kvdict[key]

        return True

    # END dictionary access methods
    #####

    #####
    # BEGIN topo tree access methods

    def topo_get_size(self):
        return len(self.__tree)

    # root access

    def topo_get_root(self):
        return self.__tree.root()

    def topo_set_root(self, ep, uuid):
        with self.__kvlock:
            kvlist = self.__tree.insert_root(ep, uuid)
            self.__kvlist_store_and_pub(kvlist, skip_topo=True)

    # branch/collector access

    def topo_group_size(self, group):
        c = self.topo_get_collector(group)
        return c is not None and len(c.children) or 0

    def topo_get_collector(self, group):
        return self.__tree.get_collector(group)

    def topo_get_all_collectors(self):
        cs = []
        for g in self.config_get_groups():
            c = self.__tree.get_collector(g)
            if c is not None:
                cs.append(c)
        return cs

    def topo_del_branch(self, collector):
        with self.__kvlock:
            kvlist = self.__tree.remove_collector(collector)
            self.__kvlist_store_and_pub(kvlist, skip_topo=True)

    # leaf/node access

    def topo_get_node(self, endpoint):
        return self.__tree.get_node(endpoint)

    def topo_set_node(self, node):
        with self.__kvlock:
            kvlist = self.__tree.update_node(node)
            self.__kvlist_store_and_pub(kvlist, skip_topo=True)

    def topo_insert_endpoint(self, ep, uuid, level, group, parent):
        node = TopoNode(ep, uuid, level, group)
        node.touch()
        with self.__kvlock:
            kvlist = self.__tree.insert_node(node, parent)
            self.__kvlist_store_and_pub(kvlist, skip_topo=True)
        return node

    def topo_touch_node(self, node):
        with self.__kvlock:
            kvlist = self.__tree.touch_node(node)
            self.__kvlist_store_and_pub(kvlist, skip_topo=True)

    # END topo tree access methods
    #####

    def __set_gogo(self):
        self.state_cond.acquire()
        self.state = Configuration.STATE_GOGO
        self.state_cond.notify_all()
        self.state_cond.release()

    def _is_gogo(self):
        return Configuration.STATE_GOGO == self.state

    def _is_sync(self):
        return Configuration.STATE_SYNC == self.state

    def wait_for_gogo(self):
        self.state_cond.acquire()
        self.state_cond.wait_for(self._is_gogo)
        self.state_cond.release()

    #####
    # BEGIN config access methods

    def config_get_hb_int(self):
        if self._is_gogo():
            return self['/CONFIG/global/heartbeat']
        return 5  # default hb interval until init is complete

    def config_get_metric_specs(self, group=None):
        assert self._is_gogo()

        # root needs all metrics
        if self.level == 'root':
            s = -1
            m = []

            for g in self.config_get_groups():
                (gm, gs) = self.get('/CONFIG/%s/metrics' % g, [])
                s = max(gs, s)
                m.extend(gm)

            return m, s

        # otherwise, just return this group's metrics
        if group is None:
            group = self.group

        # return tuple = (spec-list, seq-id) or (None, -1)
        return self.get('/CONFIG/%s/metrics' % group, [])

    def config_get_endpoints(self, group=None):
        assert self._is_gogo()

        if group is None:
            group = self.group

        # return ep-list or []
        try:
            return self['/CONFIG/%s/endpoints' % group]
        except KeyError:
            return []

    def config_get_groups(self):
        regex = re.compile('/CONFIG/(\w+)/endpoints')
        gs = set()
        with self.__kvlock:
            for key in self.__kvdict:
                m = regex.match(key)
                if m is not None:
                    # every group has three keys in the dict; lazily find the unique
                    # groups names by using a set
                    gs.add(m.group(1))
        return gs

    # END config access methods
    #####

    def __init_kvdict_from_file(self, config_file):
        assert 'root' == self.level
        assert config_file is not None
        cfg = ConfigFileMixin()
        cfg.read_file(open(config_file))
        for (k, v) in cfg.kvdict.items():
            assert isinstance(k, str)
            self.logger.debug('INIT: {}: {}'.format(k, v))
        self.__kvlist_store_and_pub(cfg.kvdict.items())
        self.logger.debug('INIT: final kv-seq = {}'.format(self.__kv_seq))

    def __init_kvdict_from_dict(self, cfg):
        assert 'root' == self.level
        for (k, (v, seq)) in cfg.items():
            assert isinstance(k, str)
            self.logger.debug('INIT: [{}] {}: {}'.format(seq, k, v))
        self.__kvlist_store_and_pub([(k, v, seq) for k, (v, seq) in cfg.items()])
        self.logger.debug('INIT: final kv-seq = {}'.format(self.__kv_seq))

    def __init_consumer_sockets(self):
        assert self.level in ['branch', 'leaf']
        assert self.parent is not None
        assert len(self.topics) > 0

        # 1) subscribe to udpates from parent
        self.update_sub = self.ctx.socket(SUB)
        for t in self.topics:
            self.update_sub.setsockopt_string(SUBSCRIBE, t)
        self.update_sub.connect(self.parent.connect_uri(EndpntSpec.CONFIG_UPDATE))

        self.poller.register(self.update_sub, POLLIN)

        # 2) request snapshot(s) from parent
        self.kvsync_req = self.ctx.socket(DEALER)
        self.kvsync_req.connect(self.parent.connect_uri(EndpntSpec.CONFIG_SNAPSHOT))
        for t in self.topics:
            icanhaz = config.ICANHAZ(t)
            icanhaz.send(self.kvsync_req)

        self.poller.register(self.kvsync_req, POLLIN)

    def __init_producer_sockets(self):
        assert self.level in ['root', 'branch']
        assert self._is_gogo()

        # 3) publish updates to children (bind)
        self.update_pub = self.ctx.socket(PUB)
        self.update_pub.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_UPDATE))

        if 'branch' == self.level:
            assert self.group is not None
            t = '/CONFIG/%s' % self.group
        elif 'root' == self.level:
            t = '/TOPO'
        else:
            raise NotImplementedError('unknown level: %s' % self.level)

        self.hug_msg = config.HUGZ(t)
        self.__hb_sent()  # start the hb timer

        # 4) service snapshot requests to children (bind)
        self.kvsync_rep = self.ctx.socket(ROUTER)
        self.kvsync_rep.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_SNAPSHOT))
        self.poller.register(self.kvsync_rep, POLLIN)

        # process pending updates; this will trigger kvpub updates for each message processed
        self.__kvlist_store_and_pub(self.pending_updates)
        del self.pending_updates

    def _cleanup(self):
        # service exiting; return some status info and cleanup
        self.logger.debug(
            "%d subs; %d pubs; %d hugz; %d reqs; %d reps" %
            (self.subcnt, self.pubcnt, self.hugcnt, self.reqcnt, self.repcnt))

        # print each key-value pair; value is really (value, seq-num)
        self.logger.debug('kv-seq: %d' % self.__kv_seq)
        width = len(str(self.__kv_seq))
        with self.__kvlock:
            for (k, (v, s)) in sorted(self.__kvdict.items()):
                self.logger.debug('({0:0{width}d}) {1}: {2}'.format(s, k, v, width=width))

        self.__tree.print()

        if self.update_sub is not None:
            self.update_sub.close()
        del self.update_sub

        if self.update_pub is not None:
            self.update_pub.close()
        del self.update_pub

        if self.kvsync_req is not None:
            self.kvsync_req.close()
        del self.kvsync_req

        if self.kvsync_rep is not None:
            self.kvsync_rep.close()
        del self.kvsync_rep

        ServiceMixin._cleanup(self)

    def _pre_poll(self):
        # wait until finished with sync state before sending anything
        if not self._is_gogo():
            self.poller_timer = None
            return

        if self.level in ['root', 'branch']:
            assert self.next_hug is not None
            if self.next_hug <= now_secs():
                self.__send_hug()

        if self.level in ['branch', 'leaf']:
            assert self.next_sos is not None
            if self.next_sos <= now_secs():
                self.sos()
                self.__hb_received()  # reset hb monitor so we don't flood the system with sos

        self.poller_timer = self.__get_next_wakeup()

    def _post_poll(self, items):
        if self.update_sub in items:
            self.__recv_update()
        if self.kvsync_req in items:
            self.__recv_snapshot()
        if self.kvsync_rep in items:
            self.__send_snapshot()

    def __send_hug(self):
        assert self.level in ['root', 'branch']
        assert self._is_gogo()
        assert self.update_pub is not None
        assert self.hug_msg is not None

        self.hug_msg.send(self.update_pub)
        self.__hb_sent()
        self.hugcnt += 1

    def __get_next_wakeup(self):
        """ @returns next wakeup time (as msecs delta) """
        assert self._is_gogo()

        next_wakeup = None
        next_hug_wakeup = None
        next_sos_wakeup = None

        if self.level in ['root', 'branch']:
            assert self.next_hug is not None  # initialized by __init_producer_sockets()
            # next_hug is in secs; subtract current msecs to get next wakeup
            next_hug_wakeup = (self.next_hug * 1e3) - now_msecs()
            next_wakeup = next_hug_wakeup

        if self.level in ['branch', 'leaf']:
            assert self.next_sos is not None  # initialized by __recv_snapshot() / _recv_update()
            # next_sos is in secs; subtract current msecs to get next wakeup
            next_sos_wakeup = (self.next_sos * 1e3) - now_msecs()
            next_wakeup = next_sos_wakeup

        if 'branch' == self.level:
            assert next_hug_wakeup is not None
            assert next_sos_wakeup is not None
            # pick the sooner of the two wakeups
            next_wakeup = min(next_hug_wakeup, next_sos_wakeup)

        assert next_wakeup is not None
        # make sure it's not negative
        val = max(0, next_wakeup)

        self.logger.debug('next wakeup in %dms' % val)

        return val

    def __hb_sent(self):
        # reset hugz using last pub time
        self.next_hug = now_secs() + self.config_get_hb_int()

    def __hb_received(self):
        # reset sos using last hb time
        self.next_sos = now_secs() + (self.config_get_hb_int() * 5)

    def __recv_update(self):
        update = config.CONFIG.recv(self.update_sub)
        self.__hb_received()  # any message from parent is considered a heartbeat
        self.subcnt += 1

        if update.is_error:
            self.logger.error('received error message from parent: %s' % update)
            return

        if update.is_hugz:
            # already noted above. moving on...
            self.logger.debug('received hug.')
            return

        if self._is_sync():
            # another solution is to just not read the message; let them queue
            # up on the socket itself...but that relies on the HWM of the socket
            # being set high enough to account for all messages received while
            # in the SYNC state. this approach guarantees no updates are lost.
            self.pending_updates.append(update)
        elif self._is_gogo():
            self.__kvlist_store_and_pub([update])
        else:
            raise NotImplementedError('unknown state')

    def __recv_snapshot(self):
        assert self.level in ['branch', 'leaf']
        assert self._is_sync()

        # should either be KVSYNC or KTHXBAI
        response = config.CONFIG.recv(self.kvsync_req)
        self.__hb_received()  # any message from parent is considered a heartbeat
        self.repcnt += 1

        if response.is_error:
            self.logger.error(response)
            return

        if isinstance(response, config.KTHXBAI):
            if response.value not in self.topics:
                self.logger.error('received KTHXBAI of unexpected subtree: %s' % response.value)
                return

            # add given subtree to completed list; return if still waiting for other
            # subtree kvsync sessions
            self.kvsync_completed[response.value] = response.sequence
            if len(self.kvsync_completed) != len(self.topics):
                return

            self.__kv_seq = max(self.kvsync_completed.values())
            del self.kvsync_completed

            # set GOGO state; this basically means we have all the config values
            self.__set_gogo()

            if 'branch' == self.level:
                self.__init_producer_sockets()

        else:
            self.__kvlist_store_and_pub([response], ignore_seq=True)

    def __send_snapshot(self):
        assert self._is_gogo()

        request = config.CONFIG.recv(self.kvsync_rep)
        self.reqcnt += 1

        if request.is_error:
            self.logger.error(request)
            return

        peer_id = request.peer_id
        subtree = request.value  # subtree stored as value in ICANHAZ message

        # send all the key-value pairs in our dict
        max_seq = -1
        with self.__kvlock:
            for (k, (v, s)) in self.__kvdict.items():
                # skip keys not in the requested subtree
                if not k.startswith(subtree):
                    continue
                max_seq = max([max_seq, s])
                snap = config.KVSYNC(k, v, s, peer_id)
                snap.send(self.kvsync_rep)

        # send final message, closing the kvsync session
        snap = config.KTHXBAI(max_seq, peer_id, subtree)
        snap.send(self.kvsync_rep)
