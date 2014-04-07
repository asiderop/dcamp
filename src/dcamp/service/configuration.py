import re
from threading import Lock, Condition
from types import MethodType

from zmq import PUB, SUB, SUBSCRIBE, POLLIN, DEALER, ROUTER  # pylint: disable-msg=E0611

import dcamp.types.messages.configuration as config
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import ServiceMixin
from dcamp.util.functions import now_secs, now_msecs
from dcamp.util.decorators import decorate_all, printer
from dcamp.types.config_file import ConfigFileMixin


class Configuration(ServiceMixin):  # , metaclass=decorate_all(printer)):

    # states
    STATE_SYNC = 0
    STATE_GOGO = 1

    def __init__(
            self,
            control_pipe,  # control pipe for shutting down service
            config_svc,  # obviously, this must be None
            local_ep,  # this is us
            parent_ep,  # from where we receive config updates/snapshots
            level,
            group,
            sos_func,  # call when our parent stops sending HUGZ
            config_file=None,  # should only be given for root level
    ):
        ServiceMixin.__init__(self, control_pipe, config_svc)

        assert config_svc is None

        assert level in ['root', 'branch', 'leaf']
        self.level = level

        self.group = group

        assert isinstance(parent_ep, (EndpntSpec, type(None)))
        self.parent = parent_ep

        assert isinstance(local_ep, EndpntSpec)
        self.endpoint = local_ep

        if self.level in ['branch', 'leaf']:
            assert isinstance(sos_func, MethodType)
        else:
            assert sos_func is None
        self.sos = sos_func

        # While no special locking is needed for the kvdict / seq_num members w.r.t.
        # multiple writers (only the Management service can write changes), reading the
        # dict while a change is being made will cause a RuntimeError to be raised. We
        # could solve this by making a copy of the dict everytime a snapshot request is
        # being made, but we will just use a Lock object instead.
        #
        # The Lock only needs to be held while sending a snapshot (i.e. iterating across
        # the entire dictionary) and writing changes to the dictionary. Getting individual
        # elements does not require the Lock.
        #
        # WRITE: If level is root, there will be no sub messages to process, so only one
        #        thread (the Management service thread) will ever write to the kvdict and
        #        seq_num members.
        # READ:  If a kvsync and __setitem__() call occur simultaneously, the new item will
        #        trigger a kvpub update. So if the new value is not included in the sync
        #        snapshot, the clients will still get the new value.

        # { key : ( value, seq-num ) }
        self.kvlock = Lock()
        self.kvdict = {}
        self.kv_seq = -1

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

        # receiving updates
        self.topics = []
        if 'leaf' == self.level:
            assert self.group is not None
            self.topics.append('/config/%s' % self.group)
        elif 'branch' == self.level:
            self.topics.append('/config')
            self.topics.append('/topo')

        ### let's get it started

        # sockets and message counts
        (self.update_sub, self.update_pub, self.kvsync_req, self.kvsync_rep) = (None, None, None, None)
        (self.subcnt, self.pubcnt, self.hugcnt, self.reqcnt, self.repcnt) = (0, 0, 0, 0, 0)

        # populate dict with config_file values if root level; values are used later
        if 'root' == self.level:
            self.__initialize_kvdict(config_file)

        self.__initialize_sockets()

    #####
    #  BEGIN dictionary access methods

    def get(self, k, default=None):
        """ returns (value, seq-id) """
        return self.kvdict.get(k, (default, -1))

    def __getitem__(self, k):
        (val, seq) = self.kvdict[k]
        return val

    def __setitem__(self, k, v):
        assert 'root' == self.level, "only root level allowed to make modifications"
        item = config.KVPUB(k, v, self.kv_seq + 1)
        self.__process_update_message(item)  # add to our kvdict and publish update

    def __delitem__(self, k):
        assert 'root' == self.level, "only root level allowed to make modifications"
        item = config.KVPUB(k, None, self.kv_seq + 1)
        self.__process_update_message(item)  # remove from our kvdict and publish update

    def __len__(self):
        return len(self.kvdict)

    def __iter__(self):
        return iter(self.kvdict)

    # END dictionary access methods
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

    @property
    def hb_int(self):
        if self._is_gogo():
            return self['/config/global/heartbeat']
        return 5  # default hb interval until init is complete

    def root(self, new_root=None):
        if new_root is not None:
            # TODO: this will fail an assertion
            self['/topo/root'] = new_root

        return self['/topo/root']

    def get_metric_specs(self, group=None):
        assert self._is_gogo()

        if group is None:
            group = self.group

        # return tuple = (spec-list, seq-id) or (None, -1)
        return self.get('/config/%s/metrics' % group, (None, -1))

    def get_endpoints(self, group=None):
        assert self._is_gogo()

        if group is None:
            group = self.group

        # return ep-list or []
        try:
            return self['/config/%s/endpoints' % group]
        except KeyError:
            return []

    def get_groups(self):
        regex = re.compile('/config/(\w+)/.+')
        gs = set()
        with self.kvlock:
            for key in self:
                m = regex.match(key)
                if m is not None:
                    # every group has three keys in the kvdict; lazily find the unique
                    # groups names by using a set
                    gs.add(m.group(1))
        return gs

    # END config access methods
    #####

    def __initialize_kvdict(self, config_file):
        assert 'root' == self.level
        assert config_file is not None
        cfg = ConfigFileMixin()
        cfg.read_file(open(config_file))
        for (k, v) in cfg.kvdict.items():
            assert isinstance(k, str)
            self.logger.debug('INIT: {}: {}'.format(k, v))
            self[k] = v
        self.logger.debug('INIT: final kv-seq = {}'.format(self.kv_seq))

    def __initialize_sockets(self):
        if self.level in ['branch', 'leaf']:
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

        else:
            assert 'root' == self.level

            # set GOGO state; this basically means we have all the config values
            self.__set_gogo()

            self.__setup_outbound()

    def __setup_outbound(self):
        assert self.level in ['root', 'branch']
        assert self._is_gogo()

        # 3) publish updates to children (bind)
        self.update_pub = self.ctx.socket(PUB)
        self.update_pub.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_UPDATE))

        if 'branch' == self.level:
            assert self.group is not None
            t = '/config/%s' % self.group
        elif 'root' == self.level:
            t = '/topo'
        else:
            raise NotImplementedError('unknown level: %s' % self.level)

        self.hug_msg = config.HUGZ(t)
        self.__hb_sent()  # start the hb timer

        # 4) service snapshot requests to children (bind)
        self.kvsync_rep = self.ctx.socket(ROUTER)
        self.kvsync_rep.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_SNAPSHOT))
        self.poller.register(self.kvsync_rep, POLLIN)

        # process pending updates; this will trigger kvpub updates for each message
        # processed
        for update in self.pending_updates:
            self.__process_update_message(update)
        del self.pending_updates

    def _cleanup(self):
        # service exiting; return some status info and cleanup
        self.logger.debug(
            "%d subs; %d pubs; %d hugz; %d reqs; %d reps" %
            (self.subcnt, self.pubcnt, self.hugcnt, self.reqcnt, self.repcnt))

        # print each key-value pair; value is really (value, seq-num)
        self.logger.debug('kv-seq: %d' % self.kv_seq)
        width = len(str(self.kv_seq))
        with self.kvlock:
            for (k, (v, s)) in sorted(self.kvdict.items()):
                self.logger.debug('({0:0{width}d}) {1}: {2}'.format(s, k, v, width=width))

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

        assert self.next_hug is not None
        if self.level in ['root', 'branch'] and self.next_hug <= now_secs():
            self.__send_hug()

        assert self.next_sos is not None
        if self.level in ['branch', 'leaf'] and self.next_sos <= now_secs():
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
            assert self.next_hug is not None  # initialized by __setup_outbound()
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
        self.next_hug = now_secs() + self.hb_int

    def __hb_received(self):
        # reset sos using last hb time
        self.next_sos = now_secs() + (self.hb_int * 5)

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
            self.__process_update_message(update)
        else:
            raise NotImplementedError('unknown state')

    def __process_update_message(self, update, ignore_sequence=False):
        # if not greater than current kv-sequence, skip this one
        if not ignore_sequence and update.sequence <= self.kv_seq:
            self.logger.warn('KVPUB out of sequence (cur=%d, recvd=%d); dropping' % (
                self.kv_seq, update.sequence))
            return

        # during kvsync, we allow out of sequence updates; we only set our seq-num when
        # not doing a kvsync
        if not ignore_sequence:
            self.kv_seq = update.sequence

        with self.kvlock:
            self.kvdict[update.key] = (update.value, update.sequence)
            if update.value is None:
                del self.kvdict[update.key]

        # wait until finished with sync state before sending updates
        if self._is_gogo():
            assert self.update_pub is not None
            update.send(self.update_pub)
            self.__hb_sent()
            self.pubcnt += 1

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

            self.kv_seq = max(self.kvsync_completed.values())
            del self.kvsync_completed

            # set GOGO state; this basically means we have all the config values
            self.__set_gogo()

            if 'branch' == self.level:
                self.__setup_outbound()

        else:
            self.__process_update_message(response, ignore_sequence=True)

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
        with self.kvlock:
            for (k, (v, s)) in self.kvdict.items():
                # skip keys not in the requested subtree
                if not k.startswith(subtree):
                    continue
                max_seq = max([max_seq, s])
                snap = config.KVSYNC(k, v, s, peer_id)
                snap.send(self.kvsync_rep)

        # send final message, closing the kvsync session
        snap = config.KTHXBAI(self.kv_seq, peer_id, subtree)
        snap.send(self.kvsync_rep)
