from threading import Lock
from types import MethodType

from zmq import PUB, SUB, SUBSCRIBE, POLLIN, DEALER, ROUTER  # pylint: disable-msg=E0611

import dcamp.types.messages.configuration as config
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import ServiceMixin
from dcamp.util.functions import now_secs, now_msecs


class Configuration(ServiceMixin):

    # states
    STATE_SYNC = 0
    STATE_GOGO = 1

    def __init__(
            self,
            control_pipe,  # control pipe for shutting down service
            level,
            group,
            parent_ep,  # from where we receive config updates/snapshots
            local_ep,  # this is us
            sos_func,  # call when our parent stops sending HUGZ
    ):
        ServiceMixin.__init__(self, control_pipe)
        assert level in ['root', 'branch', 'leaf']
        assert isinstance(parent_ep, (EndpntSpec, type(None)))
        assert isinstance(local_ep, EndpntSpec)

        self.level = level
        self.group = group
        self.parent = parent_ep
        self.endpoint = local_ep

        if self.level in ['branch', 'leaf']:
            assert isinstance(sos_func, MethodType)
        else:
            assert sos_func is None
        self.sos = sos_func

        self.state = None

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

        current_secs = now_secs()

        # root/branch members for sending hearbeats to children
        self.hug_int = 5  # units: seconds
        self.next_hug = current_secs + self.hug_int  # units: seconds
        self.last_pub = current_secs  # units: seconds
        self.hug = None

        # branch/leaf members for detecting parent heartbeats
        self.last_hb = current_secs  # units: seconds
        self.next_sos = current_secs + (self.hug_int * 5)  # units: seconds

        # sockets and message counts
        (self.update_sub, self.update_pub, self.kvsync_req, self.kvsync_rep) = (None, None, None, None)
        (self.subcnt, self.pubcnt, self.hugcnt, self.reqcnt, self.repcnt) = (0, 0, 0, 0, 0)

        ### Branch/Leaf Members

        # { topic : final-seq-num }
        self.kvsync_completed = {}
        self.pending_updates = []

        self.topics = []
        if 'leaf' == self.level:
            assert self.group is not None
            self.topics.append('/config/%s' % self.group)
        elif 'branch' == self.level:
            self.topics.append('/config')
            self.topics.append('/topo')

        self.__initalize_sockets()

    def __initalize_sockets(self):
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

            self.state = Configuration.STATE_SYNC

        else:
            assert 'root' == self.level
            self.__setup_outbound()

    ### Dictionary Access
    # map access to internal kvdict

    def __getitem__(self, k):
        (val, seq) = self.kvdict[k]
        return val

    def get(self, k, default=None):
        (val, seq) = self.kvdict.get(k, (default, 0))
        return val

    def __setitem__(self, k, v):
        assert 'root' == self.level, "only root level allowed to make modifications"
        item = config.KVPUB(k, v, self.kv_seq + 1)
        self.__process_update_message(item)  # add to our kvdict and publish update

    def __delitem__(self, k):
        assert 'root' == self.level, "only root level allowed to make modifications"
        item = config.KVPUB(k, None, self.kv_seq + 1)
        self.__process_update_message(item)  # remove from our kvdict and publish update

    def get_metric_specs(self, group=None):
        if group is None:
            group = self.group

        # return (spec-list, seq-id) or (None, -1)
        return self.kvdict.get('/config/%s/metrics' % group, (None, -1))

    def __setup_outbound(self):
        assert self.level in ['root', 'branch']

        # 3) publish updates to children (bind)
        self.update_pub = self.ctx.socket(PUB)
        self.update_pub.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_UPDATE))

        if 'branch' == self.level:
            assert self.group is not None
            t = '/config/%s' % self.group
        else:  # root
            t = '/topo'

        self.hug = config.HUGZ(t)
        self.last_pub = now_secs()

        # 4) service snapshot requests to children (bind)
        self.kvsync_rep = self.ctx.socket(ROUTER)
        self.kvsync_rep.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_SNAPSHOT))
        self.poller.register(self.kvsync_rep, POLLIN)

        # process pending updates; this will trigger kvpub updates for each message
        # processed
        for update in self.pending_updates:
            self.__process_update_message(update)
        del self.pending_updates

        self.state = Configuration.STATE_GOGO

    def _cleanup(self):
        # service exiting; return some status info and cleanup
        self.logger.debug(
            "%d subs; %d pubs; %d hugz; %d reqs; %d reps" %
            (self.subcnt, self.pubcnt, self.hugcnt, self.reqcnt, self.repcnt))

        # print each key-value pair; value is really (value, seq-num)
        self.logger.debug('kv-seq: %d' % self.kv_seq)
        width = len(str(self.kv_seq))
        for (k, (v, s)) in sorted(self.kvdict.items()):
            self.logger.debug('({0:0{width}d}) {1}: {2}'.format(s, k, v, width=width))

        if self.update_sub is not None:
            self.update_sub.close()
        if self.update_pub is not None:
            self.update_pub.close()
        if self.kvsync_req is not None:
            self.kvsync_req.close()
        if self.kvsync_rep is not None:
            self.kvsync_rep.close()
        del self.update_sub, self.update_pub, self.kvsync_req, self.kvsync_rep

        ServiceMixin._cleanup(self)

    def _pre_poll(self):
        if self.level in ['root', 'branch'] and self.next_hug <= now_secs():
            self.__send_hug()

        if self.level in ['branch', 'leaf'] and self.next_sos <= now_secs():
            self.sos()
            self.last_hb = now_secs()  # reset last hb so we don't flood the system with sos

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

        # wait until finished with sync state
        if self.update_pub is not None:
            self.hug.send(self.update_pub)
            self.last_pub = now_secs()
            self.hugcnt += 1

    def __get_next_wakeup(self):
        """ @returns next wakeup time (as msecs delta) """

        next_wakeup = None
        next_hug_wakeup = None
        next_sos_wakeup = None

        if self.level in ['root', 'branch']:
            # reset hugz using last pub time
            self.next_hug = self.last_pub + self.hug_int
            # next_hug is in secs; subtract current msecs to get next wakeup
            next_hug_wakeup = (self.next_hug * 1e3) - now_msecs()
            next_wakeup = next_hug_wakeup

        if self.level in ['branch', 'leaf']:
            # reset sos using last hb time
            self.next_sos = self.last_hb + (self.hug_int * 5)
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

    def __recv_update(self):
        update = config.CONFIG.recv(self.update_sub)
        self.subcnt += 1

        if update.is_error:
            self.logger.error('received error message from parent: %s' % update)
            return

        if update.is_hugz:
            # noted. moving on...
            self.last_hb = now_secs()
            self.logger.debug('received hug.')
            return

        if Configuration.STATE_SYNC == self.state:
            # TODO: another solution is to just not read the message; let them queue up on
            #       the socket itself...
            self.pending_updates.append(update)
        elif Configuration.STATE_GOGO == self.state:
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

        # this should be None if still in SYNC state
        if self.update_pub is not None:
            update.send(self.update_pub)
            self.last_pub = now_secs()
            self.pubcnt += 1

    def __recv_snapshot(self):
        assert Configuration.STATE_SYNC == self.state

        # should either be KVSYNC or KTHXBAI
        response = config.CONFIG.recv(self.kvsync_req)

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
            if 'branch' == self.level:
                self.__setup_outbound()
        else:
            self.__process_update_message(response, ignore_sequence=True)

    def __send_snapshot(self):
        assert Configuration.STATE_GOGO == self.state

        request = config.CONFIG.recv(self.kvsync_rep)

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
