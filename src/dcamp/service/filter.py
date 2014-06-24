import tempfile
from os import makedirs

from zmq import PUB, PULL, Again  # pylint: disable-msg=E0611

import dcamp.types.messages.data as data
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import ServiceMixin
from dcamp.util.functions import now_secs, now_msecs


class Filter(ServiceMixin):
    def __init__(
            self,
            control_pipe,
            local_ep,
            local_uuid,
            config_svc,
            parent_ep,
            level
    ):
        ServiceMixin.__init__(self, control_pipe, local_ep, local_uuid, config_svc)

        assert isinstance(parent_ep, (EndpntSpec, type(None)))
        self.parent = parent_ep

        assert level in ['root', 'branch', 'leaf']
        self.level = level

        # { config-name: (metric-spec, cached-list }
        self.metric_specs = {}
        self.metric_seqid = -1

        (self.pull_cnt, self.pubs_cnt, self.hugz_cnt) = (0, 0, 0)

        # pull metrics on this socket; all levels will pull (either from the
        # sensor service or the aggregation service)
        self.pull_socket = self.ctx.socket(PULL)
        self.pull_socket.bind(self.endpoint.bind_uri(EndpntSpec.DATA_INTERNAL, 'inproc'))
        self.poller.register(self.pull_socket)

        makedirs('./logs/', exist_ok=True)
        self.data_file = tempfile.NamedTemporaryFile(mode='w', delete=False,
                                                     prefix='{}-{}.'.format(self.level, self.endpoint),
                                                     suffix='.dcamp-data', dir='./logs/')
        self.logger.debug('writing data to %s' % self.data_file.name)

        # pub metrics on this sockets; only non-root level nodes will pub (to the parent)
        self.pubs_socket = None
        if self.level in ['branch', 'leaf']:
            self.pubs_socket = self.ctx.socket(PUB)
            self.pubs_socket.connect(self.parent.connect_uri(EndpntSpec.DATA_EXTERNAL))

        self.next_hug = now_secs()  # units: seconds
        self.last_pub = now_secs()  # units: seconds

    def _cleanup(self):
        # service exiting; return some status info and cleanup
        self.logger.debug("%d pulls; %d pubs; %d hugz; metrics= [%s]" %
                          (self.pull_cnt, self.pubs_cnt, self.hugz_cnt, self.metric_specs))

        self.data_file.close()
        self.pull_socket.close()
        del self.pull_socket

        if self.pubs_socket is not None:
            self.pubs_socket.close()
        del self.pubs_socket

        ServiceMixin._cleanup(self)

    def _pre_poll(self):
        self.__check_config_for_metric_updates()

        if self.level in ['branch', 'leaf']:
            if self.next_hug <= now_secs():
                self.__send_hug()

            self.poller_timer = self.__get_next_wakeup()

    def __send_hug(self):
        assert (self.level in ['branch', 'leaf'])

        hug = data.DataHugz(self.endpoint)
        hug.send(self.pubs_socket)
        self.hugz_cnt += 1
        self.last_pub = now_secs()

    def _post_poll(self, items):
        if self.pull_socket in items:
            while True:
                # read all messages on socket, i.e. keep reading until there is nothing
                # left to process
                try:
                    msg = data.Data.recv(self.pull_socket)
                except Again:
                    break
                self.pull_cnt += 1

                if msg.is_error:
                    self.logger.error('received error message: %s' % msg)
                    continue

                self.data_file.write(msg.log_str() + '\n')

                # process message (i.e. do the filtering) and then forward to parent
                if self.level in ['branch', 'leaf']:
                    # if unknown metric, just drop it
                    if msg.config_seqid not in range(self.metric_seqid + 1):
                        self.logger.warn('unknown config seq-id (%d); dropping data'
                                         % msg.config_seqid)
                        self.logger.debug('dropped: <{}>'.format(msg))
                        continue

                    # if non-local data, just send it off to the parent
                    if msg.source != self.endpoint or msg.config_name.endswith('-aggr'):
                        assert (self.level in ['branch'])
                        msg.send(self.pubs_socket)
                        self.pubs_cnt += 1
                        continue

                    # lookup metric spec, default is None and an empty cache list
                    (metric, cache) = self.metric_specs.get(msg.config_name, (None, []))

                    if metric is None:
                        self.logger.warn('unknown metric config-name (%s); dropping data'
                                         % msg.config_name)
                        continue

                    cache.append(msg)
                    self.logger.debug('cache size: %d' % len(cache))
                    self.__filter_and_send(metric, cache)

    def __filter_and_send(self, metric, cache):
        assert (self.level in ['branch', 'leaf'])
        do_send = True
        saved = cache[-1]  # save most recent message

        if metric.threshold is not None:
            value = None
            if metric.threshold.is_timed:
                # add to local cache and run time check with first message
                assert '*' == metric.threshold.op, 'only "hold" operation supported'
                value = cache[0].time  # time-based threshold compares against the earliest time

            elif metric.threshold.is_limit:
                # if first sample, just add to cache and skip further processing
                if len(cache) == 1:
                    return

                assert len(cache) == 2
                value = cache[0].calculate(cache[1])

            # check threshold
            assert value is not None
            if not metric.threshold.check(value):
                self.logger.debug('%s failed filter (%s): %.2f' % (metric.config_name,
                                                                   metric.threshold, value))
                do_send = False

        if do_send:
            # forward message(s) to parent
            for message in cache:
                message.send(self.pubs_socket)
                self.pubs_cnt += 1

            self.last_pub = now_secs()

            # clear cache since we just sent all the messages
            cache.clear()

        # limit-based thresholds need the previous data message for calculations
        if metric.threshold is None or metric.threshold.is_limit:
            cache.clear()
            cache.append(saved)

    def __check_config_for_metric_updates(self):
        (specs, seq) = self.cfgsvc.config_get_metric_specs()
        if seq <= self.metric_seqid:
            return

        # TODO: instead of clearing the list, try to keep old specs (and cached data)
        self.metric_specs = {}
        for s in specs:
            self.metric_specs[s.config_name] = (s, [])
        self.metric_seqid = seq
        self.logger.debug('new metric specs: {}'.format(self.metric_specs))
        # XXX: trigger new metric setup

    def __get_next_wakeup(self):
        """ @returns next wakeup time (as msecs delta) """
        assert (self.level in ['branch', 'leaf'])

        # reset hugz using last pub time
        self.next_hug = self.last_pub + self.cfgsvc.config_get_hb_int()

        # next_hug is in secs; subtract current msecs to get next wakeup
        val = max(0, (self.next_hug * 1e3) - now_msecs())
        self.logger.debug('next wakeup in %dms' % val)
        return val
