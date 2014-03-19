import logging, sys, psutil

from zmq import PUSH  # pylint: disable-msg=E0611

import dcamp.types.messages.data as DataMsg
from dcamp.types.specs import EndpntSpec, MetricCollection
from dcamp.service.service import Service_Mixin
from dcamp.util.functions import now_secs, now_msecs


class Sensor(Service_Mixin):
    def __init__(self,
                 control_pipe,
                 config_service,
                 endpoint):
        Service_Mixin.__init__(self, control_pipe)

        self.config_service = config_service
        self.endpoint = endpoint

        # goal: sort by next collection time
        self.metric_specs = []
        self.metric_seqid = -1

        self.push_cnt = 0

        # we push metrics on this socket (to filter service)
        self.metrics_socket = self.ctx.socket(PUSH)
        self.metrics_socket.connect(self.endpoint.connect_uri(EndpntSpec.DATA_INTERNAL, 'inproc'))

        self.next_collection = now_secs() + 5  # units: seconds

    def _cleanup(self):
        # service exiting; return some status info and cleanup
        self.logger.debug("%d pushes; metrics = [\n%s]" %
                          (self.push_cnt, self.metric_specs))

        self.metrics_socket.close()
        del self.metrics_socket

        Service_Mixin._cleanup(self)

    def _pre_poll(self):
        self.__check_config_for_metric_updates()

        now = now_secs()
        if self.next_collection <= now:
            self.__collect_and_push_metrics()

        # next_collection is in secs; subtract current msecs to get next wakeup epoch
        wakeup = max(0, (self.next_collection * 1e3) - now_msecs())
        self.logger.debug('next wakeup in %dms' % wakeup)
        self.poller_timer = wakeup

    def _post_poll(self, items):
        pass

    def __collect_and_push_metrics(self):

        if len(self.metric_specs) == 0:
            self.next_collection = now_secs() + 5
            return

        collected = []
        while True:
            collection = self.metric_specs.pop(0)
            assert collection.epoch <= now_secs(), 'next metric is not scheduled for collection'

            (msg, collection) = self.__process(collection)
            if msg is not None:
                msg.send(self.metrics_socket)
                self.push_cnt += 1
            collected.append(collection)

            if len(self.metric_specs) == 0:
                # no more work
                break

            if self.metric_specs[0].epoch > now_secs():
                # no more work scheduled
                break

        # add the collected metrics back into our list
        self.metric_specs = sorted(self.metric_specs + collected)
        # set the new collection wakeup
        self.next_collection = self.metric_specs[0].epoch

    def __check_config_for_metric_updates(self):
        # TODO: optimize this to only check the seq-id
        (specs, seq) = self.config_service.get_metric_specs()
        if seq > self.metric_seqid:

            new_specs = []

            # add all old metric specs, continue with its next collection time
            for collection in self.metric_specs:
                if collection.spec in specs:
                    new_specs.append(collection)
                    specs.remove(collection.spec)

            # add all new metric specs, starting collection now
            new_specs = [MetricCollection(0, elem) for elem in specs]

            self.metric_specs = sorted(new_specs)
            self.metric_seqid = seq

            self.logger.debug('new metric specs: %s' % self.metric_specs)

            # reset next collection wakeup with new values
            if len(self.metric_specs) > 0:
                self.next_collection = self.metric_specs[0].epoch
            else:
                # check for new metric specs every five seconds
                self.next_collection = now_secs() + 5

    def __process(self, collection):
        ''' returns tuple of (data-msg, metric-collection) '''
        # TODO: move this to another class?

        (time, value, base_value) = (None, None, None)

        props = {}
        props['detail'] = collection.spec.detail
        props['config-name'] = collection.spec.config_name
        props['config-seqid'] = self.metric_seqid

        # local vars for easier access
        detail = collection.spec.detail
        message = None

        if 'CPU' == detail:
            props['type'] = 'percent'
            message = DataMsg.DATA_PERCENT

            time = now_msecs()
            # cpu_times() is accurate to two decimal points
            cpu_times = psutil.cpu_times()
            value = int(( sum(cpu_times) - cpu_times.idle ) * 1e2)
            base_value = int(sum(cpu_times) * 1e2)

        elif 'DISK' == detail:
            props['type'] = 'rate'
            message = DataMsg.DATA_RATE

            disk = psutil.disk_io_counters()

            time = now_msecs()
            value = disk.read_bytes + disk.write_bytes

        elif 'NETWORK' == detail:
            props['type'] = 'rate'
            message = DataMsg.DATA_RATE

            net = psutil.net_io_counters()

            time = now_msecs()
            value = net.bytes_sent + net.bytes_recv

        elif 'MEMORY' == detail:
            props['type'] = 'percent'
            message = DataMsg.DATA_PERCENT

            vmem = psutil.virtual_memory()

            time = now_msecs()
            value = vmem.total - vmem.available
            base_value = vmem.total

        m = message(self.endpoint, props, time, value, base_value)

        # create new collection with next collection time
        c = MetricCollection(now_secs() + collection.spec.rate, collection.spec)

        return (m, c)
