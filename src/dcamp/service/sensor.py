import psutil

from zmq import PUSH  # pylint: disable-msg=E0611

import dcamp.types.messages.data as data
from dcamp.types.specs import EndpntSpec, MetricCollection
from dcamp.service.service import ServiceMixin
from dcamp.util.functions import now_secs, now_msecs


class Sensor(ServiceMixin):
    def __init__(
            self,
            control_pipe,
            config_svc,
            local_ep
    ):

        ServiceMixin.__init__(self, control_pipe, config_svc)

        self.cfgsvc = config_svc
        self.endpoint = local_ep

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
        self.logger.debug("%d pushes; metrics = [%s]" %
                          (self.push_cnt, self.metric_specs))

        self.metrics_socket.close()
        del self.metrics_socket

        ServiceMixin._cleanup(self)

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
        (specs, seq) = self.cfgsvc.config_get_metric_specs()
        if seq > self.metric_seqid:

            new_specs = []

            # add all old metric specs, continue with its next collection time
            for collection in self.metric_specs:
                if collection.spec in specs:
                    new_specs.append(collection)
                    specs.remove(collection.spec)

            # add all new metric specs, starting collection now
            new_specs = [MetricCollection(0, elem, None) for elem in specs]

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
        """ returns tuple of (data-msg, metric-collection) """
        # TODO: move this to another class?

        (value, base_value) = (None, None)
        time = now_msecs()

        props = {
            'detail': collection.spec.detail,
            'config-name': collection.spec.config_name,
            'config-seqid': self.metric_seqid,
        }

        # local vars for easier access
        detail = collection.spec.detail
        param = collection.spec.param
        p = collection.p

        if 'CPU' == detail:
            props['type'] = 'percent'
            message = data.DataPercent

            # cpu_times() is accurate to two decimal points
            cpu_times = psutil.cpu_times()
            value = int((sum(cpu_times) - cpu_times.idle) * 1e2)
            base_value = int(sum(cpu_times) * 1e2)

        elif 'MEMORY' == detail:
            props['type'] = 'percent'
            message = data.DataPercent

            vmem = psutil.virtual_memory()
            value = vmem.total - vmem.available
            base_value = vmem.total

        elif 'DISK' == detail:
            props['type'] = 'rate'
            message = data.DataRate

            disk = psutil.disk_io_counters()
            value = disk.read_bytes + disk.write_bytes

        elif 'NETWORK' == detail:
            props['type'] = 'rate'
            message = data.DataRate

            net = psutil.net_io_counters()
            value = net.bytes_sent + net.bytes_recv

        elif detail.startswith('PROC_'):

            if p is None or not p.is_running():
                p = None
                for proc in psutil.process_iter():
                    try:
                        pinfo = proc.as_dict(attrs=['pid', 'name'])
                    except psutil.NoSuchProcess:
                        continue
                    if pinfo['name'] == param:
                        p = proc
                        break

                if p is None:
                    c = MetricCollection(now_secs() + collection.spec.rate, collection.spec, None)
                    return None, c

            assert p is not None

            if 'PROC_CPU' == detail:
                props['type'] = 'percent'
                message = data.DataPercent

                # cpu_times() is accurate to two decimal points
                proc_cpu_times = p.cpu_times()
                glob_cpu_times = psutil.cpu_times()
                value = int(sum(proc_cpu_times) * 1e2)
                base_value = int(sum(glob_cpu_times) * 1e2)

            elif 'PROC_MEM' == detail:
                props['type'] = 'percent'
                message = data.DataPercent

                glob_vmem = psutil.virtual_memory()
                proc_vmem = p.memory_info()
                value = proc_vmem.rss
                base_value = glob_vmem.total

            elif 'PROC_IO' == detail:
                props['type'] = 'rate'
                message = data.DataRate

                try:
                    io = p.io_counters()
                    value = io.read_bytes + io.write_bytes
                    if value < 0:
                        # no support for *_bytes in bsd
                        value = 0
                except AttributeError:
                    # no support for io_counters() in osx
                    value = 0

        m = message(self.endpoint, props, time, value, base_value)

        # create new collection with next collection time
        c = MetricCollection(now_secs() + collection.spec.rate, collection.spec, p)

        return m, c
