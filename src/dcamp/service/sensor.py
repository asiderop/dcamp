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
            local_ep,
            local_uuid,
            config_svc,
    ):
        ServiceMixin.__init__(self, control_pipe, local_ep, local_uuid, config_svc)

        # goal: sort by next collection time
        self.metric_collections = []
        self.metric_seqid = -1

        self.push_cnt = 0

        # we push metrics on this socket (to filter service)
        self.metrics_socket = self.ctx.socket(PUSH)
        self.metrics_socket.connect(self.endpoint.connect_uri(EndpntSpec.DATA_INTERNAL, 'inproc'))

        self.next_collection = now_secs()  # units: seconds

    def _cleanup(self):
        # service exiting; return some status info and cleanup
        self.logger.debug("%d pushes; metrics = [%s]" %
                          (self.push_cnt, self.metric_collections))

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

        if len(self.metric_collections) == 0:
            # check for new metric specs every hb-interval seconds
            self.next_collection = now_secs() + self.cfgsvc.config_get_hb_int()
            return

        collected = []
        while True:
            collection = self.metric_collections.pop(0)
            assert collection.epoch <= now_secs(), 'next metric is not scheduled for collection'

            (msg, collection) = self.__process(collection)
            if msg is not None:
                msg.send(self.metrics_socket)
                self.push_cnt += 1
            collected.append(collection)

            if len(self.metric_collections) == 0:
                # no more work
                break

            if self.metric_collections[0].epoch > now_secs():
                # no more work scheduled
                break

        # add the collected metrics back into our list
        self.metric_collections = sorted(self.metric_collections + collected)
        # set the new collection wakeup
        self.next_collection = self.metric_collections[0].epoch

    def __check_config_for_metric_updates(self):
        # TODO: optimize this to only check the seq-id
        (specs, seq) = self.cfgsvc.config_get_metric_specs()
        if seq <= self.metric_seqid:
            return

        old_specs = []

        # add all old metric specs, continue with its next collection time
        for collection in self.metric_collections:
            if collection.spec in specs:
                old_specs.append(collection)
                specs.remove(collection.spec)

        # add all new metric specs, starting collection now
        new_specs = [MetricCollection(0, elem) for elem in specs]

        self.metric_collections = sorted(old_specs + new_specs)
        self.metric_seqid = seq

        self.logger.debug('new metric specs: %s' % self.metric_collections)

        # reset next collection wakeup with new values
        if len(self.metric_collections) > 0:
            self.next_collection = self.metric_collections[0].epoch
        else:
            # check for new metric specs every hb-interval seconds
            self.next_collection = now_secs() + self.cfgsvc.config_get_hb_int()

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

        if 'CPU' == detail:
            props['type'] = 'percent'
            msg_cls = data.DataPercent

            # cpu_times() is accurate to two decimal points
            cpu_times = psutil.cpu_times()
            value = int((sum(cpu_times) - cpu_times.idle) * 1e2)
            base_value = int(sum(cpu_times) * 1e2)

        elif 'MEMORY' == detail:
            props['type'] = 'basic'
            msg_cls = data.DataBasic

            vmem = psutil.virtual_memory()
            value = vmem.total - vmem.available

        elif 'DISK' == detail:
            props['type'] = 'rate'
            msg_cls = data.DataRate

            disk = psutil.disk_io_counters()
            value = disk.read_bytes + disk.write_bytes

        elif 'NETWORK' == detail:
            props['type'] = 'rate'
            msg_cls = data.DataRate

            net = psutil.net_io_counters()
            value = net.bytes_sent + net.bytes_recv

        elif detail.startswith('PROC_'):

            # get new list of processes every time in order to not miss any new
            # processes that are started after the first collection
            pcount = 0
            value = 0
            for proc in psutil.process_iter():
                try:
                    pinfo = proc.as_dict(attrs=['pid', 'name'])
                except psutil.NoSuchProcess:
                    continue

                if pinfo['name'] != param:
                    continue

                pcount += 1
                value += self.__get_proc_value(detail, proc)

            if 0 == pcount:
                c = MetricCollection(now_secs() + collection.spec.rate, collection.spec)
                return None, c

            props['p-count'] = pcount

            if 'PROC_CPU' == detail:
                props['type'] = 'percent'
                msg_cls = data.DataPercent
                # cpu_times() is accurate to two decimal points
                base_value = int(sum(psutil.cpu_times()) * 1e2)

            elif 'PROC_MEM' == detail:
                props['type'] = 'percent'
                msg_cls = data.DataPercent
                base_value = psutil.virtual_memory().total

            elif 'PROC_IO' == detail:
                props['type'] = 'rate'
                msg_cls = data.DataRate
            
            else:
                raise NotImplementedError('unknown process metric type: {}'.format(detail))


        else:
            raise NotImplementedError('unknown metric type: {}'.format(detail))

        m = msg_cls(self.endpoint, props, time, value, base_value)

        # create new collection with next collection time
        c = MetricCollection(now_secs() + collection.spec.rate, collection.spec)

        return m, c

    def __get_proc_value(self, detail, proc):
        if 'PROC_CPU' == detail:
            # cpu_times() is accurate to two decimal points
            return int(sum(proc.cpu_times()) * 1e2)

        elif 'PROC_MEM' == detail:
            return proc.memory_info().rss

        elif 'PROC_IO' == detail:
            try:
                io = proc.io_counters()
                value = io.read_bytes + io.write_bytes
                if value < 0:
                    # no support for *_bytes in bsd
                    value = 0
            except AttributeError:
                # no support for io_counters() in osx
                value = 0

            return value

