from zmq import SUB, SUBSCRIBE, PUSH, Again  # pylint: disable-msg=E0611

from dcamp.types.specs import EndpntSpec, MetricCollection
from dcamp.service.service import ServiceMixin
import dcamp.types.messages.data as data
from dcamp.util.functions import now_secs, now_msecs


class Aggregation(ServiceMixin):
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

        assert level in ['root', 'branch']
        self.level = level

        (self.sub_cnt, self.push_cnt) = (0, 0)

        self.metric_specs = []
        self.metric_seqid = -1

        # sub data from child(ren) ...
        self.sub = self.ctx.socket(SUB)
        self.sub.setsockopt(SUBSCRIBE, b'')
        self.sub.bind(self.endpoint.bind_uri(EndpntSpec.DATA_EXTERNAL))
        self.poller.register(self.sub)

        # ... and push them to Filter service
        self.push = self.ctx.socket(PUSH)
        self.push.connect(self.endpoint.connect_uri(EndpntSpec.DATA_INTERNAL, 'inproc'))

    def _pre_poll(self):
        self.__check_config_for_metric_updates()

        if self.next_collection <= now_secs():
            self.__collect_and_push_metrics()

        # next_collection is in secs; subtract current msecs to get next wakeup epoch
        wakeup = max(0, (self.next_collection * 1e3) - now_msecs())
        self.logger.debug('next wakeup in %dms' % wakeup)
        self.poller_timer = wakeup

    def _post_poll(self, items):
        if self.sub in items:
            while True:
                try:
                    msg = data.Data.recv(self.sub)
                except Again:
                    break

                self.sub_cnt += 1

                # TODO: store sample for aggregation

                msg.send(self.push)
                self.push_cnt += 1

    def _cleanup(self):

        # service exiting; return some status info and cleanup
        self.logger.debug("%d subs; %d pushes" %
                          (self.sub_cnt, self.push_cnt))

        self.sub.close()
        self.push.close()
        del self.sub, self.push

        ServiceMixin._cleanup(self)

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

        # TODO: calculate aggregation

        return self, collection
