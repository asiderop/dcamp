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

        # { config-name: aggregate-metric }
        self.metric_aggregations = {}
        self.metric_collections = []  # sorted by next collection time
        self.metric_seqid = -1
        self.next_aggregation = now_secs() + 5  # units: seconds

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

        if self.next_aggregation <= now_secs():
            self.__aggregate_and_push_metrics()

        # next_aggregation is in secs; subtract current msecs to get next wakeup epoch
        wakeup = max(0, (self.next_aggregation * 1e3) - now_msecs())
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

                if msg.is_hugz:
                    # noted. moving on...
                    self.logger.debug('received hug.')
                    continue

                # if unknown metric, just drop it
                if msg.config_seqid not in range(self.metric_seqid + 1):
                    self.logger.warn('unknown config seq-id (%d); dropping data'
                                     % msg.config_seqid)
                    self.logger.debug('dropped: <{}>'.format(msg))
                    continue

                # only push metrics we know about
                msg.send(self.push)
                self.push_cnt += 1

                # lookup aggregation using given message's configuration name
                aggr_data = self.metric_aggregations.get(msg.config_name, None)

                if aggr_data is None:
                    # unknown metric OR aggregation not configured
                    continue

                # store sample for later aggregation
                aggr_data.add_sample(msg)

    def _cleanup(self):

        # service exiting; return some status info and cleanup
        self.logger.debug("%d subs; %d pushes" %
                          (self.sub_cnt, self.push_cnt))

        self.sub.close()
        self.push.close()
        del self.sub, self.push

        ServiceMixin._cleanup(self)

    def __aggregate_and_push_metrics(self):

        if len(self.metric_collections) == 0:
            self.next_aggregation = now_secs() + 5
            return

        aggregated = []
        while True:
            # pop first item from dict using collection list order
            collection = self.metric_collections.pop(0)
            assert collection.epoch <= now_secs(), 'next metric is not scheduled for collection'
            assert collection.spec.config_name in self.metric_aggregations

            aggr_data = self.metric_aggregations[collection.spec.config_name]
            if aggr_data.aggregate(now_msecs()) is not None:
                aggr_data.send(self.push)
                self.push_cnt += 1

            # reset aggregation for the next period
            aggr_data.reset()

            # update collection spec with next epoch
            aggregated.append(MetricCollection(now_secs() + collection.spec.rate,
                                               collection.spec,
                                               None))

            if len(self.metric_collections) == 0:
                # no more work
                break

            if self.metric_collections[0].epoch > now_secs():
                # no more work scheduled
                break

        # add the aggregated metrics back into our list
        self.metric_collections = sorted(self.metric_collections + aggregated)
        # set the new collection wakeup
        self.next_aggregation = self.metric_collections[0].epoch

    def __check_config_for_metric_updates(self):
        # TODO: optimize this to only check the seq-id
        (specs, seqid) = self.cfgsvc.config_get_metric_specs()
        if seqid <= self.metric_seqid:
            self.logger.debug('no new metric specs: {} <= {}'.format(seqid, self.metric_seqid))
            return

        collections = []
        aggregations = {}

        # add all old collections, saving their aggregated metrics
        for c in self.metric_collections:
            if c.spec in specs:
                collections.append(c)
                aggregations[c.spec.config_name] = self.metric_aggregations[c.spec.config_name]
                specs.remove(c.spec)

        if self.level == 'root':
            aggr_id = 'ROOT'
        else:
            aggr_id = self.cfgsvc.group

        # add all new metric specs, using now+period for collection/aggregation time
        now = now_secs()
        for s in specs:
            if s.aggr is None:
                # skip metrics without aggregation configured
                continue

            # root needs "config-aggr" so as to only aggregate aggregated messages; the same would
            # apply to higher-level branch nodes, when those are implemented...
            cname = s.config_name
            aggr_cname = cname + '-aggr'
            if 'root' == self.level:
                cname = aggr_cname

            # create new spec with updated config_name
            s = s._replace(config_name=cname)
            c = MetricCollection(
                epoch=now + s.rate,
                spec=s,
                p=None)

            collections.append(c)

            props = {
                'detail': c.spec.detail,
                'config-name': aggr_cname,  # message always uses aggregated config name
                'config-seqid': seqid,  # use new seqid
                'aggr-id': aggr_id,
                'type': 'aggregate-' + s.aggr,
            }
            assert cname not in aggregations
            aggregations[cname] = data.DataAggregate(self.endpoint, props)

        assert len(aggregations) == len(collections)
        self.metric_collections = sorted(collections)
        self.metric_aggregations = aggregations
        self.metric_seqid = seqid

        self.logger.debug('new metric specs: %s' % self.metric_collections)

        # reset next collection wakeup with new values
        if len(self.metric_collections) > 0:
            self.next_aggregation = self.metric_collections[0].epoch
        else:
            # check for new metric specs every five seconds
            self.next_aggregation = now_secs() + 5
