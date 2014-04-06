from zmq import SUB, SUBSCRIBE, PUSH, Again  # pylint: disable-msg=E0611

from dcamp.types.specs import EndpntSpec
from dcamp.service.service import ServiceMixin
import dcamp.types.messages.data as data


class Aggregation(ServiceMixin):
    def __init__(
            self,
            control_pipe,
            config_svc,
            local_ep,
            parent_ep,
            level
    ):

        ServiceMixin.__init__(self, control_pipe, config_svc)
        assert level in ['root', 'branch']
        assert isinstance(parent_ep, (EndpntSpec, type(None)))
        assert isinstance(local_ep, EndpntSpec)

        self.level = level
        self.config_service = config_svc
        self.parent = parent_ep
        self.endpoint = local_ep

        (self.sub_cnt, self.push_cnt) = (0, 0)

        # sub data from child(ren) and push them to Filter service
        self.sub = self.ctx.socket(SUB)
        self.sub.setsockopt(SUBSCRIBE, b'')
        self.sub.bind(self.endpoint.bind_uri(EndpntSpec.DATA_EXTERNAL))
        self.poller.register(self.sub)

        self.push = self.ctx.socket(PUSH)
        self.push.connect(self.endpoint.connect_uri(EndpntSpec.DATA_INTERNAL, 'inproc'))

    def _post_poll(self, items):
        if self.sub in items:
            while True:
                try:
                    msg = data.Data.recv(self.sub)
                except Again:
                    break

                self.sub_cnt += 1
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
