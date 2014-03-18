from zmq import SUB, SUBSCRIBE, PUSH, Again # pylint: disable-msg=E0611

from dcamp.types.specs import EndpntSpec
from dcamp.service.service import Service_Mixin
import dcamp.types.messages.data as DataMsg

class Aggregation(Service_Mixin):

	def __init__(self,
			control_pipe,
			level,
			config_service,
			local_ep,
			parent_ep):
		Service_Mixin.__init__(self, control_pipe)
		assert level in ['root', 'branch']
		assert isinstance(parent_ep, (EndpntSpec, type(None)))
		assert isinstance(local_ep, EndpntSpec)

		self.level = level
		self.config_service = config_service
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
				try: msg = DataMsg._DATA.recv(self.sub)
				except Again as e: break

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

		Service_Mixin._cleanup(self)
