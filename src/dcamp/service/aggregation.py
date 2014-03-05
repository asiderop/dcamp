from zmq import SUB, SUBSCRIBE, PUSH # pylint: disable-msg=E0611
from zmq.devices import ThreadProxy

from dcamp.types.specs import EndpntSpec
from dcamp.service.service import Service_Mixin

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

		# sub metrics from child(ren) and push them to pull socket
		# TODO: replace this with actual SUB/PUSH sockets so aggregation can be performed
		self.proxy = ThreadProxy(SUB, PUSH)
		self.proxy.setsockopt_in(SUBSCRIBE, b'')
		self.proxy.bind_in(self.endpoint.bind_uri(EndpntSpec.DATA_EXTERNAL))
		self.proxy.connect_out(self.endpoint.connect_uri(EndpntSpec.DATA_INTERNAL, 'inproc'))
		self.proxy.start()

	def _cleanup(self):

		self.proxy.join()
		del self.proxy

		Service_Mixin._cleanup(self)
