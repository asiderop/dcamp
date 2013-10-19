import logging, zmq, tempfile
from time import time

import dcamp.types.messages.data as DataMsg
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import Service

class Filter(Service):

	def __init__(self,
			control_pipe,
			level,
			config_service,
			local_ep,
			parent_ep):
		Service.__init__(self, control_pipe)
		assert level in ['root', 'branch', 'leaf']
		assert isinstance(parent_ep, (EndpntSpec, type(None)))
		assert isinstance(local_ep, EndpntSpec)

		self.level = level
		self.config_service = config_service
		self.parent = parent_ep
		self.endpoint = local_ep

		self.metric_specs = []
		self.metric_seqid = -1

		(self.pullcnt, self.pubcnt) = (0, 0)

		# pull metrics on this socket; all levels will pull (either internally from the
		# sensor service or externally from child filter service's)
		self.pull_socket = self.ctx.socket(zmq.PULL)
		self.pull_socket.bind(self.endpoint.bind_uri(EndpntSpec.DATA_PUSH_PULL, 'inproc'))
		self.poller.register(self.pull_socket)

		self.data_file = tempfile.NamedTemporaryFile(mode='w', delete=False,
				prefix='{}.'.format(self.level), suffix='.dcamp-data', dir='.')
		self.logger.debug('writing data to %s' % self.data_file.name)

		# pub metrics on this sockets; only non-root level nodes will pub (to the parent)
		self.pubs_socket = None
		if self.level in ['branch', 'leaf']:
			self.pubs_socket = self.ctx.socket(zmq.PUB)
			self.pubs_socket.connect(self.parent.connect_uri(EndpntSpec.DATA_SUB))

		# sub metrics from child(ren) and push them to pull socket; only non-leaf levels
		# will sub->push
		self.proxy = None
		if self.level in ['branch', 'root']:
			self.proxy = zmq.devices.ThreadProxy(zmq.SUB, zmq.PUSH)
			self.proxy.setsockopt_in(zmq.SUBSCRIBE, b'')
			self.proxy.bind_in(self.endpoint.bind_uri(EndpntSpec.DATA_SUB))
			self.proxy.connect_out(self.endpoint.connect_uri(EndpntSpec.DATA_PUSH_PULL, 'inproc'))
			self.proxy.start()

		# XXX: get metric specs from config service
		self.pub_int = 10 # start out checking for config updates every 5 seconds
		self.next_pub = 0

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d pulls; %d pubs; %s metrics" %
				(self.pullcnt, self.pubcnt, self.metric_specs))

		self.data_file.close()
		self.pull_socket.close()

		if self.level in ['branch', 'leaf']:
		self.pubs_socket.close()

		if self.level in ['branch', 'root']:
			assert self.proxy is not None
			self.proxy.join()

		del self.pull_socket, self.pubs_socket, self.proxy

		super()._cleanup()

	def _pre_poll(self):
		self.__check_config_for_metric_updates()

		if self.next_pub < time():
			self.__pub_metrics()

		# XXX -- bring heartbeats here from sensor
		self.poller_timer = 1e3 * max(0, self.next_pub - time())

	def _post_poll(self, items):
		if self.pull_socket in items:
			while True:
				data = None

				# read all messages on socket, i.e. keep reading until there is nothing
				# left to process
				try: data = DataMsg.DATA.recv(self.pull_socket)
				except zmq.Again as e: break

			self.pullcnt += 1
				self.data_file.write(data.log_str() + '\n')

				# forward metric to parent
				if self.level in ['branch', 'leaf']:
					data.send(self.pubs_socket)
					self.pubcnt += 1

			del data

	def __pub_metrics(self):
		#metric = DataMsg.DATA(self.endpoint, 'HUGZ')
		#metric.send(self.pubs_socket)
		self.logger.debug('PUB-METRICS')
		#self.pubcnt += 1
		self.next_pub = time() + self.pub_int

	def __check_config_for_metric_updates(self):
		(specs, seq) = self.config_service.get_metric_specs()
		if seq > self.metric_seqid:
			self.metric_specs = specs[:] # copy spec list
			self.metric_seqid = seq
			self.logger.debug('new metric specs: %s' % self.metric_specs)
			# XXX: trigger new metric setup
