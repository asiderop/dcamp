import logging, zmq
from time import time

import dcamp.types.messages.data as DataMsg
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import Service

class Filter(Service):

	def __init__(self,
			control_pipe,
			config_service,
			endpoint):
		Service.__init__(self, control_pipe)

		self.config_service = config_service
		self.endpoint = endpoint

		self.metric_specs = []
		self.metric_seqid = -1

		(self.pullcnt, self.pubcnt) = (0, 0)

		# pull metrics on this socket (from the sensor and child services)
		self.pull_socket = self.ctx.socket(zmq.PULL)
		self.pull_socket.connect(self.endpoint.connect_uri(EndpntSpec.DATA_PUSH_PULL, 'inproc'))
		self.poller.register(self.pull_socket)

		# pub metrics on this sockets (to the node's parent)
		self.pub_socket = self.ctx.socket(zmq.PUB)
		self.pull_socket.bind(self.endpoint.bind_uri(EndpntSpec.DATA_PUB))

		# XXX: get metric specs from config service
		self.pub_int = 10 # start out checking for config updates every 5 seconds
		self.next_pub = 0

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d pulls; %d pubs; %s metrics" %
				(self.pullcnt, self.pubcnt, self.metric_specs))

		self.pull_socket.close()
		self.pub_socket.close()
		del self.pull_socket, self.pub_socket
		super()._cleanup()

	def _pre_poll(self):
		self.__check_config_for_metric_updates()

		if self.next_pub < time():
			self.__pub_metrics()

		self.poller_timer = 1e3 * max(0, self.next_pub - time())

	def _post_poll(self, items):
		if self.pull_socket in items:
			data = DataMsg.DATA.recv(self.pull_socket)
			self.pullcnt += 1
			del data

	def __pub_metrics(self):
		metric = DataMsg.DATA(self.endpoint, 'HUGZ')
		metric.send(self.pub_socket)
		self.pubcnt += 1
		self.next_pub = time() + self.pub_int

	def __check_config_for_metric_updates(self):
		(specs, seq) = self.config_service.get_metric_specs()
		if seq > self.metric_seqid:
			self.metric_specs = specs[:] # copy spec list
			self.metric_seqid = seq
			self.logger.debug('new metric specs: %s' % self.metric_specs)
			# XXX: trigger new metric setup
