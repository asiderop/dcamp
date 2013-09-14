import logging, zmq
from time import time

import dcamp.types.messages.data as DataMsg
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import Service

class Sensor(Service):

	def __init__(self,
			control_pipe,
			config_service,
			endpoint):
		Service.__init__(self, control_pipe)

		self.config_service = config_service
		self.endpoint = endpoint

		self.metric_specs = []
		self.metric_seqid = -1

		self.pushcnt = 0

		# we push metrics on this socket (to filter service)
		self.metrics_socket = self.ctx.socket(zmq.PUSH)
		self.metrics_socket.bind(self.endpoint.bind_uri(EndpntSpec.DATA_PUSH_PULL, 'inproc'))

		# XXX: get metric specs from config service
		self.push_int = 5 # start out checking for config updates every 5 seconds
		self.next_collection = 0

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d pushes; %s metrics" %
				(self.pushcnt, self.metric_specs))

		self.metrics_socket.close()
		del self.metrics_socket
		super()._cleanup()

	def _pre_poll(self):
		self.__check_config_for_metric_updates()

		if self.next_collection < time():
			self.__collect_and_push_metrics()

		self.poller_timer = 1e3 * max(0, self.next_collection - time())

	def _post_poll(self, items):
		pass

	def __collect_and_push_metrics(self):
		metric = DataMsg.DATA(self.endpoint, 'HUGZ')
		metric.send(self.metrics_socket)
		self.pushcnt += 1
		self.next_collection = time() + self.push_int

	def __check_config_for_metric_updates(self):
		(specs, seq) = self.config_service.get_metric_specs()
		if seq > self.metric_seqid:
			self.metric_specs = specs[:] # copy spec list
			self.metric_seqid = seq
			self.logger.debug('new metric specs: %s' % self.metric_specs)
			# XXX: trigger new metric setup
