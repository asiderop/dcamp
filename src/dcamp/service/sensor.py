import logging, time, threading, zmq

class Sensor(Service):

	def __init__(self,
			pipe,
			config_service,
			endpoint):
		Service.__init__(self, pipe)

		self.config = config_service
		self.endpoint = endpoint

		# we push metrics on this socket (to filter service)
		self.metrics_socket = self.ctx.socket(zmq.PUSH)
		self.metrics_socket.bind(self.endpoint.bind_uri(EndpntSpec.TOPO_PUSH_PULL, 'inproc'))

		# XXX: get metric config from service
		self.pubnext = time()
		self.pubint = 5

	def _cleanup(self):
		# service exiting; return some status info and cleanup
#		self.logger.debug("%d pubs; %d reqs; %d reps" %
#				(self.pubcnt, self.reqcnt, self.repcnt))

		self.metrics_socket.close()
		del self.metrics_socket
		super()._cleanup()

	def _pre_poll(self):
		if self.pubnext < time():
			self.metricmsg.send(self.disc_socket)
			self.pubnext = time() + self.pubint
			self.pubcnt += 1

		self.poller_timer = 1e3 * max(0, self.pubnext - time())
