import logging, threading
import zmq

class Service(threading.Thread):
	logger = logging.getLogger('dcamp.service')

	def __init__(self, pipe):
		super().__init__()
		self.ctx = zmq.Context.instance()
		self.pipe = pipe

		self.poller = zmq.Poller()
		self.poller_timer = None

	def _cleanup(self):
		# shared context; will be term()'ed by caller
		self.pipe.close()
		del self.pipe
		pass

	def _pre_poll(self):
		pass
	def _post_poll(self, items):
		raise NotImplemented('subclass must implement _post_poll()')

	def run(self):
		while True:
			try:
				self._pre_poll()
				items = dict(self.poller.poll(self.poller_timer))
				self._post_poll(items)

			except zmq.ZMQError as e:
				if e.errno == zmq.ETERM:
					self.logger.debug('received ETERM: %s' % self.__class__)
					break
				else:
					raise

		# caught ETERM; thread is stopping; cleanup and exit
		return self._cleanup()
