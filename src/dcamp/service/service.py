import logging, threading
import zmq

class Service(threading.Thread):
	logger = logging.getLogger('dcamp.service')

	def __init__(self, pipe):
		super().__init__()
		self.ctx = zmq.Context.instance()
		self.pipe = pipe

	def _cleanup(self):
		# shared context; will be term()'ed by caller
		self.pipe.close()
		del self.pipe
		pass

	def _run(self):
		raise NotImplemented('subclass must implement _run()')

	def run(self):
		try:
			self._run()
		except zmq.ZMQError as e:
			if e.errno == zmq.ETERM:
				self.logger.debug('received ETERM: %s' % self.__class__)
			else:
				raise

		# caught ETERM; thread is stopping; cleanup and exit
		return self._cleanup()
