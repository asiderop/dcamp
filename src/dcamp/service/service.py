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

	def run(self):
		raise NotImplemented('subclass must implement run()')
