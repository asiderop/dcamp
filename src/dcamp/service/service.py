import logging, threading

class Service(threading.Thread):
	logger = logging.getLogger('dcamp.service')
	ctx = None

	def __init__(self, context):
		super().__init__()
		self.ctx = context

	def setup(self):
		pass
	def run(self):
		raise NotImplemented('subclass must implement run()')
