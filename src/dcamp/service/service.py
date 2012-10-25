import logging

class Service(object):
	logger = logging.getLogger('dcamp.service')
	ctx = None

	def __init__(self, context):
		self.ctx = context
	def setup(self):
		pass
	def poll(self):
		pass
