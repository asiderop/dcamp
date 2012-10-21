import logging

from dcamp.service.management import Node

class Base(Role):
	'''
	Base Role
	'''

	def __init__(self,
			port=None,
			topics=None):
		super().__init__()

		self.ctx = zmq.Context.instance()
		self.services = [
				Node(self.ctx, port, topics)
		]
