import logging, zmq

from dcamp.role.role import Role
from dcamp.service.node import Node

class Base(Role):
	'''
	Base Role
	'''

	def __init__(self,
			address=None,
			topics=None):
		super().__init__()

		self.ctx = zmq.Context.instance()
		self.services = [
				Node(self.ctx, address, topics)
		]
