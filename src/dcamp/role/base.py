import logging, zmq

from dcamp.role.role import Role
from dcamp.service.node import Node

class Base(Role):
	'''
	Base Role
	'''

	def __init__(self,
			context,
			pipe,
			address,
			topics=None):
		super().__init__(context, pipe)

		self.services = [
				Node(self.ctx, address, topics)
		]
