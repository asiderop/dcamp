import logging, zmq

from dcamp.role.role import Role
from dcamp.service.node import Node

class Base(Role):
	'''
	Base Role
	'''

	def __init__(self,
			address,
			topics=None):
		super().__init__()

		self.services = [
				Node(self.ctx, address, topics)
		]
