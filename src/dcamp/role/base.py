import logging

from dcamp.role.role import Role
from dcamp.service.node import Node

class Base(Role):
	'''
	Base Role
	'''

	def __init__(self,
			pipe,
			address):
		Role.__init__(self, pipe)

		# add Node Service
		self._add_service(Node, address)
