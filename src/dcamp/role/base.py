import logging

from dcamp.role.role import Role_Mixin
from dcamp.service.node import Node

class Base(Role_Mixin):
	'''
	Base Role
	'''

	def __init__(self,
			pipe,
			address):
		Role_Mixin.__init__(self, pipe)

		# add Node Service
		self._add_service(Node, address)
