import logging
from zhelpers import zpipe

from dcamp.role.role import Role
from dcamp.service.node import Node

class Base(Role):
	'''
	Base Role
	'''

	def __init__(self,
			pipe,
			address,
			topics=None):
		super().__init__(pipe)

		# [(service, pipe), ...]
		self.services = []

		# Node Service
		s_pipe, s_peer = zpipe(self.ctx) # create control socket pair
		service = Node(s_peer, address, topics) # create service, passing peer socket
		self.services.append((service, s_pipe)) # add to our list, saving pipe socket
