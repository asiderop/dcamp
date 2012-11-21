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

		# { pipe: service, ...}
		self.services = {}

		# Node Service
		s_pipe, s_peer = zpipe(self.ctx) # create control socket pair
		service = Node(s_peer, address, topics) # create service, passing peer socket
		self.services[s_pipe] = service # add to our dict, using pipe socket as key
