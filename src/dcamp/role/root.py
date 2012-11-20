import logging
from zhelpers import zpipe

from dcamp.role.role import Role
from dcamp.service.management import Management

class Root(Role):
	'''
	Root Role
	'''

	def __init__(self,
			pipe,
			config):
		super().__init__(pipe)

		# [(service, pipe), ...]
		self.services = []

		# Management Service
		s_pipe, s_peer = zpipe(self.ctx) # create control socket pair
		service = Management(s_peer, config) # create service, passing peer socket
		self.services.append((service, s_pipe)) # add to our list, saving pipe socket
