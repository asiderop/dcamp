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

		# { pipe: service, ...}
		self.services = {}

		# Management Service
		s_pipe, s_peer = zpipe(self.ctx) # create control socket pair
		service = Management(s_peer, config) # create service, passing peer socket
		self.services[s_pipe] = service # add to our dict, using pipe socket as key
