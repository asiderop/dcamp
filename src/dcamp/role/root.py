import logging

from dcamp.role.role import Role
from dcamp.service.management import Management

class Root(Role):
	'''
	Root Role
	'''

	def __init__(self,
			pipe,
			config):
		Role.__init__(self, pipe)

		# add Management Service
		self._add_service(Management, config)
