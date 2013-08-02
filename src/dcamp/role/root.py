from zhelpers import zpipe

from dcamp.role.role import Role
from dcamp.service.management import Management
from dcamp.service.configuration import Configuration

class Root(Role):
	'''
	Root Role
	'''

	def __init__(self,
			control_pipe,
			config):
		Role.__init__(self, control_pipe)

		(mgmt_pipe, config_pipe) = zpipe(self.ctx) # socket pair for services to communicate with each other

		# add Management Service
		self._add_service(Management, mgmt_pipe, config)

		# add Configuration Service
		self._add_service(Configuration,
				config_pipe,
				'root',
				None, # group
				None, # parent--root should use cli endpoint as parent / #42
				config.root['endpoint'],
			)
