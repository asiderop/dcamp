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

		### add services

		config_service = self._add_service(Configuration,
				'root',
				None, # group
				None, # parent--root should use cli endpoint as parent / #42
				config.root['endpoint'],
			)

		self._add_service(Management, config_service, config)
		self._add_service(Filter, 'root', config_service, local_ep)
