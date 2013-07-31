from zhelpers import zpipe

from dcamp.role.role import Role
from dcamp.service.config import Configuration

class Collector(Role):
	'''
	Collector Role
	'''

	def __init__(self,
			control_pipe,
			parent_ep,
			local_ep,
			):
		Role.__init__(self, control_pipe)

		(other_pipe, config_pipe) = zpipe(self.ctx) # socket pair for services to communicate with each other

		# add Configuration Service
		self._add_service(Configuration,
				config_pipe,
				'branch',	# our action
				parent_ep,	# root/collector endpoint
				local_ep,	# our endpoint
			)