from zhelpers import zpipe

from dcamp.role.role import Role
from dcamp.service.configuration import Configuration

class Collector(Role):
	'''
	Collector Role
	'''

	def __init__(self,
			control_pipe,
			group,
			parent_ep,
			local_ep,
			):
		Role.__init__(self, control_pipe)

		# add Configuration Service
		self._add_service(Configuration,
				'branch',
				group,
				parent_ep,	# root/collector endpoint
				local_ep,	# our endpoint
			)
