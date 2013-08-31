from zhelpers import zpipe

from dcamp.role.role import Role
from dcamp.service.configuration import Configuration

class Metric(Role):
	'''
	Metric Role
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
				'leaf',
				group,
				parent_ep,	# collector endpoint
				local_ep,	# our endpoint
			)
