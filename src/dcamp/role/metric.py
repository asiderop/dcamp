from zhelpers import zpipe

from dcamp.role.role import Role
from dcamp.service.configuration import Configuration
from dcamp.service.sensor import Sensor
from dcamp.service.filter import Filter

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
		config_service = self._add_service(Configuration,
				'leaf',
				group,
				parent_ep,	# collector endpoint
				local_ep,	# our endpoint
			)

		# add Sensor Service
		self._add_service(Sensor, config_service, local_ep)
		self._add_service(Filter, config_service, local_ep)
