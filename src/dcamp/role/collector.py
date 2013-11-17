from zhelpers import zpipe

from dcamp.role.role import Role_Mixin

from dcamp.service.configuration import Configuration
from dcamp.service.filter import Filter
from dcamp.service.sensor import Sensor

class Collector(Role_Mixin):
	'''
	Collector Role
	'''

	def __init__(self,
			control_pipe,
			group,
			parent_ep,
			local_ep,
			):
		Role_Mixin.__init__(self, control_pipe)

		### add services

		config_service = self._add_service(Configuration,
				'branch',
				group,
				parent_ep,	# root/collector endpoint
				local_ep,	# our endpoint
			)

		self._add_service(Filter, 'branch', config_service, local_ep, parent_ep)
		self._add_service(Sensor, config_service, local_ep)
