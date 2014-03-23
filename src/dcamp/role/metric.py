from dcamp.role.role import RoleMixin

from dcamp.service.configuration import Configuration
from dcamp.service.filter import Filter
from dcamp.service.sensor import Sensor


class Metric(RoleMixin):
    """
    Metric Role
    """

    def __init__(self,
                 control_pipe,
                 group,
                 parent_ep,
                 local_ep,
    ):
        RoleMixin.__init__(self, control_pipe)

        ### add services

        config_service = self._add_service(Configuration,
                                           'leaf',
                                           group,
                                           parent_ep,  # collector endpoint
                                           local_ep,  # our endpoint
                                           self.sos,
        )

        self._add_service(Filter, 'leaf', config_service, local_ep, parent_ep)
        self._add_service(Sensor, config_service, local_ep)
