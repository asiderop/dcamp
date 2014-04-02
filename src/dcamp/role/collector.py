from dcamp.role.role import RoleMixin

from dcamp.service.configuration import Configuration
from dcamp.service.filter import Filter
from dcamp.service.aggregation import Aggregation


class Collector(RoleMixin):
    """
    Collector Role
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
                                           'branch',
                                           group,
                                           parent_ep,  # root/collector endpoint
                                           local_ep,  # our endpoint
                                           self.sos,
        )

        self._add_service(Filter,      'branch', config_service, local_ep, parent_ep)
        self._add_service(Aggregation, 'branch', config_service, local_ep, parent_ep)
