from dcamp.role.role import RoleMixin

from dcamp.service.configuration import Configuration
from dcamp.service.filter import Filter
from dcamp.service.sensor import Sensor


class Metric(RoleMixin):
    """
    Metric Role
    """

    def __init__(
            self,
            control_pipe,
            local_ep,
            parent_ep,
            group,
    ):
        RoleMixin.__init__(self, control_pipe)

        ### add services

        config_service = self._add_service(
            Configuration,
            None,         # config service, None for config service (obviously)
            local_ep,     # our endpoint
            parent_ep,    # parent endpoint, root/collector node
            'leaf',       # node's level
            group,        # node's group
            self.sos,     # sos callable, None for root node
            None,         # configuration file, only for root node
        )

        #                (ServiceClass, config-service, local-ep, parent-ep, level   )
        self._add_service(Filter,       config_service, local_ep, parent_ep, 'leaf'  )
        self._add_service(Sensor,       config_service, local_ep                     )
