from dcamp.role.role import RoleMixin

from dcamp.service.configuration import Configuration
from dcamp.service.filter import Filter
from dcamp.service.aggregation import Aggregation


class Collector(RoleMixin):
    """
    Collector Role
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

        self.__config_service = self._add_service(
            Configuration,
            None,         # config service, None for config service (obviously)
            local_ep,     # our endpoint
            parent_ep,    # parent endpoint, root/collector node
            'branch',     # node's level
            group,        # node's group
            self.sos,     # sos callable, None for root node
            None,         # configuration file, only for root node
        )

        #                (ServiceClass, config-service, local-ep, parent-ep, level   )
        self._add_service(Filter,       self.__config_service, local_ep, parent_ep, 'branch')
        self._add_service(Aggregation,  self.__config_service, local_ep, parent_ep, 'branch')

    def get_config_service_kvdict(self):
        return self.__config_service.copy_kvdict()

    def get_config_service(self):
        return self.__config_service
