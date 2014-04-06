from dcamp.role.role import RoleMixin
from dcamp.service.configuration import Configuration
from dcamp.service.management import Management
from dcamp.service.filter import Filter
from dcamp.service.aggregation import Aggregation


class Root(RoleMixin):
    """
    Root Role
    """

    def __init__(
            self,
            control_pipe,
            local_ep,
            config_file,
    ):

        RoleMixin.__init__(self, control_pipe)

        ### add services

        config_service = self._add_service(
            Configuration,
            None,         # config service, None for config service (obviously)
            local_ep,     # our endpoint
            None,         # parent endpoint--root should use cli endpoint as parent / #42
            'root',       # node's level
            None,         # node's group
            None,         # sos callable, None for root node
            config_file,  # configuration file, only for root node
        )

        #                (ServiceClass, config-service, local-ep, parent-ep, level   )
        self._add_service(Management,   config_service, local_ep                     )
        self._add_service(Filter,       config_service, local_ep, None,      'root'  )
        self._add_service(Aggregation,  config_service, local_ep, None,      'root'  )
