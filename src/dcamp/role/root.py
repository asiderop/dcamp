from dcamp.role.role import Role_Mixin
from dcamp.service.configuration import Configuration
from dcamp.service.management import Management
from dcamp.service.filter import Filter
from dcamp.service.aggregation import Aggregation


class Root(Role_Mixin):
    """
    Root Role
    """

    def __init__(self,
                 control_pipe,
                 config):
        Role_Mixin.__init__(self, control_pipe)

        local_ep = config.root['endpoint']

        ### add services

        config_service = self._add_service(Configuration,
                                           'root',
                                           None,  # group
                                           None,  # parent--root should use cli endpoint as parent / #42
                                           local_ep,
        )

        self._add_service(Management, config_service, config)
        self._add_service(Filter, 'root', config_service, local_ep, None)
        self._add_service(Aggregation, 'root', config_service, local_ep, None)
