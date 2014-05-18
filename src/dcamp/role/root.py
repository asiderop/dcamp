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
            local_uuid,
            config_file,
    ):
        RoleMixin.__init__(self, control_pipe, local_ep, local_uuid)

        ### add services (order matters)

        self._add_service(
            Configuration,
            None,         # parent endpoint--root should use cli endpoint as parent / #42
            'root',       # node's level
            None,         # node's group
            None,         # sos callable, None for root node
            config_file,  # configuration file, only for root node
        )

        self._add_service(Management)
        self._add_service(Filter, None, 'root')
        self._add_service(Aggregation, None, 'root')
