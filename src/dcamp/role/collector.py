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
            local_uuid,
            parent_ep,
            group,
    ):
        RoleMixin.__init__(self, control_pipe, local_ep, local_uuid)

        ### add services

        self._add_service(
            Configuration,
            parent_ep,    # parent endpoint, root/collector node
            'branch',     # node's level
            group,        # node's group
            self.sos,     # sos callable, None for root node
            None,         # configuration file, only for root node
        )

        self._add_service(Filter, parent_ep, 'branch')
        self._add_service(Aggregation, parent_ep, 'branch')
