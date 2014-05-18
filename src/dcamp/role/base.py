from dcamp.role.role import RoleMixin
from dcamp.service.node import Node


class Base(RoleMixin):
    """
    Base Role
    """

    def __init__(
            self,
            control_pipe,
            local_ep,
            local_uuid,
    ):
        RoleMixin.__init__(self, control_pipe, local_ep, local_uuid)

        self._add_service(Node)
