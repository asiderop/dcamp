from dcamp.role.role import RoleMixin
from dcamp.service.node import Node


class Base(RoleMixin):
    """
    Base Role
    """

    def __init__(
            self,
            control_pipe,
            local_ep
    ):
        RoleMixin.__init__(self, control_pipe)

        #                (ServiceClass, config-service, local-ep, parent-ep, level   )
        self._add_service(Node,         None,           local_ep                     )
