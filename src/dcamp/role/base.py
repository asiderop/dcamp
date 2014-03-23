from dcamp.role.role import RoleMixin
from dcamp.service.node import Node


class Base(RoleMixin):
    """
    Base Role
    """

    def __init__(self,
                 pipe,
                 address):
        RoleMixin.__init__(self, pipe)

        # add Node Service
        self._add_service(Node, address)
