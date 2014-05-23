from uuid import UUID

from dcamp.types.messages.common import DCMsg, _PROPS, WTF
from dcamp.types.specs import EndpntSpec

__all__ = [
    'CONTROL',
    'POLO',
    'SOS',
    'STOP',
    'ASSIGN',
]


class CONTROL(DCMsg, _PROPS):

    TOPO_COMMANDS = ['polo', 'assignment', 'stop']
    RECO_COMMANDS = ['sos', 'keepcalm', 'yo']

    def __init__(self, command, endpoint, uuid, properties=None):
        assert command in CONTROL.TOPO_COMMANDS + CONTROL.RECO_COMMANDS
        assert isinstance(endpoint, EndpntSpec)
        assert isinstance(uuid, UUID)
        DCMsg.__init__(self)
        _PROPS.__init__(self, properties)
        self.command = command
        self.endpoint = endpoint
        self.uuid = uuid

    def __str__(self):
        return '{} ({}, {}, props={})'.format(
            self.command, self.endpoint, self.uuid, self.properties)

    @property
    def frames(self):
        return [
            self.command.encode(),
            self.endpoint.encode(),
            self._encode_uuid(self.uuid),
            self._encode_dict(self.properties),
        ]

    @classmethod
    def from_msg(cls, msg, peer_id):
        # make sure we have four frames
        assert isinstance(msg, list)

        if 4 != len(msg):
            raise ValueError('wrong number of frames')

        cmd = msg[0].decode()
        ep = EndpntSpec.decode(msg[1])
        uuid = DCMsg._decode_uuid(msg[2])
        props = _PROPS._decode_dict(msg[3])

        return CONTROL(command=cmd, endpoint=ep, uuid=uuid, properties=props)

    @property
    def is_polo(self):
        return 'polo' == self.command

    @property
    def is_assign(self):
        return 'assignment' == self.command

    @property
    def is_stop(self):
        return 'stop' == self.command

    @property
    def is_sos(self):
        return 'sos' == self.command

    @property
    def is_yo(self):
        return 'yo' == self.command


###################
# Topology Messages
##

class POLO(CONTROL):
    def __init__(self, endpoint, uuid):
        CONTROL.__init__(self, command='polo', endpoint=endpoint, uuid=uuid)

    @classmethod
    def recv(cls, socket):
        msg = CONTROL.recv(socket)
        if 'polo' != msg.command:
            msg = WTF(0, 'expected polo, received ' + msg.command)
        return msg


class ASSIGN(CONTROL):
    def __init__(self, endpoint, uuid, parent_endpoint, level, group):
        assert level in ['root', 'branch', 'leaf']
        if not isinstance(parent_endpoint, EndpntSpec):
            assert isinstance(parent_endpoint, str)
            parent_endpoint = EndpntSpec.from_str(parent_endpoint)

        props = {
            'parent': parent_endpoint,
            'level': level,
            'group': group,
        }

        CONTROL.__init__(self, command='assignment', endpoint=endpoint, uuid=uuid, properties=props)


class STOP(CONTROL):
    def __init__(self, endpoint, uuid):
        CONTROL.__init__(self, command='stop', endpoint=endpoint, uuid=uuid)


###################
# Recovery Messages
##

class SOS(CONTROL):
    def __init__(self, endpoint, uuid):
        CONTROL.__init__(self, command='sos', endpoint=endpoint, uuid=uuid)

    @classmethod
    def recv(cls, socket):
        msg = CONTROL.recv(socket)
        if 'sos' != msg.command:
            msg = WTF(0, 'expected sos, received ' + msg.command)
        return msg


class YO(CONTROL):
    def __init__(self, endpoint, uuid, elect_uuid):

        props = {
            'election-uuid': str(elect_uuid),
        }

        CONTROL.__init__(self, command='yo', endpoint=endpoint, uuid=uuid, properties=props)
