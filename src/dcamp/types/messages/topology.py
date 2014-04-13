from uuid import UUID, uuid4

from dcamp.types.messages.common import DCMsg, _PROPS, WTF
from dcamp.types.specs import EndpntSpec
from dcamp.util.functions import isInstance_orNone

__all__ = [
    'gen_uuid',

    'TOPO'
    'MARCO',
    'GROUP',
    'RECOVERY',

    'CONTROL',
    'POLO',
    'SOS',
    'STOP',
    'ASSIGN',
]


def gen_uuid():
    return uuid4()


class TOPO(DCMsg):
    def __init__(self, key, ep, uuid, content=None):
        DCMsg.__init__(self)
        assert isinstance(key, str)
        assert isinstance(ep, EndpntSpec)
        assert isinstance(uuid, UUID)
        assert isInstance_orNone(content, (int, str))

        self.key = key
        self.endpoint = ep
        self.uuid = uuid
        self.content = content

    def __str__(self):
        return '{} ({}, {}, content={})'.format(
            self.key, self.endpoint, self.uuid, self.content)

    @property
    def frames(self):
        content = self.content is None and '' or str(self.content)
        return [
            self.key.encode(),
            self.endpoint.encode(),
            self._encode_uuid(self.uuid),
            content.encode(),
        ]

    @classmethod
    def from_msg(cls, msg, peer_id):
        assert isinstance(msg, list)

        # make sure we have exactly four frames
        if 4 != len(msg):
            raise ValueError('wrong number of frames')

        key = msg[0].decode()
        ep = EndpntSpec.decode(msg[1])
        uuid = DCMsg._decode_uuid(msg[2])
        content = msg[3].decode()
        if len(content) == 0:
            content = None

        return TOPO(key, ep, uuid, content)

    @staticmethod
    def marco_key():
        return '/MARCO'

    @staticmethod
    def group_key(group):
        return '/GROUP/' + group

    @staticmethod
    def recovery_key(msg_type=''):
        return '/RECOVERY/' + msg_type


class MARCO(TOPO):
    def __init__(self, endpoint, uuid, content=0):
        TOPO.__init__(self, TOPO.marco_key(), endpoint, uuid, content)

    def send(self, socket):
        result = TOPO.send(self, socket)
        self.content += 1
        return result


class GROUP(TOPO):
    def __init__(self, group, endpoint, uuid, content=None):
        key = TOPO.group_key(group)
        TOPO.__init__(self, key, endpoint, uuid, content)


class RECOVERY(TOPO):
    def __init__(self, msg_type, endpoint, uuid, content=None):
        key = TOPO.recovery_key(msg_type)
        TOPO.__init__(self, key, endpoint, uuid, content)


class CONTROL(DCMsg, _PROPS):
    def __init__(self, command, endpoint, uuid, properties=None):
        assert command in ['polo', 'assignment', 'stop', 'sos', 'keepcalm']
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


class POLO(CONTROL):
    def __init__(self, endpoint, uuid):
        CONTROL.__init__(self, command='polo', endpoint=endpoint, uuid=uuid)

    @classmethod
    def recv(cls, socket):
        msg = CONTROL.recv(socket)
        if 'polo' != msg.command:
            msg = WTF(0, 'expected polo, received ' + msg.command)
        return msg


class SOS(CONTROL):
    def __init__(self, endpoint, uuid):
        CONTROL.__init__(self, command='sos', endpoint=endpoint, uuid=uuid)


class STOP(CONTROL):
    def __init__(self, endpoint, uuid):
        CONTROL.__init__(self, command='stop', endpoint=endpoint, uuid=uuid)


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
