from uuid import UUID, uuid4

from dcamp.types.messages.common import DCMsg, _PROPS
from dcamp.types.specs import EndpntSpec
from dcamp.util.functions import isInstance_orNone

# @todo: need to include UUIDs in each message so nodes can distinguish between multiple invocations of the same
#        endpoint

__all__ = [
    'gen_uuid',

    'MARCO',
    'POLO',
    'CONTROL',
    'STOP',
    'ASSIGN',
]


def gen_uuid():
    return uuid4()


class Topo(DCMsg):
    def __init__(self, ep, uuid, content=None):
        DCMsg.__init__(self)
        assert isinstance(ep, EndpntSpec)
        assert isinstance(uuid, UUID)
        assert isInstance_orNone(content, str)

        self.endpoint = ep
        self.uuid = uuid
        self.content = content

    def __str__(self):
        return '%s (%s, content=%s)' % (self.endpoint, self.uuid, self.content)

    @property
    def frames(self):
        content = self.content or ''
        return [
            self.endpoint.encode(),
            self._encode_uuid(self.uuid),
            content.encode(),
        ]

    @classmethod
    def from_msg(cls, msg, peer_id):
        assert isinstance(msg, list)

        # make sure we have exactly two frames
        if 3 != len(msg):
            raise ValueError('wrong number of frames')

        ep = EndpntSpec.decode(msg[0])
        uuid = DCMsg._decode_uuid(msg[1])
        cont = msg[2].decode()
        if len(cont) == 0:
            cont = None

        return cls(ep, uuid, cont)


# @todo: The MARCO and POLO message types are really the same message structure. These two
#        message classes should just be combined.

class MARCO(Topo):
    def __init__(self, root_endpoint, root_uuid, content=None):
        Topo.__init__(self, root_endpoint, root_uuid, content)


class POLO(Topo):
    def __init__(self, base_endpoint, base_uuid, content=None):
        Topo.__init__(self, base_endpoint, base_uuid, content)


class CONTROL(DCMsg, _PROPS):
    def __init__(self, command, properties=None):
        assert command in ['assignment', 'stop', 'keepcalm']
        DCMsg.__init__(self)
        _PROPS.__init__(self, properties)
        self.command = command

    @property
    def frames(self):
        return [
            self.command.encode(),
            self._encode_dict(self.properties),
        ]

    @classmethod
    def from_msg(cls, msg, peer_id):
        # make sure we have two frames
        assert isinstance(msg, list)

        if 2 != len(msg):
            raise ValueError('wrong number of frames')

        cmd = msg[0].decode()
        props = _PROPS._decode_dict(msg[1])

        return cls(command=cmd, properties=props)


def STOP():
    return CONTROL(command='stop')


def ASSIGN(parent_endpoint, level, group):
    assert level in ['root', 'branch', 'leaf']
    if not isinstance(parent_endpoint, EndpntSpec):
        assert isinstance(parent_endpoint, str)
        parent_endpoint = EndpntSpec.from_str(parent_endpoint)

    props = {
        'parent': parent_endpoint,
        'level': level,
        'group': group,
    }

    return CONTROL(command='assignment', properties=props)
