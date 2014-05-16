from uuid import UUID, uuid4

from dcamp.types.messages.common import DCMsg
from dcamp.types.specs import EndpntSpec
from dcamp.util.functions import isInstance_orNone

__all__ = [
    'gen_uuid',

    'TOPO',
    'MARCO',
    'GROUP',
    'RECOVERY',
]


def gen_uuid():
    return uuid4()

# TODO: the TOPO and CONTROL classes are very similar and can be comnbined perhaps.


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
