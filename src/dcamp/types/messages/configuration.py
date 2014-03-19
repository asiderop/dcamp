from uuid import UUID

from dcamp.types.messages.common import DCMsg, _PROPS


class CONFIG(DCMsg, _PROPS):
    ICANHAZ = 0
    KVSYNCPUB = 1
    KTHXBAI = 2
    HUGZ = 3

    ctypes = [
        ICANHAZ,
        KVSYNCPUB,
        KTHXBAI,
        HUGZ,
    ]

    def __init__(self, key, value=None, sequence=0, uuid=None, properties=None, peer_id=None):
        DCMsg.__init__(self, peer_id)
        _PROPS.__init__(self, properties)
        assert isinstance(key, str)
        assert isinstance(sequence, int)
        assert isinstance(uuid, (UUID, type(None)))

        self.key = key
        self.sequence = sequence
        self.uuid = uuid
        self.value = value

        if 'ICANHAZ' == key:
            self.ctype = CONFIG.ICANHAZ
        elif 'KTHXBAI' == key:
            self.ctype = CONFIG.KTHXBAI
        elif 'HUGZ' == key:
            self.ctype = CONFIG.HUGZ
        else:
            # if not one of the "known" keys, assume sync/pub message
            self.ctype = CONFIG.KVSYNCPUB

    def __str__(self):
        if self.key in ['ICANHAZ', 'KTHXBAI']:
            return '%s %s' % (self.key, self.value)
        elif 'HUGZ' == self.key:
            return self.key
        else:
            result = '#%d: %s = %s' % (self.sequence, self.key, self.value)
            if self.uuid is not None:
                result += ' (%s)' % self.uuid
            for (prop, val) in self.properties.items():
                result += '\n%s : %s' % (prop, val)
            return result

    @property
    def frames(self):
        return [
            self.key.encode(),
            self._encode_int(self.sequence),
            self._encode_uuid(self.uuid),
            self._encode_dict(self.properties),
            self._encode_blob(self.value),
        ]

    @classmethod
    def from_msg(cls, msg):
        assert isinstance(msg, list)

        # make sure we have five frames
        if 5 != len(msg):
            raise ValueError('wrong number of frames')

        key = msg[0].decode()
        seq = DCMsg._decode_int(msg[1])
        uuid = DCMsg._decode_uuid(msg[2])
        props = _PROPS._decode_dict(msg[3])
        val = DCMsg._decode_blob(msg[4])

        return cls(key, val, seq, uuid, properties=props)


class ICANHAZ(CONFIG):
    def __init__(self, subtree=None):
        CONFIG.__init__(self, key='ICANHAZ', value=subtree)


class KVSYNC(CONFIG):
    def __init__(self, k, v, seq, pid):
        CONFIG.__init__(self, key=k, value=v, sequence=seq, peer_id=pid)


class KTHXBAI(CONFIG):
    def __init__(self, seq, pid, subtree=None):
        CONFIG.__init__(self, key='KTHXBAI', value=subtree, sequence=seq, peer_id=pid)


class KVPUB(CONFIG):
    def __init__(self, k, v, seq, uuid=None):
        CONFIG.__init__(self, key=k, value=v, sequence=seq, uuid=uuid)


class HUGZ(CONFIG):
    """ TODO: add sequence numbers to heartbeats? """
    def __init__(self, ):
        CONFIG.__init__(self, key='HUGZ')
