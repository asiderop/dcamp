from uuid import UUID

from dcamp.types.messages.common import DCMsg, _PROPS


class CONFIG(DCMsg, _PROPS):

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

    def __str__(self):
        raise NotImplementedError('sub-class must implement __str__()')

    @property
    def is_hugz(self):
        return isinstance(self, HUGZ)

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
    def from_msg(cls, msg, peer_id):
        assert isinstance(msg, list)

        # make sure we have five frames
        if 5 != len(msg):
            raise ValueError('wrong number of frames')

        key = msg[0].decode()
        seq = DCMsg._decode_int(msg[1])
        uuid = DCMsg._decode_uuid(msg[2])
        props = _PROPS._decode_dict(msg[3])
        val = DCMsg._decode_blob(msg[4])

        if key.endswith('/HUGZ'):
            # common case
            return HUGZ(key.rsplit('/', 1)[0])  # drop the last part, i.e. '/HUGZ'
        elif 'ICANHAZ' == key:
            return ICANHAZ(val)
        elif 'KTHXBAI' == key:
            return KTHXBAI(seq, peer_id, val)
        elif peer_id is not None:
            return KVSYNC(key, val, seq, peer_id, props)
        else:
            return KVPUB(key, val, seq, uuid, props)


class HUGZ(CONFIG):
    """ TODO: add sequence numbers to heartbeats? """
    def __init__(self, t):
        assert isinstance(t, str)
        CONFIG.__init__(self, key='%s/HUGZ' % t)

    def __str__(self):
        return self.key


class ICANHAZ(CONFIG):
    def __init__(self, subtree=None):
        CONFIG.__init__(self, key='ICANHAZ', value=subtree)

    def __str__(self):
        return '%s %s' % (self.key, self.value)


class KTHXBAI(CONFIG):
    def __init__(self, seq, pid, subtree=None):
        CONFIG.__init__(self, key='KTHXBAI', value=subtree, sequence=seq, peer_id=pid)

    def __str__(self):
        return '%s %s' % (self.key, self.value)


class KVSYNC(CONFIG):
    def __init__(self, k, v, seq, pid, props=None):
        CONFIG.__init__(self, key=k, value=v, sequence=seq, peer_id=pid, properties=props)

    def __str__(self):
        result = '#%d: %s = %s' % (self.sequence, self.key, self.value)
        for (prop, val) in self.properties.items():
            result += '\n%s : %s' % (prop, val)
        return result


class KVPUB(CONFIG):
    def __init__(self, k, v, seq, uuid=None, props=None):
        CONFIG.__init__(self, key=k, value=v, sequence=seq, uuid=uuid, properties=props)

    def __str__(self):
        result = '#%d: %s = %s' % (self.sequence, self.key, self.value)
        if self.uuid is not None:
            result += ' (%s)' % self.uuid
        for (prop, val) in self.properties.items():
            result += '\n%s : %s' % (prop, val)
        return result
