from collections import namedtuple

from dcamp.util.functions import seconds_to_str, now_msecs, str_to_seconds

__all__ = [
    'EndpntSpec',
    'FilterSpec',
    'GroupSpec',
    'MetricSpec',
    'SerializableSpecTypes',
    'ThreshSpec',
    'MetricCollection',
]

GroupSpec = namedtuple('GroupSpec', ['endpoints', 'filters', 'metrics'])


class ThreshSpec(namedtuple('ThreshSpec', ['op', 'value'])):
    """Class representing a Threshold specification"""
    __slots__ = ()

    def check(self, value):
        if self.op in ['<', '>']:
            return self.__limit(value)
        elif self.op in ['+', '*']:
            return self.__timed(value)
        raise NotImplementedError('unknown threshold operation')

    def __str__(self):
        return '%s%s' % (self.op,
                         self.op in ['<', '>'] and self.value or seconds_to_str(self.value))

    @property
    def is_limit(self):
        return self.op in ['<', '>']

    @property
    def is_timed(self):
        return self.op in ['+', '*']

    def __limit(self, value):
        assert self.is_limit

        if '<' == self.op:
            return value <= self.value
        elif '>' == self.op:
            return value >= self.value
        raise NotImplementedError('unknown limit operation')

    def __timed(self, value):
        assert self.is_timed
        return value + (self.value * 1000) <= now_msecs()

    @classmethod
    def from_str(cls, given):
        assert isinstance(given, str)

        errmsg = None

        op = given[0]
        val_str = given[1:]
        value = None

        if op in ['+', '*']:
            try:
                value = str_to_seconds(val_str)
            except NotImplementedError:
                errmsg = 'time-based threshold specification contains invalid time'

        elif op in ['>', '<']:
            try:
                value = float(val_str)
            except ValueError:
                errmsg = 'value-based threshold specification contains invalid value'

        else:
            errmsg = 'invalid op for threshold specification'

        if errmsg:
            raise ValueError('%s: [%s]' % (errmsg, given))

        return cls(op, value)


class MetricSpec(namedtuple('MetricSpec', ['config_name', 'rate', 'threshold', 'detail', 'param'])):
    """ Class Representing a Metric Specification """
    __slots__ = ()

    def __str__(self):
        return "%s(detail='%s', rate='%s', threshold='%s', param='%s')" % (
            self.config_name, self.detail, seconds_to_str(self.rate),
            self.threshold, self.param or '')


class MetricCollection(namedtuple('MetricCollection', 'epoch, spec, p')):
    """ Class Representing a Metric Collection Specification """
    __slots__ = ()
    pass


class FilterSpec(namedtuple('FilterSpec', ['action', 'match'])):
    """ Class Representing a Filter Specification """
    __slots__ = ()

    def __str__(self):
        return "%s'%s'" % (self.action, self.match)


class EndpntSpec(namedtuple('EndpntSpec', ['host', 'port'])):
    """ Class Representing an Endpoint Specification """
    __slots__ = ()

    ### port offsets (for binding)

    BASE = 0  # Management (PUB) -----------connects-to--> Node (SUB)
    CONTROL = 1  # Master (DEALER) ------------connects-to--> Slave (ROUTER)
    #   + Election: Collector ----commands--> Collector
    #   + User:     CLI ----------commands--> Management
    #   + Normal:   Management ---commands--> Node
    #   + Join:     Base ---------commands--> Management

    CONFIG_UPDATE = 2  # Child (SUB) ----------------connects-to--> Parent (PUB)
    CONFIG_SNAPSHOT = 3  # Child (DEALER) -----------connects-to--> Parent (ROUTER)

    DATA_EXTERNAL = 4  # Filter (PUB) ---------------connects-to--> Aggregation (SUB)
    DATA_INTERNAL = 5  # Sensor|Aggregation (PUSH) --connects-to--> Filter (PULL)

    __RESERVED6__ = 6
    __RESERVED7__ = 7
    __RESERVED8__ = 8
    __RESERVED9__ = 9

    _valid_offsets = [
        BASE,
        CONTROL,

        CONFIG_UPDATE,
        CONFIG_SNAPSHOT,

        DATA_EXTERNAL,
        DATA_INTERNAL,

        __RESERVED6__,
        __RESERVED7__,
        __RESERVED8__,
        __RESERVED9__,
    ]

    MAX_OFFSET = len(_valid_offsets)

    def __str__(self):
        return "%s:%s" % (self.host, self.port)

    def _port(self, offset):
        assert offset in EndpntSpec._valid_offsets
        return self.port + offset

    def bind_uri(self, offset, protocol='tcp'):
        return self._uri(offset, protocol, op='bind')

    def connect_uri(self, offset, protocol='tcp'):
        return self._uri(offset, protocol, op='connect')

    def _uri(self, offset, protocol, op):
        assert op in ['bind', 'connect']
        return '%s://%s:%d' % (
            protocol,
            ('inproc' == protocol or 'connect' == op) and self.host or '*',
            self._port(offset))

    def encode(self):
        return str(self).encode()

    @classmethod
    def decode(cls, given):
        assert isinstance(given, bytes)
        return cls.from_str(given.decode())

    @classmethod
    def from_str(cls, given):
        assert isinstance(given, str)
        errmsg = None

        parts = given.split(':')
        if len(parts) != 2:
            errmsg = 'endpoint specification must be "host:port"'
        elif len(parts[0]) == 0:
            errmsg = 'endpoint specification must provide host name or ip address'
        elif not parts[1].isdecimal():
            errmsg = 'endpoint port must be an integer'

        if errmsg:
            raise ValueError('%s: [%s]' % (errmsg, given))

        return cls(parts[0], int(parts[1]))


SerializableSpecTypes = {
    'MetricSpec': MetricSpec,
    'FilterSpec': FilterSpec,
    'EndpntSpec': EndpntSpec
}
