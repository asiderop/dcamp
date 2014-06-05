from dcamp.types.messages.common import DCMsg, _PROPS
from dcamp.types.specs import EndpntSpec
from dcamp.util.functions import isInstance_orNone, now_msecs

__all__ = [
    'DataHugz',
    'DataBasic',
    'DataDelta',
    'DataRate',
    'DataAverage',
    'DataPercent',
    'Data',
]


class Data(DCMsg, _PROPS):
    """
    Frame 0: data source (leaf or collector node endpoint), as 0MQ string
    Frame 1: properties, as 0MQ string
    Frame 2: time in ms epoch utc, 8 bytes in network order
    Frame 3: value, 8 bytes in network order
    Frame 4: base value, 8 bytes in network order; only for average and percent types

    properties = *( type / detail / config / seqid )
    type       = "type=" ( "HUGZ" / "basic" / "delta" / "rate" / "average" / "percent" )
    detail     = "detail=" <string>
    config     = "config-name=" <string>
    seqid      = "config-seqid=" <integer>
    """

    def __init__(self, source, properties, time=None, value=None, base_value=None):
        DCMsg.__init__(self)
        _PROPS.__init__(self, properties)

        assert isinstance(source, EndpntSpec)

        # validate given type
        assert 'type' in properties, 'missing metric "type" key'
        assert self.m_type in _MTYPES.keys(), 'given metric "type" not valid'

        # validate class was constructed with type-appropriate sub-class
        assert isinstance(self, _MTYPES[self.m_type])

        assert isInstance_orNone(time, int)
        assert isInstance_orNone(value, int)
        assert isInstance_orNone(base_value, int)

        # TODO: add more verifications of parameters based on given m_type

        self.source = source

        self.time = time
        self.value = value
        self.base_value = base_value

    @property
    def m_type(self):
        return self['type']

    @property
    def detail(self):
        return self.get('detail', None)

    @property
    def config_name(self):
        return self.get('config-name', None)

    @property
    def config_seqid(self):
        return self.get('config-seqid', None)

    @property
    def is_hugz(self):
        return isinstance(self, DataHugz)

    def __is_compatible(self, given):
        return (self.source == given.source and
                self.m_type == given.m_type and
                self.time < given.time)

    def print(self, given):
        """
        Return value is a string representation of the calculated value and a contextual
        suffix.
        """
        return '%.2f %s' % (self.calculate(given), self.suffix)

    def calculate(self, given):
        """
        Calculates a value based on this and the given data message. The given message
        must be from the same source, have the same message type, and have a later
        timestamp. Return value is a float representation of calculated result.
        """
        assert self.__is_compatible(given)
        return self._calculate(given)

    def _calculate(self, given):
        raise NotImplementedError('sub-class implementation missing')

    @property
    def suffix(self):
        raise NotImplementedError('sub-class implementation missing')

    def __str__(self):
        return '%s -- %s @ %d = %d' % (self.source, self.detail, self.time, self.value)

    def log_str(self):
        return '%d\t%s\t%s\t%d' % (self.time, self.source, self.detail, self.value)

    @property
    def frames(self):
        return [
            self.source.encode(),
            self._encode_dict(self.properties),
            self._encode_int(self.time),
            self._encode_int(self.value),
            self._encode_int(self.base_value),
        ]

    @classmethod
    def from_msg(cls, msg, peer_id):
        assert isinstance(msg, list)

        # make sure we have six frames
        if 5 != len(msg):
            raise ValueError('wrong number of frames')

        source = EndpntSpec.decode(msg[0])
        props = _PROPS._decode_dict(msg[1])

        # validate given type
        assert 'type' in props, 'missing metric "type" key'
        assert props['type'] in _MTYPES.keys(), 'given metric "type" not valid'

        real_class = _MTYPES[props['type']]

        time = DCMsg._decode_int(msg[2])

        # HUGZ have a special/minimal constructor
        if real_class == DataHugz:
            return real_class(source, time)

        value = DCMsg._decode_int(msg[3])
        base_value = DCMsg._decode_int(msg[4])

        return real_class(source, props, time, value, base_value)


class DataHugz(Data):
    def __init__(self, the_source, the_time=None):
        super().__init__(
            source=the_source,
            properties={'type': 'HUGZ'},
            time=the_time or now_msecs()
        )

    def _calculate(self, given):
        raise NotImplementedError('HUGZ have no value')

    @property
    def suffix(self):
        raise NotImplementedError('HUGZ have no suffix')

    def __str__(self):
        return '%s -- HUGZ @ %d' % (str(self.source), self.time)

    def log_str(self):
        return '%d\t%s\t%s' % (self.time, self.source, self.m_type)


class DataBasic(Data):
    def _calculate(self, given=None):
        return float(self.value)

    @property
    def suffix(self):
        return ''


class DataDelta(Data):
    def _calculate(self, given):
        return float(given.value - self.value)

    @property
    def suffix(self):
        return ''


class DataAverage(Data):
    def __str__(self):
        return '%s / %d' % (Data.__str__(self), self.base_value)

    def log_str(self):
        return '%s\t%d' % (Data.log_str(self), self.base_value)

    @property
    def suffix(self):
        return 'average'

    def _calculate(self, given):
        return float((given.value - self.value) / (given.base_value - self.base_value))


class DataPercent(Data):
    def __str__(self):
        return '%s / %d' % (Data.__str__(self), self.base_value)

    def log_str(self):
        return '%s\t%d' % (Data.log_str(self), self.base_value)

    @property
    def suffix(self):
        return '%'

    def _calculate(self, given):
        return float((given.value - self.value) / (given.base_value - self.base_value)) * 100.0


class DataRate(Data):
    def __the_rate(self, given):
        return float((given.value - self.value) / (given.time - self.time) * 1e3)

    @property
    def suffix(self):
        return '/ sec'

    def _calculate(self, given):
        return self.__the_rate(given)


class DataAggregate(Data):
    def __init__(self, source, properties, time=None, value=None, base_value=None):
        Data.__init__(self, source, properties, time, value, base_value)
        assert self.m_type != 'HUGZ'
        assert 'aggr-id' in properties
        assert 'is-final' in properties

        # { EndpntSpec : Data }
        self._nodes = {}

        if self['is-final']:
            assert value is not None
        else:
            assert value is None and base_value is None

    def add_sample(self, msg):
        assert not self['is-final']
        # TODO: check the new msg matches previous ones
        self._nodes[msg.source] = msg

    def _aggregate(self):
        assert len(self._nodes) > 0
        if not self['is-final']:
            self._do_aggregation()
            self['is-final'] = True
            self['node-cnt'] = len(self._nodes)

    def _do_aggregation(self):
        raise NotImplementedError('subclass must implement')


class DataAggregateSum(DataAggregate, DataBasic):

    def _do_aggregation(self):
        self.value = 0
        self.base_value = 0

        # add all samples together
        for msg in self._nodes.values():
            self.value += msg.value,
            self.base_value += msg.base_value,


class DataAggregateMax(DataAggregate, DataBasic):
    def _do_aggregation(self):
        self.value = 0
        self.base_value = 0

        msg_zero = None
        msg_max = None
        calc_max = 0

        # find largest sample (doing calculation with empty sample)
        for msg in self._nodes.values():
            if msg_zero is None:
                # create fake "zero" message for calculation purposes
                msg_zero = msg.__class__(msg.source, msg.properties, 0, 0, 0)

            calc = abs(msg.calculate(msg_zero))
            if calc > calc_max:
                msg_max = msg

        self.value = msg_max.value
        self.base_value = msg_max.base_value


class DataAggregateMin(DataAggregate, DataBasic):
    def _do_aggregation(self):
        self.value = 0
        self.base_value = 0

        msg_zero = None
        msg_min = None
        calc_min = 0

        # find largest sample (doing calculation with empty sample)
        for msg in self._nodes.values():
            if msg_zero is None:
                # create fake "zero" message for calculation purposes
                msg_zero = msg.__class__(msg.source, msg.properties, 0, 0, 0)

            calc = abs(msg.calculate(msg_zero))
            if calc < calc_min:
                msg_min = msg

        self.value = msg_min.value
        self.base_value = msg_min.base_value


class DataAggregateAvg(DataAggregate, DataAverage):

    def _do_aggregation(self):
        self.value = 0
        self.base_value = 0

        for msg in self._nodes.values():
            self.value += msg.value,
            self.base_value += msg.base_value,

        self.value /= len(self._nodes)
        self.base_value /= len(self._nodes)


_MTYPES = {
    'HUGZ': DataHugz,

    'basic': DataBasic,
    'delta': DataDelta,
    'rate': DataRate,
    'average': DataAverage,
    'percent': DataPercent,

    'aggregate-sum': DataAggregateSum,
    'aggregate-max': DataAggregateMax,
    'aggregate-min': DataAggregateMin,
    'aggregate-avg': DataAggregateAvg,
}
