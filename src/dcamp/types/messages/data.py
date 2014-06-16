from dcamp.types.messages.common import DCMsg, _PROPS, dev_mode
from dcamp.types.specs import EndpntSpec
from dcamp.util.functions import isInstance_orNone, now_msecs, get_logger_from_caller

__all__ = [
    'Data',

    'DataHugz',

    'DataBasic',
    'DataDelta',
    'DataRate',
    'DataAverage',
    'DataPercent',

    'DataAggregate',
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
        self.source = source

        # validate properties
        req_props = ['type', 'detail', 'config-name', 'config-seqid']
        if self.is_hugz:
            req_props = ['type']
        for p in req_props:
            assert p in properties, 'missing metric "{}" key'.format(p)

        assert self.m_type in _MTYPES.keys(), 'given metric "type" not valid'

        # validate class was constructed with type-appropriate sub-class
        assert isinstance(self, _MTYPES[self.m_type])

        assert isInstance_orNone(time, int)
        self.time = time

        if isinstance(value, int):
            value = float(value)
        assert isInstance_orNone(value, float)
        self.value = value

        if isinstance(base_value, int):
            base_value = float(base_value)
        assert isInstance_orNone(base_value, float)
        self.base_value = base_value

        # TODO: add more verifications of parameters based on given m_type

    def __eq__(self, other):
        if not isinstance(self, self.__class__):
            return False
        this = (self.source, self.properties, self.time, self.value, self.base_value)
        that = (other.source, other.properties, other.time, other.value, other.base_value)
        return this == that

    def __ne__(self, other):
        return self != other

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
        return '%s -- %s [%d] @ %d = %.2f' % (self.source,
                                              self.detail,
                                              self.config_seqid,
                                              self.time,
                                              self.value)

    def log_str(self):
        props = {
            'source': self.source,
            'value': self.value,
            'base': self.base_value,
        }
        props.update(self.properties)
        logstr = '{}'.format(self.time)
        for (k, v) in props.items():
            logstr += ' {}={}'.format(k, v)
        return logstr

    @property
    def frames(self):
        return [
            self.source.encode(),
            self._encode_dict(self.properties),
            self._encode_uint(self.time),
            self._encode_float(self.value),
            self._encode_float(self.base_value),
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

        time = DCMsg._decode_uint(msg[2])

        # HUGZ have a special/minimal constructor
        if real_class == DataHugz:
            return real_class(source, time)

        value = DCMsg._decode_float(msg[3])
        base_value = DCMsg._decode_float(msg[4])

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
        raise NotImplementedError('HUGZ has no log value')


class DataBasic(Data):
    def _calculate(self, given=None):
        return self.value

    @property
    def suffix(self):
        return ''


class DataDelta(Data):
    def _calculate(self, given):
        return given.value - self.value

    @property
    def suffix(self):
        return ''


class DataAverage(Data):
    def __str__(self):
        return '%s / %.2f' % (Data.__str__(self), self.base_value)

    @property
    def suffix(self):
        return 'average'

    def _calculate(self, given):
        numerator = given.value - self.value
        denominator = given.base_value - self.base_value
        if 0 == denominator:
            return 0.0
        return numerator / denominator


class DataPercent(DataAverage):
    def __str__(self):
        return '%s / %.2f' % (Data.__str__(self), self.base_value)

    @property
    def suffix(self):
        return '%'

    def _calculate(self, given):
        return super()._calculate(given) * 100.0


class DataRate(Data):
    def __the_rate(self, given):
        numerator = given.value - self.value
        denominator = given.time - self.time
        if 0 == denominator:
            return 0.0
        return (numerator / denominator) * 1e3

    @property
    def suffix(self):
        return '/ sec'

    def _calculate(self, given):
        return self.__the_rate(given)


class DataAggregate(DataBasic):
    def __init__(self, source, properties, time=None, value=None, base_value=None):
        DataBasic.__init__(self, source, properties, time, value, base_value)
        assert self.m_type.startswith('aggregate')
        assert 'aggr-id' in properties
        assert base_value is None

        # { EndpntSpec : [ Data, ... ] }
        self._samples_cache = {}

        if 'is-final' in properties:
            assert self['is-final']
            assert 'samples-type' in properties
            assert 'node-cnt' in properties
            assert 'aggr-source' in properties
            assert value is not None
            assert time is not None
        else:
            self['is-final'] = False
            self['samples-type'] = None
            assert value is None
            assert time is None

    def add_sample(self, msg):
        assert not self['is-final']

        if self['samples-type'] is None:
            assert msg.m_type != 'HUGZ'
            self['samples-type'] = msg.m_type
        else:
            assert msg.m_type == self['samples-type']

        if msg.source not in self._samples_cache:
            # add empty list to dict
            self._samples_cache[msg.source] = []

        # cache at most two samples, keeping the first and last
        cache = self._samples_cache[msg.source]
        if len(cache) < 2:
            cache.append(msg)
        else:
            assert msg.time > cache[1].time
            cache[1] = msg
            assert cache[1].time > cache[0].time

    def reset(self):
        # clear properties state
        for p in ('node-cnt', 'aggr-source'):
            try:
                del(self[p])
            except KeyError:
                continue
        self['is-final'] = False

        # clear samples (only keep last sample)
        for (node, cache) in self._samples_cache.items():
            if len(cache) == 2:
                cache.pop(0)
            assert len(cache) == 1

        # clear time, value, base_value
        self.value = None
        self.base_value = None
        self.time = None

    def aggregate(self, time):

        if self['is-final']:
            return self.value

        source = None
        value = None

        op = self.m_type[len('aggregate-'):]

        node_cnt = 0
        # find largest sample (doing calculation)
        for (node, cache) in self._samples_cache.items():
            if len(cache) < 2:
                continue
            node_cnt += 1
            calc = cache[0].calculate(cache[1])

            if value is None:
                value = calc
            elif op in ('sum', 'avg'):
                value += calc
                source = self.source
            elif op == 'min':
                if calc < value:
                    value = calc
                    source = node
            elif op == 'max':
                if calc > value:
                    value = calc
                    source = node
            else:
                raise NotImplementedError('unknown aggregation type: {}'.format(op))

        if node_cnt < 1:
            logger = self.logger
            if dev_mode:
                logger = get_logger_from_caller(self.logger)
            logger.error('{}: not enough samples to aggregate'.format(self['aggr-id']))
            return None

        if op == 'avg':
            value /= node_cnt

        self.time = time
        self.value = value
        self['node-cnt'] = node_cnt
        self['aggr-source'] = source

        self['is-final'] = True
        return self.value


_MTYPES = {
    'HUGZ': DataHugz,

    'basic': DataBasic,
    'delta': DataDelta,
    'rate': DataRate,
    'average': DataAverage,
    'percent': DataPercent,

    'aggregate-sum': DataAggregate,
    'aggregate-max': DataAggregate,
    'aggregate-min': DataAggregate,
    'aggregate-avg': DataAggregate,
}
