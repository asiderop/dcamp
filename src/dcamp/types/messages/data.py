'''
dCAMP Data Protocol
'''
import logging

from dcamp.types.messages.common import DCMsg, _PROPS
from dcamp.types.specs import EndpntSpec
from dcamp.util.functions import format_bytes, isInstance_orNone, now_msecs

__all__ = [
	'DATA_HUGZ',
	'DATA_BASIC',
	'DATA_RATE',
	'DATA_AVERAGE',
	'DATA_PERCENT',
	'_DATA',
	]

class _DATA(DCMsg, _PROPS):
	'''
	Frame 0: data source (leaf or collector node endpoint), as 0MQ string
	Frame 1: properties, as 0MQ string
	Frame 2: time in ms epoch utc, 8 bytes in network order
	Frame 3: value, 8 bytes in network order
	Frame 4: base value, 8 bytes in network order; only for average and percent types

	properties = *( type / detail / config )
	type       = "type=" ( "HUGZ" / "basic" / "rate" / "average" / "percent" )
	detail     = "detail=" <string>
	config     = "config-name=" <string>
	'''

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

	def __is_compatible(self, given):
		return (self.source == given.source and
			self.m_type == given.m_type and
			self.time < given.time)

	def print(self, given):
		'''
		Return value is a string representation of the calculated value and a contextual
		suffix.
		'''
		return '%.2f %s' % (self.calculate(given), self.suffix(given))

	def calculate(self, given):
		'''
		Calculates a value based on this and the given data message. The given message
		must be from the same source, have the same message type, and have a later
		timestamp. Return value is a float representation of calculated result.
		'''
		assert self.__is_compatible(given)
		return self._calculate(given)

	def suffix(self, given):
		'''
		Calculates a value based on this and the given data message. The given message
		must be from the same source, have the same message type, and have a later
		timestamp. Return value is a float representation of calculated result.
		'''
		assert self.__is_compatible(given)
		return self._suffix(given)

	def _calculate(self, given):
		raise NotImplementedError('sub-class implementation missing')
	def _suffix(self, given):
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
	def from_msg(cls, msg):
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
		if real_class == DATA_HUGZ:
			return real_class(source, time)

		value = DCMsg._decode_int(msg[3])
		base_value = DCMsg._decode_int(msg[4])

		return real_class(source, props, time, value, base_value)

class DATA_HUGZ(_DATA):
	def __init__(self, the_source, the_time=None):
		super().__init__(
				source=the_source,
				properties={'type': 'HUGZ'},
				time=the_time or now_msecs()
			)

	def _calculate(self, given):
		raise NotImplementedError('HUGZ have no value')

	def _suffix(self, given):
		raise NotImplementedError('HUGZ have no suffix')

	def __str__(self):
		return '%s -- HUGZ @ %d' % (str(self.source), self.time)

	def log_str(self):
		return '%d\t%s\t%s' % (self.time, self.source, self.m_type)

class DATA_BASIC(_DATA):
	def _calculate(self, given):
		return float(given.value - self.value)

	def _suffix(self, given):
		return ''

class DATA_AVERAGE(_DATA):
	def __str__(self):
		return '%s / %d' % (_DATA.__str__(self), self.base_value)
	def log_str(self):
		return '%s\t%d' % (_DATA.log_str(self), self.base_value)

	def _suffix(self, given):
		return 'average'

	def _calculate(self, given):
		return float( (given.value - self.value) / (given.base_value - self.base_value) )

class DATA_PERCENT(_DATA):
	def __str__(self):
		return '%s / %d' % (_DATA.__str__(self), self.base_value)
	def log_str(self):
		return '%s\t%d' % (_DATA.log_str(self), self.base_value)

	def _suffix(self, given):
		return '%'

	def _calculate(self, given):
		return float( (given.value - self.value) / (given.base_value - self.base_value) ) * 100.0

class DATA_RATE(_DATA):
	def __the_rate(self, given):
		return float( (given.value - self.value) / (given.time - self.time) * 1e3 )

	def _suffix(self, given):
		return '%s / sec' % format_bytes(self.__the_rate(given), num_or_suffix='suffix')

	def _calculate(self, given):
		return format_bytes(self.__the_rate(given), num_or_suffix='num')

_MTYPES = {
	'HUGZ': DATA_HUGZ,
	'basic': DATA_BASIC,
	'rate': DATA_RATE,
	'average': DATA_AVERAGE,
	'percent': DATA_PERCENT,
}
