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
	'DATA_SUM',
	'DATA_AVERAGE',
	'DATA_PERCENT',
	'DATA_RATE',
	'_DATA',
	]

class _DATA(DCMsg, _PROPS):
	'''
	Frame 0: data source (leaf or collector node endpoint), as 0MQ string
	Frame 1: properties, as 0MQ string
	Frame 2: time t1 in ms epoch utc, 8 bytes in network order
	Frame 3: value v1, 8 bytes in network order
	Frame 4: time t2 in ms epoch utc, 8 bytes in network order; not empty for average and rate
	Frame 5: value v2, 8 bytes in network order; empty for basic and sum

	properties = *( type / detail / config )
	type       = "type=" ( "HUGZ" / "basic" / "sum" / "average" / "percent" / "rate" )
	detail     = "detail=" <string>
	config     = "config-name=" <string>
	'''

	def __init__(self, source, properties,
			time1=None, value1=None, time2=None, value2=None):
		DCMsg.__init__(self)
		_PROPS.__init__(self, properties)

		assert isinstance(source, EndpntSpec)

		# validate given type
		assert 'type' in properties, 'missing metric "type" key'
		assert self.m_type in _MTYPES.keys(), 'given metric "type" not valid'

		# validate class was constructed with type-appropriate sub-class
		assert isinstance(self, _MTYPES[self.m_type])

		assert isInstance_orNone(time1, int)
		assert isInstance_orNone(value1, int)
		assert isInstance_orNone(time2, int)
		assert isInstance_orNone(value2, int)

		# TODO: add more verifications of parameters based on given m_type

		self.source = source

		self.time1 = time1
		self.value1 = value1
		self.time2 = time2
		self.value2 = value2

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
	def calculated(self):
		'''returns float representing calculated value of data message'''
		raise NotImplementedError('sub-class implementation missing')

	@property
	def suffix(self):
		return ''

	def accumulate(self, new_data):
		'''adds new_data to this data'''
		raise NotImplementedError('sub-class implementation missing')

	def __str__(self):
		return '%s -- %s @ %d = %.2f' % (self.source, self.detail, self.time1, self.calculated)

	def log_str(self):
		return '%d\t%s\t%s\t%.2f%s' % (self.time1, self.source, self.detail, self.calculated, self.suffix)

	@property
	def frames(self):
		return [
				self.source.encode(),
				self._encode_dict(self.properties),
				self._encode_int(self.time1),
				self._encode_int(self.value1),
				self._encode_int(self.time2),
				self._encode_int(self.value2),
			]

	@classmethod
	def from_msg(cls, msg):
		assert isinstance(msg, list)

		# make sure we have six frames
		if 6 != len(msg):
			raise ValueError('wrong number of frames')

		source = EndpntSpec.decode(msg[0])
		props = _PROPS._decode_dict(msg[1])

		time1 = DCMsg._decode_int(msg[2])
		value1 = DCMsg._decode_int(msg[3])
		time2 = DCMsg._decode_int(msg[4])
		value2 = DCMsg._decode_int(msg[5])

		return cls(source, props, time1, value1, time2, value2)

def HUGZ(endpoint):
	return DATA_HUGZ(
			source=endpoint,
			properties={'type': 'HUGZ'},
			time1=now_msecs()
		)

class DATA_HUGZ(_DATA):
	def __str__(self):
		return '%s -- HUGZ @ %d' % (str(self.source), self.time1)

	def log_str(self):
		return '%d\t%s\t%s' % (self.time1, self.source, self.m_type)

class DATA_BASIC(_DATA):
	@property
	def calculated(self):
		return float(self.value1)

	def accumulate(self, new_data):
		''' always returns single, point-in-time value '''
		self.value1 = new_data.value1
		self.time1 = new_data.time1

class DATA_SUM(_DATA):
	@property
	def suffix(self):
		suff = ' total'
		if self.time2 is not None:
			suff += ' after %.2f sec' % ((self.time2 - self.time1) / 1e3)
		return suff

	@property
	def calculated(self):
		return float(self.value1)

	def accumulate(self, new_data):
		''' returns sum of all values between time1 and time2 '''
		self.value1 += new_data.value1
		self.time2 = new_data.time1

class DATA_AVERAGE(_DATA):
	@property
	def suffix(self):
		suff = ' average'
		if self.time2 is not None:
			suff += ' after %.2f sec' % ((self.time2 - self.time1) / 1e3)
		return suff

	@property
	def calculated(self):
		return (self.value1 / self.value2)

	def accumulate(self, new_data):
		''' returns average of values between time1 and time2 '''
		self.value1 += new_data.value1
		self.value2 += new_data.value2
		self.time2 = new_data.time1

class DATA_PERCENT(_DATA):
	@property
	def suffix(self):
		suff = '%'
		if self.time2 is not None:
			suff += ' after %.2f sec' % ((self.time2 - self.time1) / 1e3)
		return suff

	@property
	def calculated(self):
		return (self.value1 / self.value2) * 100

	def accumulate(self, new_data):
		''' returns percent of values between time1 and time2 '''
		self.value1 += new_data.value1
		self.value2 += new_data.value2
		self.time2 = new_data.time1

class DATA_RATE(_DATA):
	@property
	def suffix(self):
		return '%s / sec' % format_bytes((self.value2 - self.value1) / (self.time2 - self.time1) * 1e3, num_or_suffix='suffix')

	@property
	def calculated(self):
		return format_bytes((self.value2 - self.value1) / (self.time2 - self.time1) * 1e3, num_or_suffix='num')

	def accumulate(self, new_data):
		''' returns rate of change between time1 and time2 '''
		self.value2 = new_data.value2
		self.time2 = new_data.time2

_MTYPES = {
	'HUGZ': DATA_HUGZ,
	'basic': DATA_BASIC,
	'sum': DATA_SUM,
	'average': DATA_AVERAGE,
	'percent': DATA_PERCENT,
	'rate': DATA_RATE,
}
