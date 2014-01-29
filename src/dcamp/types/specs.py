from collections import namedtuple
from datetime import datetime
from operator import xor

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
	'''Class representing a Threshold specification'''

	def check(self, data):
		if self.op in ['<', '>']:
			return self.__limit(data)
		elif self.op in ['+', '*']:
			return self.__timed(data)
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

	def __limit(self, data):
		if '<' == self.op:
			return data.calculate() <= self.value
		elif '>' == self.op:
			return data.calculate() >= self.value
		raise NotImplementedError('unknown limit operation')

	def __timed(self, data):
		return data.time1 + (self.value * 1000) > now_msecs()

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
			except NotImplementedError as e:
				errmsg = 'time-based threshold specification contains invalid time'

		elif op in ['>', '<']:
			try:
				value = float(val_str)
			except ValueError as e:
				errmsg = 'value-based threshold specification contains invalid value'

		else:
			errmsg = 'invalid op for threshold specification'

		if errmsg:
			raise ValueError('%s: [%s]' % (errmsg, given))

		return cls(op, value)

class MetricSpec(namedtuple('MetricSpec', ['config_name', 'rate', 'threshold', 'detail', 'param'])):
	''' Class Representing a Metric Specification '''
	def __str__(self):
		return "%s(detail='%s', rate='%s', threshold='%s' param='%s')" % (self.config_name,
				self.detail, seconds_to_str(self.rate), self.threshold, self.param or '')

class MetricCollection(namedtuple('MetricCollection', 'epoch, spec')):
	''' Class Representing a Metric Collection Specification '''
	pass

class FilterSpec(namedtuple('FilterSpec', ['action', 'match'])):
	''' Class Representing a Filter Specification '''
	def __str__(self):
		return "%s'%s'" % (self.action, self.match)

class EndpntSpec(namedtuple('EndpntSpec', ['host', 'port'])):
	''' Class Representing an Endpoint Specification '''

	### port offsets (for binding)

	TOPO_BASE = 0			# SUB topo discovery PUB from mgmt service
	TOPO_JOIN = 1			# REP assignment to node service topo join REQ

	# XXX: user control of system goes to root role; use mgmt or config service?
	ROOT_CONTROL = 2		# REP to commands from cli REQ
	CONFIG_CONTROL = 12		# SUB commands from parent PUB

	CONFIG_UPDATE = 10	 	# PUB updates to child SUB
	CONFIG_SNAPSHOT = 11	# REP snapshots to child REQ

	DATA_SUB = 20			# SUB metrics from child PUB
	DATA_PUSH_PULL = 21		# PUSH/PULL metrics between sensor and filter services (intra-node)

	_valid_offsets = [
		TOPO_BASE,
		TOPO_JOIN,
		ROOT_CONTROL,

		CONFIG_UPDATE,
		CONFIG_SNAPSHOT,
		CONFIG_CONTROL,

		DATA_SUB,
		DATA_PUSH_PULL,
	]

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
