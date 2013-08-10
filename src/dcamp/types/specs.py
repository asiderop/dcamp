from collections import namedtuple
from datetime import datetime
from operator import xor

import dcamp.util.functions as Util

GroupSpec = namedtuple('GroupSpec', ['endpoints', 'filters', 'metrics'])

class MetricSpec(namedtuple('MetricSpec', ['rate', 'threshold', 'metric'])):
	''' Class Representing a Metric Specification '''
	def __str__(self):
		return "%s(rate='%s', threshold='%s')" % (self.metric,
				Util.seconds_to_str(self.rate), self.threshold)

class FilterSpec(namedtuple('FilterSpec', ['action', 'match'])):
	''' Class Representing a Filter Specification '''
	def __str__(self):
		return "%s'%s'" % (self.action, self.match)

class EndpntSpec(namedtuple('EndpntSpec', ['host', 'port'])):
	''' Class Representing an Endpoint Specification '''

	### port offsets

	TOPO_BASE = 0			# SUB topo discovery PUB from mgmt service
	TOPO_JOIN = 1			# REP assignment to node service topo join REQ

	# XXX: user control of system goes to root role; use mgmt or config service?
	ROOT_CONTROL = 2		# REP to commands from cli REQ
	CONFIG_CONTROL = 12		# SUB parent PUB commands

	CONFIG_UPDATE = 10	 	# PUB updates to child SUB
	CONFIG_SNAPSHOT = 11	# REP snapshots to child REQ

	DATA_PUB = 20			# PUB to parent
	DATA_SUB = 21			# SUB from children

	_valid_offsets = [
		TOPO_BASE,
		TOPO_JOIN,
		ROOT_CONTROL,

		CONFIG_UPDATE,
		CONFIG_SNAPSHOT,
		CONFIG_CONTROL,

		DATA_PUB,
		DATA_SUB,
	]

	def __str__(self):
		return "%s:%s" % (self.host, self.port)

	def encode(self):
		return str(self).encode()

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
				op == 'bind' and '*' or self.host,
				self._port(offset))

	@classmethod
	def from_str(cls, given):
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

__all__ = [
		EndpntSpec,
		FilterSpec,
		GroupSpec,
		MetricSpec,
		SerializableSpecTypes,
	]
