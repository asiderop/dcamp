from functools import total_ordering

@total_ordering
class EndpntSpec(object):
	'''Class representing an endpoint'''

	# port offsets
	BASE = 0				# topo
	ROOT_DISCOVERY = 1		# topo
	ROOT_CONTROL = 2		# topo
	CONFIG_UPDATE = 10		# config
	CONFIG_SNAPSHOT = 11	# config
	CONFIG_CONTROL = 12		# config
	DATA = 20				# data

	_valid_offsets = [
		BASE,
		ROOT_DISCOVERY,
		ROOT_CONTROL,
		CONFIG_UPDATE,
		CONFIG_SNAPSHOT,
		CONFIG_CONTROL,
		DATA,
	]

	def __init__(self, host, port):
		self.host = host
		self._port = port

	# methods to make this class printable, comparable, and hashable
	def __str__(self):
		return "%s:%s" % (self.host, self._port)
	def __repr__(self):
		return "EndpntSpec(host='%s', port=%s)" % (self.host, self._port)
	def __eq__(self, given):
		return (self.host, self._port) == (given.host, given._port)
	def __lt__(self, given):
		return (self.host, self._port) < (given.host, given._port)
	def __hash__(self):
		return operator.xor(hash(self.host), hash(self._port))

	def encode(self):
		return str(self).encode()

	def port(self, offset=BASE):
		assert offset in EndpntSpec._valid_offsets
		return self._port + offset

	####
	# convenience getter methods

	@property
	def port_base(self):
		return self.port(self.BASE)
	@property
	def port_root_disc(self):
		return self.port(self.ROOT_DISCOVERY)
	@property
	def port_root_cntl(self):
		return self.port(self.ROOT_CONTROL)
	@property
	def port_cnfg_updt(self):
		return self.port(self.CONFIG_UPDATE)
	@property
	def port_cnfg_snap(self):
		return self.port(self.CONFIG_SNAPSHOT)
	@property
	def port_cnfg_cntl(self):
		return self.port(self.CONFIG_CONTROL)
	@property
	def port_data(self):
		return self.port(self.DATA)

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
