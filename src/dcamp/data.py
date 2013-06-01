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

def Runnable(given_class):
	'''Class decorator turning given class into a "runnable" object'''

	# _state "enum" values
	given_class.INIT = 0
	given_class.RUNNING = 1
	given_class.STOPPED = 2
	given_class.ERRORED = 3

	given_class.STATES = [
		given_class.INIT,
		given_class.RUNNING,
		given_class.STOPPED,
		given_class.ERRORED,
	]

	original_init = given_class.__init__
	def __init__(self, *args, **kwargs):
		self._state = given_class.INIT
		original_init(self, *args, **kwargs)
	given_class.__init__ = __init__

	def set_state(self, state):
		assert state in given_class.STATES
		self._state = state
	given_class.set_state = set_state

	####
	# convenience setter methods

	def stop_state(self):
		self._state = given_class.STOPPED
	given_class.stop_state = stop_state

	def run_state(self):
		self._state = given_class.RUNNING
	given_class.run_state = run_state

	def error_state(self):
		self._state = given_class.ERRORED
	given_class.error_state = error_state

	####
	# convenience getter methods

	@property
	def is_stopped(self):
		return self._state == given_class.STOPPED
	given_class.is_stopped = is_stopped

	@property
	def is_running(self):
		return self._state == given_class.RUNNING
	given_class.is_running = is_running

	@property
	def is_errored(self):
		return self._state == given_class.ERRORED
	given_class.is_errored = is_errored

	return given_class
