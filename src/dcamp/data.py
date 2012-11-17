class EndpntSpec(object):
	'''Class representing an endpoint'''

	def __init__(self, host, port):
		self.host = host
		self.port = port

	# methods to make this class printable, comparable, and hashable
	def __str__(self):
		return "%s:%s" % (self.host, self.port)
	def __repr__(self):
		return "EndpntSpec(host='%s', port=%s)" % (self.host, self.port)
	def __eq__(self, given):
		return self.host == given.host and self.port == given.port
	def __ne__(self, given):
		return not self == given
	def __hash__(self):
		return operator.xor(hash(self.host), hash(self.port))

	def encode(self):
		return str(self).encode()

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
