import logging, configparser
from collections import namedtuple

Endpoint = namedtuple('Endpoint', ['host', 'port'])

class DCParsingError(configparser.Error):
	pass

class DCConfig(configparser.ConfigParser):
	def __init__(self):
		self.logger = logging.getLogger('dcamp.config')
		super().__init__(allow_no_value=True, delimiters=('='))

	@staticmethod
	def validate(file):
		config = DCConfig()
		config.read_file(file)

	def read_file(self, f, source=None):
		super().read_file(f, source)

		sections = list(self)
		sections.remove('DEFAULT')
		if 'root' in self:
			sections.remove('root')

		# find all sample specifications
		self.sample_specs = {}
		for s in sections:
			sample = self[s]
			if 'rate' in sample or 'metric' in sample:
				self.sample_specs[s] = sample

		# find all group specifications (and validate)
		self.group_specs = {}
		for s in sections:
			if s not in self.sample_specs:
				self.group_specs[s] = dict(self[s])

		self.__validate()

	def __eprint(self, *objects):
		import sys
		self.isvalid = False
		print('Error:', *objects, file=sys.stderr)

	def get_endpoint(self, section, option):
		errmsg = None

		strval = ''
		if self[section][option] is None:
			strval = option
		else:
			strval = self[section][option]

		parts = strval.split(':')
		if len(parts) != 2:
			errmsg = 'endpoint specification must be "host:port"'
		elif len(parts[0]) == 0:
			errmsg = 'endpoint specification must provide host name or ip address'
		elif not parts[1].isdecimal():
			errmsg = 'endpoint port must be an integer'

		if errmsg:
			raise DCParsingError("[%s]%s: %s" % (section, option, errmsg))

		return Endpoint(parts[0], int(parts[1]))

	def __validate(self):
		self.isvalid = True

		# check root specification
		if 'root' not in self:
			self.__eprint("missing [root] section")
		else:
			try:
				self.get_endpoint('root', 'endpoint')
			except DCParsingError as e:
				self.__eprint(e)
			except KeyError as e:
				self.__eprint("missing %s option in [root] section" % (e))

			if 'heartbeat' not in self['root']:
				self.__eprint("missing 'heartbeat' option in [root] section")

			if len(self['root']) > 2:
				self.__eprint("extraneous values in [root] section")

		for (group, items) in self.group_specs.items():
			for item in items:
				if 'filter' == item or item in self.sample_specs:
					# skip filter specs and sample spec names
					continue
				try:
					self.get_endpoint(group, item)
				except DCParsingError as e:
					self.__eprint(e)

		if not self.isvalid:
			raise DCParsingError('parsing errors in dcamp config file; '
					'see above error messages for details')
