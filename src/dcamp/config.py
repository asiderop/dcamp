import logging, configparser
from collections import namedtuple

EndpntSpec = namedtuple('EndpntSpec', ['host', 'port'])
MetricSpec = namedtuple('MetricSpec', ['rate', 'threshold', 'metric'])
FilterSpec = namedtuple('FilterSpec', ['action', 'match'])

GroupSpec = namedtuple('GroupSpec', ['endpoints', 'filters', 'metrics'])

class DCParsingError(configparser.Error):
	pass

class DCConfig(configparser.ConfigParser):
	def __init__(self):
		self.logger = logging.getLogger('dcamp.config')
		super().__init__(allow_no_value=True, delimiters=('='))

		self.__prefix = []
		self.__delimiter = '/'

	@staticmethod
	def validate(file):
		config = DCConfig()
		config.read_file(file)

	def read_file(self, f, source=None):
		super().read_file(f, source)

		sections = list(self)
		sections.remove('DEFAULT')
		if 'root' in self: # not validated yet
			sections.remove('root')

		# find all metric specifications
		self.metric_sections = {}
		for s in sections:
			section = dict(self[s])
			if 'rate' in section or 'metric' in section:
				self.metric_sections[s] = section

		# find all group specifications
		self.group_sections = {}
		for s in sections:
			if s not in self.metric_sections:
				self.group_sections[s] = dict(self[s])

		self.__validate()
		self.__create_groups()
		self.__create_kvdict()

	def get_seconds(self, string):
		'''
		Method determines how given time is specified and return int value in seconds;
		e.g. get_seconds('90s') == 90

		valid time units:
			s -- seconds

		@todo add this to validation routine
		'''
		if string.endswith('s'):
			return int(string[:len(string)-1])
		else:
			raise DCParsingError('invalid time unit given--valid units: s')

	def get_endpoint(self, string):
		errmsg = None

		parts = string.split(':')
		if len(parts) != 2:
			errmsg = 'endpoint specification must be "host:port"'
		elif len(parts[0]) == 0:
			errmsg = 'endpoint specification must provide host name or ip address'
		elif not parts[1].isdecimal():
			errmsg = 'endpoint port must be an integer'

		if errmsg:
			raise DCParsingError(errmsg)

		return EndpntSpec(parts[0], int(parts[1]))

	def __create_groups(self):
		self.groups = {}

		# add group specs
		for group in self.group_sections:
			endpoints = []
			metrics = []
			filters = []

			for key in self[group]:
				if key in self.metric_sections:
					# create metric spec
					rate = self.get_seconds(self[key]['rate'])
					threshold = self[key]['threshold'] if 'threshold' in self[key] else None
					metric = self[key]['metric']
					metrics.append(MetricSpec(rate, threshold, metric))
				elif key.startswith(('+', '-')):
					# create filter spec
					filters.append(FilterSpec(key[0], key[1:]))
				else:
					# create endpoint spec
					endpoints.append(self.get_endpoint(key))

			self.groups[group] = GroupSpec(endpoints, filters, metrics)

	# get/pop/push prefix--always ensure a trailing delimiter
	def __get_pre(self):
		result = self.__delimiter
		for pre in self.__prefix:
			result += pre + self.__delimiter
		return result
	def __pop_pre(self):
		# check for sole delimiter
		if len(self.__prefix) > 0:
			self.__prefix.pop()
		return self.__get_pre()
	def __push_pre(self, pre):
		if self.__delimiter in pre:
			self.logger.error('delimiter in pushed prefix')
		if pre.endswith(self.__delimiter):
			pre = pre.rstrip(self.__delimiter)
		self.__prefix.append(pre)
		return self.__get_pre()

	def __create_kvdict(self):
		self.kvdict = {}

		# add root specs
		prefix = self.__push_pre('root')
		self.kvdict[prefix+'endpoint'] = self.get_endpoint(self['root']['endpoint'])
		self.kvdict[prefix+'heartbeat'] = self.get_seconds(self['root']['heartbeat'])
		prefix = self.__pop_pre()

		for (group, spec) in self.groups.items():
			# add group name to prefix
			prefix = self.__push_pre(group)

			self.kvdict[prefix+'endpoints'] = spec.endpoints
			self.kvdict[prefix+'filters'] = spec.filters
			self.kvdict[prefix+'metrics'] = spec.metrics

			# remove name from prefix
			prefix = self.__pop_pre()

		# verify we popped as many times as we pushed
		assert len(self.__get_pre()) == 1

	#####
	# validation methods

	def __eprint(self, *objects):
		import sys
		self.isvalid = False
		print('Error:', *objects, file=sys.stderr)
	def __wprint(self, *objects):
		import sys
		print('Warning:', *objects, file=sys.stderr)

	def __validate(self):
		'''
		@todo turn each of these checks into a test routine
		'''

		self.isvalid = True

		# check root specification
		if 'root' not in self:
			self.__eprint("missing [root] section")
		else:
			try:
				self.get_endpoint(self['root']['endpoint'])
			except DCParsingError as e:
				self.__eprint('[root]endpoint', e)
			except KeyError as e:
				self.__eprint("missing %s option in [root] section" % (e))

			if 'heartbeat' not in self['root']:
				self.__eprint("missing 'heartbeat' option in [root] section")

			if len(self['root']) > 2:
				self.__eprint("extraneous values in [root] section")

		# check for at least one group and one metric
		if len(self.metric_sections) < 1:
			self.__eprint("must specify at least one metric")
		if len(self.group_sections) < 1:
			self.__eprint("must specify at least one group")

		# find all used metric specs and validate group endpoint definitions
		used_metrics = set()
		for group in self.group_sections:
			nodecnt = 0
			for key in self[group]:
				if key in self.metric_sections:
					# add metric spec name to used list and continue
					used_metrics.add(key)
					continue
				elif key.startswith(('+', '-')):
					# skip filter specs
					continue
				try:
					nodecnt += 1
					self.get_endpoint(key)
				except DCParsingError as e:
					self.__eprint('[%s]%s' % (group, key), e)

			# verify group has at least one endpoint
			if nodecnt == 0:
				self.__eprint('[%s] section contains no nodes or subnets' % (group))

		# warn if some metric specs not used
		unused_metrics = set(self.metric_sections.keys()) - used_metrics
		if len(unused_metrics) > 0:
			self.__wprint('unused metric specification:', unused_metrics)

		if not self.isvalid:
			raise DCParsingError('parsing errors in dcamp config file; '
					'see above error messages for details')
