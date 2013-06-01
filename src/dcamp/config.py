import logging, configparser
from collections import namedtuple

from dcamp.data import EndpntSpec

MetricSpec = namedtuple('MetricSpec', ['rate', 'threshold', 'metric'])
FilterSpec = namedtuple('FilterSpec', ['action', 'match'])
GroupSpec = namedtuple('GroupSpec', ['endpoints', 'filters', 'metrics'])

# helper methods
def to_seconds(given):
	'''
	Method determines how given time is specified and return int value in seconds;
	e.g. to_seconds('90s') == 90

	valid time units:
		s -- seconds

	@todo add this to validation routine / issue #23
	'''
	if given.endswith('s'):
		return int(given[:len(given)-1])
	else:
		raise DCParsingError('invalid time unit given--valid units: s')

class DCParsingError(configparser.Error):
	pass

class DCConfig(configparser.ConfigParser):
	def __init__(self):
		self.logger = logging.getLogger('dcamp.config')
		super().__init__(allow_no_value=True, delimiters=('='))

		self.isvalid = False

		self.root = {}
		self.metrics = {}
		self.groups = {}

		self.__prefix = []
		self.delimiter = '/'

		self.kvdict = {}

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

		# the order of these calls matters

		self.__validate()

		self.__create_root()
		self.__create_metrics()
		self.__create_groups()

		self.__create_kvdict()

	def __create_root(self):
		assert self.isvalid

		result = {}
		result['endpoint'] = EndpntSpec.from_str(self['root']['endpoint'])
		result['heartbeat'] = to_seconds(self['root']['heartbeat'])
		self.root = result

	def __create_metrics(self):
		assert self.isvalid

		result = {}

		# process all metric specifications
		for name in self.metric_sections:
			rate = to_seconds(self[name]['rate'])
			threshold = self[name]['threshold'] if 'threshold' in self[name] else None
			metric = self[name]['metric']
			result[name] = MetricSpec(rate, threshold, metric)

		self.metrics = result

	def __create_groups(self):
		assert self.isvalid
		assert len(self.metrics) > 0

		result = {}

		# process all group sepcifications
		for name in self.group_sections:
			endpoints = []
			metrics = []
			filters = []

			for key in self[name]:
				if key in self.metrics:
					# add metric spec
					metrics.append(self.metrics[key])
				elif key.startswith(('+', '-')):
					# create/add filter spec
					filters.append(FilterSpec(key[0], key[1:]))
				else:
					# create endpoint spec
					endpoints.append(EndpntSpec.from_str(key))

			result[name] = GroupSpec(endpoints, filters, metrics)

		self.groups = result

	# get/pop/push prefix--always ensure a trailing delimiter
	def __get_pre(self):
		result = self.delimiter
		for pre in self.__prefix:
			result += pre + self.delimiter
		return result
	def __pop_pre(self):
		# check for sole delimiter
		if len(self.__prefix) > 0:
			self.__prefix.pop()
		return self.__get_pre()
	def __push_pre(self, pre):
		if self.delimiter in pre:
			self.logger.error('delimiter in pushed prefix')
		if pre.endswith(self.delimiter):
			pre = pre.rstrip(self.delimiter)
		self.__prefix.append(pre)
		return self.__get_pre()

	def __create_kvdict(self):
		assert self.isvalid
		assert len(self.root) > 0
		assert len(self.metrics) > 0

		result = {}

		# add root specs
		prefix = self.__push_pre('root')
		result[prefix+'endpoint'] = self.root['endpoint']
		result[prefix+'heartbeat'] = self.root['heartbeat']
		prefix = self.__pop_pre()

		for (group, spec) in self.groups.items():
			# add group name to prefix
			prefix = self.__push_pre(group)

			result[prefix+'endpoints'] = spec.endpoints
			result[prefix+'filters'] = spec.filters
			result[prefix+'metrics'] = spec.metrics

			# remove name from prefix
			prefix = self.__pop_pre()

		# verify we popped as many times as we pushed
		assert len(self.__get_pre()) == 1

		self.kvdict = result

	#####
	# validation methods

	def __eprint(self, *objects):
		import sys
		self.__num_errors += 1
		print('Error:', *objects, file=sys.stderr)
	def __wprint(self, *objects):
		import sys
		self.__num_warns += 1
		print('Warning:', *objects, file=sys.stderr)

	def __validate(self):
		'''
		@todo turn each of these checks into a test routine / issue #24
		'''
		self.__num_errors = 0
		self.__num_warns = 0

		# { host : [ port ] }
		endpoints = {}

		# check root specification
		if 'root' not in self:
			self.__eprint("missing [root] section")
		else:
			try:
				ep = EndpntSpec.from_str(self['root']['endpoint'])
				endpoints[ep.host] = [ep.port_base]
			except ValueError as e:
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
					ep = EndpntSpec.from_str(key)
					if ep.host in endpoints:
						endpoints[ep.host].append(ep.port_base)
					else:
						endpoints[ep.host] = [ep.port_base]
				except ValueError as e:
					self.__eprint('[%s]%s' % (group, key), e)

			# verify group has at least one endpoint
			if nodecnt == 0:
				self.__eprint('[%s] section contains no nodes or subnets' % (group))

		# ensure no overlapping ports
		for (host, ports) in endpoints.items():
			prev = ports[0];
			for p in sorted(ports[1:]):
				if (prev + 29) >= p:
					self.__eprint('endpoint port overlap on host %s: %d and %d' % (host, prev, p))
				prev = p

		# warn if some metric specs not used
		unused_metrics = set(self.metric_sections.keys()) - used_metrics
		if len(unused_metrics) > 0:
			self.__wprint('unused metric specification:', unused_metrics)

		if self.__num_errors > 0:
			raise DCParsingError('%d parsing errors in dcamp config file; '
					'see above error messages for details' % (self.__num_errors))
		else:
			self.isvalid = True
