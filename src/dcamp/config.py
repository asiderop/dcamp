'''
@author: Alexander Sideropoulos
'''
import ConfigParser, logging, time

class Config:
	'''
	Helper class for parsing out the dCAMP configuration file
	'''
	logger = logging.getLogger('dcamp.config')
	timestamp = 0
	
	def __init__(self, configfile):
		self.__configlist = list()
		
		self.reconfig(self.__generate_defaults())
		self.__config.read(configfile)
	
	def __generate_defaults(self):
		conf = ConfigParser.SafeConfigParser()
		
		conf.add_section('metrics')
		conf.set('metrics', 'level_leaf', 'GetPercentProcessorTime,GetFreePhysicalMemory;sample=5,report=60')
		conf.set('metrics', 'level_root', 'GetPercentProcessorTime,GetFreePhysicalMemory;report=120')
		conf.set('metrics', 'enabled', 'false')
		
		conf.add_section('root')
		conf.set('root', 'dumpfile', 'dcamp.out')
		conf.set('root', 'dumptofile', 'false')
		conf.set('root', 'heartbeat', '120')
		conf.set('root', 'port', '18884')
		conf.set('root', 'enabled', 'false')
		
		conf.add_section('aggregate')
		conf.set('aggregate', 'heartbeat', '60')
		conf.set('aggregate', 'port', '18883')
		conf.set('aggregate', 'enabled', 'false')
		
		conf.add_section('camp')
		conf.set('camp', 'port', '18882')
		conf.set('camp', 'enabled', 'false')
		
		conf.add_section('base')
		conf.set('base', 'port', '18881')
		
		conf.add_section('dcamp')
		conf.set('dcamp', 'loglevel', 'warning')
		conf.set('dcamp', 'logfile', 'dcamp.log')
		
		return conf
	
	@property
	def __config(self):
		return self.__configlist[0] # return top-most config
	
	def deconfig(self):
		if len(self.__configlist) > 1:
			self.__configlist.pop(0)
			self.timestamp = time.clock()
		
	def reconfig(self, config):
		self.__configlist.insert(0, config)
		self.timestamp = time.clock()

	#------------------ convenience methods for accessing configuration sections
	def is_service(self, svc):
		if svc in ['base', 'dcamp']:
			return True
		return self.__get_boolean(svc, 'enabled')
	
	def dcamp(self, option, isbool=False):
		return self.__get('dcamp', option, isbool)
	
	def base(self, option, isbool=False):
		return self.__get('base', option, isbool)
	
	@property
	def iscamp(self):
		return self.__get_boolean('camp', 'enabled')
	def camp(self, option, isbool=False):
		return self.__get('camp', option, isbool)
	
	@property
	def isaggregate(self):
		return self.__get_boolean('aggregate', 'enabled')
	def aggregate(self, option, isbool=False):
		return self.__get('aggregate', option, isbool)
	
	@property
	def isroot(self):
		return self.__get_boolean('root', 'enabled')
	def root(self, option, isbool=False):
		return self.__get('root', option, isbool)
	
	@property
	def ismetrics(self):
		return self.__get_boolean('metrics', 'enabled')
	def metrics(self, option):
		return self.__get('metrics', option)
	
	def sample_rate(self, level='leaf'):
		v = self.metrics('level_'+ level)
		d = dict(map(lambda x: x.split('='), v.split(';')[1].split(',')))
		return d['sample']
	def report_rate(self, level='leaf'):
		v = self.metrics('level_'+ level)
		d = dict(map(lambda x: x.split('='), v.split(';')[1].split(',')))
		return d['report']
	def metrics_list(self, level='leaf'):
		v = self.metrics('level_'+ level)
		l = v.split(';')[0].split(',')
		return l
	
	#-------- private helper methods to access config parameters using the stack
	def __get(self, section, option, isbool=False):
		'''
		finds the section:option pair from the highest config in the stack
		and returns it
		'''
		for c in self.__configlist:
			if c.has_option(section, option):
				if isbool:
					return c.getboolean(section, option)
				else:
					return c.get(section, option, 1)
		raise Exception(section +':'+ option +' does not exist in this config')
	
	def __get_boolean(self, section, option):
		return self.__get(section, option, isbool=True)
	
	def __has_section(self, section):
		for c in self.__configlist:
			if c.has_section(section):
				return True
		return False
