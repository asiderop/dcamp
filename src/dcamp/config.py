'''
@author: Alexander
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
		
		self.reconfig(ConfigParser.SafeConfigParser())
		self.__config.readfp(open('dcamp.cfg.default'))
		self.__config.read(configfile)
	
	@property
	def __config(self):
		return self.__configlist[0]
	
	def deconfig(self):
		if len(self.__configlist) > 1:
			self.__configlist.pop(0)
			self.timestamp = time.clock()
		
	def reconfig(self, config):
		self.timestamp = time.clock()
		self.__configlist.insert(0, config)

	#------------------ convenience methods for accessing configuration sections
	@property
	def isdcamp(self):
		return True
	
	def dcamp(self, option, isbool=False):
		return self.__get('dcamp', option, isbool)
	
	@property
	def isbase(self):
		return True
	def base(self, option, isbool=False):
		return self.__get('base', option, isbool)
	
	@property
	def iscamp(self):
		return self.__getboolean('camp', 'enabled')
	def camp(self, option, isbool=False):
		return self.__get('camp', option, isbool)
	
	@property
	def isaggregate(self):
		return self.__getboolean('aggregate', 'enabled')
	def aggregate(self, option, isbool=False):
		return self.__get('aggregate', option, isbool)
	
	@property
	def isroot(self):
		return self.__getboolean('root', 'enabled')
	def root(self, option, isbool=False):
		return self.__get('root', option, isbool)
	
	def ismetrics(self):
		return self.__has_section('metrics')
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
	
	def __getboolean(self, section, option):
		return self.__get(section, option, isbool=True)
	
	def __has_section(self, section):
		for c in self.__configlist:
			if c.has_section(section):
				return True
		return False
