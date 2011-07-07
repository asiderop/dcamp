'''
@author: Alexander
'''

from .d_service import CustomService 
from threading import Thread,Timer
from .d_server import CustomServer
import time

class BaseThread(Thread):
	'''
	The base service, providing configuration and heart-beat support
	'''
	
	def __init__(self, config, dApp):
		Thread.__init__(self, name='BaseService')
		
		self.server = self.BaseServer(dApp, config,
											service=self.BaseService,
											port=int(config.base('port')),
											auto_register=False)
	
	def run(self):
		self.server.start()
	
	def stop(self):
		self.server.close()
	
#----------------------------------------------------------------------------- 
	class BaseServer(CustomServer):
		
		__samples = list()
		
		def sample(self):
			import camp.app
			self.logger.debug('base sampled: '+ time.asctime())
			for metric in self.config.metrics_list(level='leaf'):
				self.__samples.append((time.time(), metric, camp.app.__dict__[metric]()))
			
		def report(self):
			import rpyc
			self.logger.debug('base reported: '+ time.asctime())
			n = self.config.dcamp('root')
			port = int(self.config.root('port'))
			try:
				self.logger.debug('connecting to root at: '+ str(n) +':'+ str(port))
				c = rpyc.connect(n, port)
				c.root.dump(self.__samples)
				c.close()
				self.__samples = list()
			except:
				self.logger.error('couldn\'t connect to root')
				raise

		
#----------------------------------------------------------------------------- 
	class BaseService(CustomService):
		
		#----------------------------------------------------- base service stuff
		def exposed_config(self, configtuple):
			import ConfigParser
			class TParser:
				def __init__(self, tuple):
					self.l = list(tuple)
				def readline(self):
					if len(self.l) > 0:
						return self.l.pop(0)
					else:
						return ''
			
			self.logger.info('applying new configuration')
			config = ConfigParser.SafeConfigParser()
			config.readfp(TParser(configtuple))
			self.dApp.reconfig(config)
		
		def exposed_deconfig(self):
			self.logger.info('reverting back to old configuration')
			self.dApp.deconfig()
		
		def exposed_beat(self, node):
			pass
		
		#------------------------------------------------- dcamp management stuff
		def exposed_active_servers(self):
			return self.dApp.active_servers()

		def exposed_stop_dcamp(self):
			t = Timer(10, self.dApp.stop_server, ['base'])
			t.start()
			return 'stopping dCAMP servers in ten seconds...'
