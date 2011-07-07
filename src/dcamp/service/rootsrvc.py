'''
@author: Alexander
'''
from __future__ import with_statement

from .d_service import CustomService 
from threading import Thread,Timer
from .d_server import CustomServer
import time

class RootThread(Thread):
	'''
	The root service, providing system management, metrics dump, system logging
	'''
	
	def __init__(self, config, dApp):
		Thread.__init__(self, name='RootService')
		
		self.server = self.RootServer(dApp, config,
											service=self.RootService,
											port=int(config.root('port')),
											auto_register=False)
	
	def run(self):
		self.server.start()
	
	def stop(self):
		self.server.close()
	
#----------------------------------------------------------------------------- 
	class RootServer(CustomServer):

		def config_nodes(self):
			import rpyc
			nodes = self.config.metrics('sensors')
			port = int(self.config.base('port'))
			configt = tuple(['[metrics]',
								'level_leaf='+ self.config.metrics('level_leaf'),
								'level_root='+ self.config.metrics('level_root')])
					
			for n in nodes.split(','):
				self.logger.debug('config: %s', n)
				try:
					c = rpyc.connect(n, port)
					c.root.config(configt)
					c.close()
				except Exception:
					self.logger.error('couldn\'t connect')
		
		def report(self):
			self.logger.info('root reported: '+ time.asctime())
	
#----------------------------------------------------------------------------- 
	class RootService(CustomService):
		
		def exposed_dump(self, metrics):
			filename = self.config.root('dumpfile')
			if self.config.root('dumptofile', isbool=True):
				self.logger.debug('dumping list: %d', len(metrics))
				with open(filename, "a") as f:
					for t in metrics:
						f.write(time.ctime(t[0]) +":"+ t[1] +":"+ str(t[2]) +"\n")
		
		def exposed_register(self, node):
			pass
		
		def exposed_log(self, message):
			pass