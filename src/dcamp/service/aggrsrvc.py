'''
@author: Alexander
'''

from .d_service import CustomService 
from threading import Thread,Timer
from .d_server import CustomServer
import logging

class AggregateThread(Thread):
	'''
	The aggregate service, providing aggregation of metrics from sensor nodes
	'''
	
	def __init__(self, config, dApp):
		Thread.__init__(self, name='AggregateService')
		
		self.server = self.AggregateServer(dApp, config,
											service=self.AggregateService,
											port=int(config.aggregate('port')),
											auto_register=False)
	
	def run(self):
		self.server.start()
	
	def stop(self):
		self.server.close()
	
#----------------------------------------------------------------------------- 
	class AggregateServer(CustomServer):
		pass
	
#----------------------------------------------------------------------------- 
	class AggregateService(CustomService):
		
		def exposed_dump(self, config):
			pass
		
		def exosed_register(self, node):
			pass