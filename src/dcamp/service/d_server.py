'''
Created on Jul 24, 2009

@author: Alexander
'''
from rpyc.utils.server import ThreadedServer
import logging

class CustomServer(ThreadedServer):
		'''
		This is the dCAMP custom server class
		'''

		def __init__(self, dApp, config, *args, **kwargs):
			'''
			the constructor passes on the parameters to the parent constructor
			'''
			ThreadedServer.__init__(self, *args, **kwargs)
			self.logger = logging.getLogger('dcamp.server')
			self.dApp = dApp
			self.config = config
			self.service.dApp = dApp
			self.service.config = config
