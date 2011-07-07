'''
Created on Jul 24, 2009

@author: Alexander
'''
from rpyc import Service
from ..app import App
from ..config import Config
import logging

class CustomService(Service):
	'''
	this is the dCAMP custom RPyC service class
	'''
	logger = logging.getLogger('dcamp.service')
