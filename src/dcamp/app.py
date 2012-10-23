'''
@author: Alexander
'''
import logging

from dcamp.role.root import Root
from dcamp.role.base import Base
from dcamp.config import DCConfig

class App:
	'''
	This is the main dCAMP application.
	'''

	def __init__(self, args):

		self.logger = logging.getLogger('dcamp.app')
		self.args = args

	def run(self):
		roles = []
		if 'root' == self.args.cmd:
			config = DCConfig()
			config.read_file(self.args.configfile)
			roles.append(Root(config))
		elif 'base' == self.args.cmd:
			roles.append(Base(self.args.port))

		for r in roles:
			r.play()
