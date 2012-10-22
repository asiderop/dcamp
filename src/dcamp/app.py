'''
@author: Alexander
'''
import logging
from dcamp.role.root import Root
from dcamp.role.base import Base

class App:
	'''
	This is the main dCAMP application.
	'''

	def __init__(self, args):

		logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
		self.logger = logging.getLogger('dcamp')
		self.args = args

		self.__setup_logging()
		self.__run()

	def __run(self):
		roles = []
		if self.args.root:
			roles.append(Root(self.args.root, self.args.bases))
		elif self.args.bases:
			assert len(self.args.bases) == 1
			roles.append(Base(self.args.bases[0]))

		for r in roles:
			r.play()

	def __setup_logging(self):
		if (self.args.verbose):
			self.logger.setLevel(logging.INFO)
			self.logger.debug('set logging level to verbose')
		elif (self.args.debug):
			self.logger.setLevel(logging.DEBUG)
			self.logger.debug('set logging level to debug')
