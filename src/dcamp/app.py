'''
@author: Alexander
'''
import logging

from zmq import Context, ZMQError
from zhelpers import zpipe

from dcamp.role.root import Root
from dcamp.role.base import Base
from dcamp.config import DCConfig

class App:
	'''
	This is the main dCAMP application.
	'''

	def __init__(self, args):
		self.ctx = Context.instance()
		self.ctx.linger = 0
		self.logger = logging.getLogger('dcamp.app')
		self.args = args

	def exec(self):
		# issue #31
		# check for base role on local node
		# if base role command:
		#    execute command, silently succeeding if role already in desired state (warn
		#    if stopping and not existent)
		# if root command:
		#    execute command, erroring if base role not running on local node

		# pair socket for controlling Role
		pipe, peer = zpipe(self.ctx)
		role = None

		if 'root' == self.args.cmd:
			config = DCConfig()
			config.read_file(self.args.configfile)
			role = Root(self.ctx, peer, config)

		elif 'base' == self.args.cmd:
			try:
				role = Base(self.ctx, peer, self.args.address)
			except ZMQError as e:
				self.logger.debug('exception while starting base role:', exc_info=True )
				print('Unable to start base node: %s' % e)
				print('Is one already running on the given address?')
				return -1

		assert None != role
		try:
			role.play()
		except KeyboardInterrupt:
			self.logger.debug('received keyboard interrupt')
			return 0
