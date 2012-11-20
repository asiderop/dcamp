'''
@author: Alexander
'''
import logging
import zmq

from zhelpers import zpipe

from dcamp.role.root import Root
from dcamp.role.base import Base
from dcamp.config import DCConfig

class App:
	'''
	This is the main dCAMP application.
	'''

	def __init__(self, args):
		self.ctx = zmq.Context.instance()
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
			role = Root(peer, config)

		elif 'base' == self.args.cmd:
			try:
				role = Base(peer, self.args.address)
			except zmq.ZMQError as e:
				self.logger.debug('exception while starting base role:', exc_info=True )
				print('Unable to start base node: %s' % e)
				print('Is one already running on the given address?')
				return -1

		# start playing role
		# NOTE: this should only return when exiting
		assert None != role
		role.play()

		# cleanup
		pipe.close()
		del pipe, peer
		self.ctx.term()
