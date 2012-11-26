'''
@author: Alexander
'''
import logging
import zmq

from zhelpers import zpipe

import dcamp.dcmsg as dcmsg
from dcamp.data import EndpntSpec
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
		# if base role command:
		#    start Base role, erroring if already running
		# if root command:
		#    execute command, erroring if base role not running on local node

		if 'root' == self.args.cmd:
			self._exec_root()
		elif 'base' == self.args.cmd:
			self._exec_base()

		self.ctx.term()

	def _exec_root(self):
		config = DCConfig()
		config.read_file(self.args.configfile)

		# 1) MARCO "root" base endpoint (multiple times?)
		# 2) if POLO'ed, ASSIGN

		# @todo: this can raise exceptions

		pub = self.ctx.socket(zmq.PUB)
		base = config.root['endpoint']
		connect_str = "tcp://%s:%d" % (base.host, base.port)
		pub.connect(connect_str)

		rep = self.ctx.socket(zmq.REP)
		bind_port = rep.bind_to_random_port("tcp://*")

		pubmsg = dcmsg.MARCO(EndpntSpec("localhost", bind_port))
		reqmsg = None
		tries = 0
		while tries < 5:
			tries += 1
			pubmsg.send(pub)
			result = rep.poll(timeout=1000)
			if 0 != result:
				reqmsg = dcmsg.DCMsg.recv(rep)
				break

		if None == reqmsg:
			print('Unable to contact base node at root address: %s' % base)
			print('Is the base node running?')
			return -1

		assert(b'POLO' == reqmsg.name)
		repmsg = dcmsg.ASSIGN(base)
		repmsg['level'] = 'root'
		repmsg['config-file'] = self.args.configfile.name

		repmsg.send(rep)

		pub.close()
		rep.close()
		del pub, rep

	def _exec_base(self):
		# pair socket for controlling Role; not used here
		pipe, peer = zpipe(self.ctx)

		role = None
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
