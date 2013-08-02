'''
@author: Alexander
'''
import logging
import zmq

from zhelpers import zpipe

import dcamp.data.messages as dcmsg
from dcamp.data.config import DCConfig
from dcamp.data.specs import EndpntSpec
from dcamp.role.root import Root
from dcamp.role.base import Base

class App:
	'''
	This is the main dCAMP application.
	'''

	def __init__(self, args):
		self.ctx = zmq.Context.instance()

		# set default options for all sockets
		self.ctx.linger = 0

		self.logger = logging.getLogger('dcamp.app')
		self.args = args

	def exec(self):
		'''
		if base role command:
		    start Base role, erroring if already running
		if root command:
		    execute command, erroring if base role not running
		'''
		result = 0
		if 'base' == self.args.cmd:
			result = self._exec_base()
		elif 'root' == self.args.cmd:
			result = self._exec_root()

		self.ctx.term()
		exit(result)

	def _exec_root(self):
		config = DCConfig()
		config.read_file(self.args.configfile)

		# 1) MARCO "root" base endpoint (multiple times?)
		# 2) if POLO'ed, send assignment CONTROL

		# @todo: this can raise exceptions

		pub = self.ctx.socket(zmq.PUB)
		root_ep = config.root['endpoint']
		connect_str = root_ep.connect_uri(EndpntSpec.TOPO_BASE)
		pub.connect(connect_str)

		rep = self.ctx.socket(zmq.REP)
		bind_addr = rep.bind_to_random_port("tcp://*")

		# subtract 1 so the TOPO_JOIN port calculated by the remote node matches the
		# random port to which we just bound
		ep = EndpntSpec("localhost", bind_addr - 1)
		self.logger.debug('bound to %s + 1' % str(ep))

		pubmsg = dcmsg.MARCO(ep)
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
			print('Unable to contact base node at root address: %s' % str(root_ep))
			print('Is the base node running?')
			return -1

		assert('POLO' == reqmsg.name)

		if 'start' == self.args.action:
			repmsg = dcmsg.ASSIGN(root_ep, 'root', None)
			repmsg['config-file'] = self.args.configfile.name

		elif 'stop' == self.args.action:
			repmsg = dcmsg.STOP()

		else:
			raise NotImplementedError('unknown root action')

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
