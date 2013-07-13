import logging, time, threading, zmq

from zhelpers import zpipe

import dcamp.data.message as dcmsg
from dcamp.service.service import Service
from dcamp.role.root import Root
from dcamp.data.config import DCConfig

class Node(Service):
	'''
	Node Service -- provides functionality for boot strapping into dCAMP system.

	@todo: need to timeout if the req fails / issue #28
	'''

	BASE = 0
	JOIN = 1
	PLAY = 4

	def __init__(self,
			pipe,
			endpoint,
			topics=None):
		Service.__init__(self, pipe)

		self.endpoint = endpoint
		self.topics = [] if topics is None else topics

		####
		# setup service for polling.

		self.bind_endpoint = "tcp://*:%d" % (self.endpoint.port())

		self.disc_socket = self.ctx.socket(zmq.SUB)

		for t in self.topics:
			self.disc_socket.setsockopt_string(zmq.SUBSCRIBE, t)
		if len(self.topics) == 0:
			self.disc_socket.setsockopt_string(zmq.SUBSCRIBE, '')

		self.disc_socket.bind(self.bind_endpoint)

		self.join_socket = None

		self.subcnt = 0
		self.reqcnt = 0
		self.repcnt = 0

		self.role = None

		self.state = Node.BASE

		self.poller.register(self.disc_socket, zmq.POLLIN)

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d subs; %d reqs; %d reps" %
				(self.subcnt, self.reqcnt, self.repcnt))

		self.disc_socket.close()
		if self.join_socket:
			self.join_socket.close()
		del self.disc_socket, self.join_socket
		super()._cleanup()

	def _post_poll(self, items):
		if self.disc_socket in items:
			submsg = dcmsg.DCMsg.recv(self.disc_socket)
			self.subcnt += 1
			assert submsg.name == b'MARCO'

			if Node.BASE == self.state:
				self.join_socket = self.ctx.socket(zmq.REQ)
				self.join_socket.connect("tcp://%s" % submsg.root_endpoint)
				self.poller.register(self.join_socket, zmq.POLLIN)
				reqmsg = dcmsg.POLO(self.endpoint)
				reqmsg.send(self.join_socket)
				self.reqcnt += 1
				self.state = Node.JOIN

		elif self.join_socket in items:
			assert Node.JOIN == self.state
			assignment = dcmsg.DCMsg.recv(self.join_socket)
			self.poller.unregister(self.join_socket)
			self.join_socket.close()
			del self.join_socket
			self.join_socket = None
			self.repcnt += 1

			assert assignment.name in [b'ASSIGN', b'WTF']
			for part in str(assignment).split('\n'):
				self.logger.debug(part)

			if b'WTF' == assignment.name:
				self.logger.error('WTF(%d): %s' % (assignment.errcode, assignment.errstr))
				return
			if 'level' not in assignment.properties:
				self.logger.error('property missing: level')
				return

			# if level == root, start Root role.
			if 'root' == assignment['level']:
				assert 'config-file' in assignment.properties
				config = DCConfig()
				config.read_file(open(assignment['config-file']))
				self.role_pipe, peer = zpipe(self.ctx)
				self.role = Root(peer, config)
				self.state = Node.PLAY

			else:
				self.logger.error('unknown role assignment')
				self.state = Node.BASE
				return

			# if level == branch, start Collector role.
			# if level == leaf, start Metrics role.

			self._play_role()

	def _play_role(self):
		# start thread
		assert self.role is not None

		self.logger.debug('starting Role: %s' % self.role)
		self.role_thread = threading.Thread(target=self.role.play)
		self.role_thread.start()
