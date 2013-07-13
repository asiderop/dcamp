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

		self.topo_endpoint = "tcp://*:%d" % (self.endpoint.port())

		# @todo these sockets need a better naming convention.
		self.topo_socket = self.ctx.socket(zmq.SUB)

		for t in self.topics:
			self.topo_socket.setsockopt_string(zmq.SUBSCRIBE, t)
		if len(self.topics) == 0:
			self.topo_socket.setsockopt_string(zmq.SUBSCRIBE, '')

		self.topo_socket.bind(self.topo_endpoint)

		self.control_socket = None

		self.subcnt = 0
		self.reqcnt = 0
		self.repcnt = 0

		self.role = None

		self.state = Node.BASE

		self.poller.register(self.topo_socket, zmq.POLLIN)

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d subs; %d reqs; %d reps" %
				(self.subcnt, self.reqcnt, self.repcnt))

		self.topo_socket.close()
		if self.control_socket:
			self.control_socket.close()
		del self.topo_socket, self.control_socket
		super()._cleanup()

	def _post_poll(self, items):
		if self.topo_socket in items:
			marco = dcmsg.DCMsg.recv(self.topo_socket)
			self.subcnt += 1
			assert marco.name == b'MARCO'

			# @todo: do we care which state we are in?
			# if Node.BASE == self.state:

			self.control_socket = self.ctx.socket(zmq.REQ)
			self.control_socket.connect("tcp://%s" % marco.root_endpoint)
			self.poller.register(self.control_socket, zmq.POLLIN)
			polo = dcmsg.POLO(self.endpoint)
			polo.send(self.control_socket)
			self.reqcnt += 1
			if Node.BASE == self.state:
				self.state = Node.JOIN

		elif self.control_socket in items:
			response = dcmsg.DCMsg.recv(self.control_socket)
			self.poller.unregister(self.control_socket)
			self.control_socket.close()
			del self.control_socket
			self.control_socket = None
			self.repcnt += 1

			assert response.name in [b'CONTROL', b'WTF']

			if b'WTF' == response.name:
				self.logger.error('WTF(%d): %s' % (response.errcode, response.errstr))
				return
			if 'command' not in response.properties:
				self.logger.error('property missing: command')
				return

			command = response.properties['command']

			if 'assignment' == command:
				# @todo need to handle re-assignment
				assert Node.JOIN == self.state

				if 'level' not in response.properties:
					self.logger.error('property missing: level')
					return

				level = response['level']
				# if level == root, start Root role.
				if 'root' == level:
					assert 'config-file' in response.properties
					config = DCConfig()
					config.read_file(open(response['config-file']))
					self.role_pipe, peer = zpipe(self.ctx)
					self.role = Root(peer, config)
					self.state = Node.PLAY

				# if level == branch, start Collector role.
				# if level == leaf, start Metrics role.
				else:
					self.logger.error('unknown assignment level: %s' % level)
					self.state = Node.BASE
					return

				self._play_role()

			elif 'stop' == command:
				assert Node.PLAY == self.state

				self.role_pipe.send_string('STOP')
				response = self.role_pipe.recv_string()
				assert 'OKAY' == response

				self.role = None
				self.role_pipe.close()
				del self.role_pipe
				self.role_pipe = None

				# @todo: wait for thread to exit?

				self.role_thread.join(timeout=60)
				if self.role_thread.isAlive():
					self.logger.error('role is still alive!')
				del self.role_thread

				self.state = Node.BASE

			else:
				self.logger.error('unknown control command: %s' % command)
				self.state = Node.BASE
				return

	def _play_role(self):
		# start thread
		assert self.role is not None

		self.logger.debug('starting Role: %s' % self.role)
		self.role_thread = threading.Thread(target=self.role.play)
		self.role_thread.start()
