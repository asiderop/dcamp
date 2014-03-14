import threading

from zmq import DEALER, SUB, SUBSCRIBE, POLLIN # pylint: disable-msg=E0611
from zhelpers import zpipe

import dcamp.types.messages.topology as TopoMsg

from dcamp.role.root import Root
from dcamp.role.collector import Collector
from dcamp.role.metric import Metric

from dcamp.service.service import Service_Mixin
from dcamp.types.config_file import DCConfig_Mixin
from dcamp.types.specs import EndpntSpec

class Node(Service_Mixin):
	'''
	Node Service -- provides functionality for boot strapping into dCAMP system.

	@todo: need to timeout if the req fails / issue #28
	'''

	BASE = 0
	PLAY = 4

	def __init__(self,
			pipe,
			endpoint):
		Service_Mixin.__init__(self, pipe)

		self.endpoint = endpoint
		self.uuid = TopoMsg.gen_uuid()
		self.polo_msg = TopoMsg.POLO(self.endpoint, self.uuid)

		####
		# setup service for polling.

		self.topo_endpoint = self.endpoint.bind_uri(EndpntSpec.BASE)
		self.logger.debug('binding to %s' % self.topo_endpoint)

		# @todo these sockets need a better naming convention.
		self.topo_socket = self.ctx.socket(SUB)
		self.topo_socket.setsockopt_string(SUBSCRIBE, '')

		self.topo_socket.bind(self.topo_endpoint)

		self.control_socket = None

		self.subcnt = 0
		self.reqcnt = 0
		self.repcnt = 0

		self.role = None

		self.state = Node.BASE

		self.poller.register(self.topo_socket, POLLIN)

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d subs; %d reqs; %d reps" %
				(self.subcnt, self.reqcnt, self.repcnt))

		self.topo_socket.close()
		if self.control_socket:
			self.control_socket.close()
		del self.topo_socket, self.control_socket
		Service_Mixin._cleanup(self)

	def _post_poll(self, items):
		if self.topo_socket in items:
			marco_msg = TopoMsg.MARCO.recv(self.topo_socket)
			self.subcnt += 1

			if marco_msg.is_error:
				self.logger.error('topo message error: %s' % marco_msg.errstr)
				return

			# @todo: add some security here so not just anyone can shutdown the root node
			self.control_socket = self.ctx.socket(DEALER)
			self.control_socket.connect(marco_msg.endpoint.connect_uri(EndpntSpec.TOPO_JOIN))
			self.poller.register(self.control_socket, POLLIN)
			self.polo_msg._peer_id = marco_msg._peer_id
			self.polo_msg.send(self.control_socket)
			self.reqcnt += 1

		elif self.control_socket in items:
			response = TopoMsg.CONTROL.recv(self.control_socket)
			self.poller.unregister(self.control_socket)
			self.control_socket.close()
			del self.control_socket
			self.control_socket = None
			self.repcnt += 1

			if response.is_error:
				self.logger.error(response)
				return

			if 'assignment' == response.command:
				self.__handle_assignment(response)
				return

			elif 'stop' == response.command:
				if Node.PLAY != self.state:
					self.logger.error('role not running; nothing to stop')
					return

				self.role_pipe.send_string('STOP')
				response = self.role_pipe.recv_string()
				assert 'OKAY' == response

				self.logger.debug('received STOP OKAY from %s role' % self.role)

				self.role = None
				self.role_pipe.close()
				del self.role_pipe
				self.role_pipe = None

				# @todo: wait for thread to exit?

				self.role_thread.join(timeout=60)
				if self.role_thread.isAlive():
					self.logger.error('role is still alive!')
				del self.role_thread

				self.logger.debug('node stopped; back to BASE')

				self.state = Node.BASE

			else:
				self.logger.error('unknown control command: %s' % response.command)
				return

	def __handle_assignment(self, response):
		# @todo need to handle re-assignment
		if Node.BASE != self.state:
			self.logger.warning('received re-assignment; ignoring')
			return

		if 'level' not in response.properties:
			self.logger.error('property missing: level')
			return

		level = response['level']
		# if level == root, start Root role.
		if 'root' == level:
			assert 'config-file' in response.properties
			config = DCConfig_Mixin()
			config.read_file(open(response['config-file']))
			self.role_pipe, peer = zpipe(self.ctx)
			self.role = Root(peer, config)

		# if level == branch, start Collector role.
		elif 'branch' == level:
			self.role_pipe, peer = zpipe(self.ctx)
			self.role = Collector(peer,
					response['group'],
					response['parent'],
					self.endpoint)

		# if level == leaf, start Metrics role.
		elif 'leaf' == level:
			self.role_pipe, peer = zpipe(self.ctx)
			self.role = Metric(peer,
					response['group'],
					response['parent'],
					self.endpoint)

		else:
			self.logger.error('unknown assignment level: %s' % level)
			return

		self.__play_role()

	def __play_role(self):
		# start thread
		assert self.role is not None

		self.logger.debug('starting Role: %s' % self.role)
		self.role_thread = threading.Thread(target=self.role.play)
		self.role_thread.start()
		self.state = Node.PLAY
