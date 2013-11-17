import logging
from time import time

from zmq import REP, PUB, POLLIN # pylint: disable-msg=E0611

from dcamp.types.messages.common import WTF
import dcamp.types.messages.topology as TopoMsg

from dcamp.service.service import Service_Mixin
from dcamp.types.specs import EndpntSpec
from dcamp.types.topo import TopoTree_Mixin, TopoNode

class Management(Service_Mixin):
	'''
	Management Service -- provides functionality for interacting with and controlling
	dCAMP.

	@todo: how to handle joining nodes not part of config / issue #26
	'''

	def __init__(self,
			role_pipe,
			config_service,
			config,
			):
		Service_Mixin.__init__(self, role_pipe)

		# TODO: use config_service as full state representation; add accessor methods to
		#       make it convenient and remove the self.config and self.tree members.
		#       IDEA: create subclass of Configuration service class which provides these
		#       additional methods? how would that work when a branch is promoted to root?
		self.config_service = config_service
		self.config = config
		self.endpoint = self.config.root['endpoint']
		self.uuid = TopoMsg.gen_uuid()

		for (k, v) in self.config.kvdict.items():
			self.config_service[k] = v

		# 1) start tree with self as root
		# 2) add each node to tree as topo-node
		# tree.nodes contains { node-endpoint: topo-node }
		# topo-node contains (endpoint, role, group, parent, children, last-seen)
		# topo keys come from tree.get_topo_key(node)

		self.tree = TopoTree_Mixin(self.endpoint, self.uuid)
		self.config_service[self.tree.get_topo_key(self.tree.root)] = 0

		# { group: collector-topo-node }
		self.collectors = {}

		####
		# setup service for polling.

		# we receive join requests on this socket
		self.join_socket = self.ctx.socket(REP)
		self.join_socket.bind(self.endpoint.bind_uri(EndpntSpec.TOPO_JOIN))

		# we send topo discovery messages on this socket
		self.disc_socket = self.ctx.socket(PUB)

		for group in self.config.groups.values():
			for ep in group.endpoints:
				self.disc_socket.connect(ep.connect_uri(EndpntSpec.TOPO_BASE))

		self.reqcnt = 0
		self.repcnt = 0

		self.pubint = self.config.root['heartbeat']
		self.pubcnt = 0

		self.marco_msg = TopoMsg.MARCO(self.endpoint, self.uuid)
		self.pubnext = time()

		self.poller.register(self.join_socket, POLLIN)

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d pubs; %d reqs; %d reps" %
				(self.pubcnt, self.reqcnt, self.repcnt))

		self.tree.print()
		for (key, node) in self.tree.walk():
			self.logger.debug('%s last seen %s' % (str(node.endpoint), node.last_seen))

		self.join_socket.close()
		self.disc_socket.close()
		del self.join_socket, self.disc_socket
		super()._cleanup()

	def _pre_poll(self):
		if self.pubnext < time():
			self.marco_msg.send(self.disc_socket)
			self.pubnext = time() + self.pubint
			self.pubcnt += 1

		self.poller_timer = 1e3 * max(0, self.pubnext - time())

	def _post_poll(self, items):
		if self.join_socket in items:
			polo_msg = TopoMsg.POLO.recv(self.join_socket)
			self.reqcnt += 1

			repmsg = None

			if polo_msg.is_error:
				errstr = 'invalid base endpoint received: %s' % (polo_msg.errstr)
				self.logger.error(errstr)
				repmsg = WTF(0, errstr)

			else:
				remote = self.tree.find_node_by_endpoint(polo_msg.endpoint)
				if remote is None:
					repmsg = self.__assign(polo_msg)

				elif remote.uuid != polo_msg.uuid:
					# node recovered before it was recognized as down; just resend the
					# same assignment info as before
					self.logger.debug('%s rePOLOed with new UUID' % str(remote.endpoint))
					remote.uuid = polo_msg.uuid
					remote.touch()
					repmsg = remote.assignment()

				else:
					repmsg = WTF(0, 'too chatty; already POLOed')
					remote.touch()

			repmsg.send(self.join_socket)
			self.repcnt += 1

	def __assign(self, polo_msg):
		'''
		Method to handle assigning joining node to topology:
		* lookup node's group
		* promote to collector (if first in group)
		* assign its parent node

		Returns CONTROL or None
		'''

		# lookup node group
		# @todo need to keep track of nodes which have already POLO'ed / issue #39
		for (group, spec) in self.config.groups.items():
			if polo_msg.endpoint in spec.endpoints:
				self.logger.debug('found base group: %s' % group)

				parent = None
				level = ''

				if group in self.collectors:
					# group already exists, make sensor (leaf) node
					parent = self.collectors[group]
					level = 'leaf'
				else:
					# first node in group, make collector
					parent = self.tree.root
					level = 'branch'

				node = TopoNode(polo_msg.endpoint, polo_msg.uuid, level, group)
				if parent == self.tree.root:
					self.collectors[group] = node

				node.touch()
				node = self.tree.insert_node(node, parent)
				self.config_service[self.tree.get_topo_key(node)] = node.last_seen

				# create reply message
				return node.assignment()

		# silently ignore unknown base endpoints
		self.logger.debug('no base group found for %s' % str(polo_msg.endpoint))
		# @todo: cannot return None--using strict REQ/REP pattern / issue #26
		return None
