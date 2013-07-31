import logging, zmq
from time import time

import dcamp.data.messages as dcmsg
from dcamp.service.service import Service
from dcamp.data.specs import EndpntSpec
from dcamp.data.topo import TopoTree, TopoNode

class Management(Service):
	'''
	Management Service -- provides functionality for interacting with and controlling
	dCAMP.

	@todo: how to handle joining nodes not part of config / issue #26
	'''

	def __init__(self,
			role_pipe,
			config_pipe,
			config,
			):
		Service.__init__(self, role_pipe)

		self.config = config
		self.endpoint = self.config.root['endpoint']

		# 1) start tree with self as root
		# 2) add each node to tree as topo-node
		# tree.nodes contains { node-endpoint: topo-node }
		# topo-node contains (endpoint, role, group, parent, children, last-seen)
		# topo keys come from tree.get_topo_key(node)

		self.tree = TopoTree(self.endpoint)

		# { group: collector-topo-node }
		self.collectors = {}

		####
		# setup service for polling.

		# we receive join requests on this socket
		self.join_socket = self.ctx.socket(zmq.REP)
		self.join_socket.bind(self.endpoint.bind_uri(EndpntSpec.TOPO_JOIN))

		# we send topo discovery messages on this socket
		self.disc_socket = self.ctx.socket(zmq.PUB)

		for group in self.config.groups.values():
			for ep in group.endpoints:
				self.disc_socket.connect(ep.connect_uri(EndpntSpec.TOPO_BASE))

		self.reqcnt = 0
		self.repcnt = 0

		self.pubint = self.config.root['heartbeat']
		self.pubcnt = 0

		self.pubmsg = dcmsg.MARCO(self.endpoint)
		self.pubnext = time()

		self.poller.register(self.join_socket, zmq.POLLIN)

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
			self.pubmsg.send(self.disc_socket)
			self.pubnext = time() + self.pubint
			self.pubcnt += 1

		self.poller_timer = 1e3 * max(0, self.pubnext - time())

	def _post_poll(self, items):
		if self.join_socket in items:
			repmsg = None
			try:
				reqmsg = dcmsg.DCMsg.recv(self.join_socket)
				self.reqcnt += 1
				assert reqmsg.name == 'POLO'

				remote = self.tree.find_node_by_endpoint(reqmsg.base_endpoint)
				if remote is None:
					repmsg = self.__assign(reqmsg.base_endpoint)
				else:
					repmsg = dcmsg.WTF(0, 'too chatty; already POLOed')
					remote.touch()

			except ValueError as e:
				errstr = 'invalid base endpoint received: %s' % str(e)
				self.logger.error(errstr)
				repmsg = dcmsg.WTF(0, errstr)

			if repmsg is not None:
				repmsg.send(self.join_socket)
				self.repcnt += 1

	def __assign(self, given_endpoint):
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
			if given_endpoint in spec.endpoints:
				self.logger.debug('found base group: %s' % group)

				parent = None
				level = ''

				if group in self.collectors:
					# group already exists, make sensor
					parent = self.collectors[group]
					level = 'leaf'
				else:
					# first node in group, make collector
					parent = self.tree.root
					level = 'branch'

				node = TopoNode(given_endpoint, level, group)
				if parent == self.tree.root:
					self.collectors[group] = node

				node.touch()
				self.tree.insert_node(node, parent)

				# create reply message
				return dcmsg.ASSIGN(parent.endpoint, level)

		# silently ignore unknown base endpoints
		self.logger.debug('no base group found for %s' % str(given_endpoint))
		# @todo: cannot return None--using strict REQ/REP pattern / issue #26
		return None
