import logging, time, zmq
from datetime import datetime

import dcamp.data.messages as dcmsg
from dcamp.service.service import Service
from dcamp.data.specs import EndpntSpec

class Management(Service):
	'''
	Management Service -- provides functionality for interacting with and controlling
	dCAMP.

	@todo: how to handle joining nodes not part of config / issue #26
	'''

	def __init__(self,
			pipe,
			config):
		Service.__init__(self, pipe)

		self.config = config
		self.endpoint = self.config.root['endpoint']

		# [endpoint]
		nodes = []
		for group in self.config.groups.values():
			nodes.extend(group.endpoints)

		# { group: collector }
		self.collectors = {}
		# { endpoint: last-seen-timestamp }
		self.remote_nodes = {}

		####
		# setup service for polling.

		# we receive join requests on this socket
		self.join_socket = self.ctx.socket(zmq.REP)
		self.join_socket.bind(self.endpoint.bind_uri(EndpntSpec.TOPO_JOIN))

		# we send topo discovery messages on this socket
		self.disc_socket = self.ctx.socket(zmq.PUB)

		for ep in nodes:
			self.disc_socket.connect(ep.connect_uri(EndpntSpec.TOPO_BASE))

		self.reqcnt = 0
		self.repcnt = 0

		self.pubint = self.config.root['heartbeat']
		self.pubcnt = 0

		self.pubmsg = dcmsg.MARCO(self.endpoint)
		self.pubnext = time.time()

		self.poller.register(self.join_socket, zmq.POLLIN)

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d pubs; %d reqs; %d reps" %
				(self.pubcnt, self.reqcnt, self.repcnt))

		for (ep, date) in self.remote_nodes.items():
			self.logger.debug('%s last seen %s' % (str(ep), date))

		self.join_socket.close()
		self.disc_socket.close()
		del self.join_socket, self.disc_socket
		super()._cleanup()

	def _pre_poll(self):
		if self.pubnext < time.time():
			self.pubmsg.send(self.disc_socket)
			self.pubnext = time.time() + self.pubint
			self.pubcnt += 1

		self.poller_timer = 1e3 * max(0, self.pubnext - time.time())

	def _post_poll(self, items):
		if self.join_socket in items:
			repmsg = None
			try:
				reqmsg = dcmsg.DCMsg.recv(self.join_socket)
				self.reqcnt += 1
				assert reqmsg.name == 'POLO'

				if reqmsg.base_endpoint not in self.remote_nodes:
					repmsg = self.__assign(reqmsg.base_endpoint)
				self.remote_nodes[reqmsg.base_endpoint] = datetime.now()

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
		parent_endpoint = None
		level = ''

		# XXX: store assigned endpoints into /topo hiearchy for distribution to other
		# config nodes

		# lookup node group
		# @todo need to keep track of nodes which have already POLO'ed / issue #39
		for (group, spec) in self.config.groups.items():
			if given_endpoint in spec.endpoints:
				self.logger.debug('found base group: %s' % group)
				if group in self.collectors:
					parent_endpoint = self.collectors[group]
					level = 'leaf'
				else:
					self.collectors[group] = given_endpoint
					parent_endpoint = self.endpoint
					level = 'branch'

		if parent_endpoint is None:
			# silently ignore unknown base endpoints
			self.logger.debug('no base group found for %s' % str(given_endpoint))
			# @todo: cannot return None--using strict REQ/REP pattern / issue #26
			return None

		# create reply message
		return dcmsg.ASSIGN(parent_endpoint, level)
