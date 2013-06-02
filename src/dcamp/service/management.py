import logging, time, zmq

import dcamp.data.message as dcmsg
from dcamp.service.service import Service
from dcamp.data.config import EndpntSpec

class Management(Service):
	'''
	Management Service -- provides functionality for interacting with and controlling
	dCAMP.

	@todo: how to handle joining nodes not part of config / issue #26
	'''

	def __init__(self,
			pipe,
			config):
		super().__init__(pipe)

		self.config = config
		self.endpoint = self.config.root['endpoint']

		# [endpoint]
		self.nodes = []
		for group in self.config.groups.values():
			self.nodes.extend(group.endpoints)

		# {group: collector}
		self.collectors = {}

		####
		# setup service for polling.

		self.bind_endpoint = 'tcp://*:%d' % (self.endpoint.port(EndpntSpec.ROOT_DISCOVERY))

		self.rep = self.ctx.socket(zmq.REP)
		self.rep.bind(self.bind_endpoint)

		self.pub = self.ctx.socket(zmq.PUB)

		for n in self.nodes:
			base_endpoint = "tcp://%s:%d" % (n.host, n.port())
			self.pub.connect(base_endpoint)

		self.reqcnt = 0
		self.repcnt = 0

		self.pubint = self.config.root['heartbeat']
		self.pubcnt = 0

		self.pubmsg = dcmsg.MARCO(self.endpoint)
		self.pubnext = time.time()

		self.poller.register(self.rep, zmq.POLLIN)

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d pubs; %d reqs; %d reps" %
				(self.pubcnt, self.reqcnt, self.repcnt))

		self.rep.close()
		self.pub.close()
		del self.rep, self.pub
		super()._cleanup()

	def _pre_poll(self):
		if self.pubnext < time.time():
			self.pubmsg.send(self.pub)
			self.pubnext = time.time() + self.pubint
			self.pubcnt += 1

		self.poller_timer = 1e3 * max(0, self.pubnext - time.time())

	def _post_poll(self, items):
		if self.rep in items:
			try:
				reqmsg = dcmsg.DCMsg.recv(self.rep)
				self.reqcnt += 1
				assert reqmsg.name == b'POLO'
				repmsg = self.__assign(reqmsg.base_endpoint)
			except ValueError as e:
				errstr = 'invalid base endpoint received: %s' % e
				self.logger.error(errstr)
				repmsg = dcmsg.WTF(0, errstr)

			if repmsg is not None:
				repmsg.send(self.rep)
				self.repcnt += 1

	def __assign(self, given_endpoint):
		'''
		Method to handle assigning joining node to topology:
		* lookup node's group
		* promote to collector (if first in group)
		* assign its parent node

		Returns ASSIGN or None
		'''
		parent_endpoint = None
		level = ''

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
		msg = dcmsg.ASSIGN(parent_endpoint)
		msg['level'] = level
		return msg
