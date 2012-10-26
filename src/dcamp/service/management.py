import logging, time, zmq

import dcamp.dcmsg as dcmsg
from dcamp.service.service import Service
from dcamp.config import DCParsingError, str_to_ep

class Management(Service):
	'''
	Management Service -- provides functionality for interacting with and controlling
	dCAMP.

	@todo: figure out how the multicast/subnets work
	@todo: how to handle joining nodes not part of config
	'''

	def __init__(self, context, config):
		super().__init__(context)

		self.config = config
		self.endpoint = self.config.root['endpoint']

		# [endpoint]
		self.nodes = []
		for group in self.config.groups.values():
			self.nodes.extend(group.endpoints)

		# {group: collector}
		self.collectors = {}

	def setup(self):
		'''
		setup service for polling.

		@todo does this need to be a separate method?
			why not do it as part of __init__()?
		'''
		assert self.ctx is not None

		self.bind_endpoint = 'tcp://*:%d' % (self.endpoint.port)
		self.root_endpoint = 'tcp://%s:%d' % (self.endpoint.host, self.endpoint.port)

		self.rep = self.ctx.socket(zmq.REP)
		self.rep.bind(self.bind_endpoint)

		self.pub = self.ctx.socket(zmq.PUB)

		for n in self.nodes:
			base_endpoint = "tcp://%s:%d" % (n.host, n.port)
			self.pub.connect(base_endpoint)

		self.reqcnt = 0
		self.repcnt = 0

		self.pubint = self.config.root['heartbeat']
		self.pubcnt = 0

	def poll(self):

		pubmsg = dcmsg.MARCO(self.root_endpoint.encode())
		pubnext = time.time()

		while True:
			poller = zmq.Poller()
			poller.register(self.rep, zmq.POLLIN)

			if pubnext < time.time():
				pubmsg.send(self.pub)
				self.logger.info("S:MARCO")
				pubnext = time.time() + self.pubint
				self.pubcnt += 1

			poller_timer = 1e3 * max(0, pubnext - time.time())

			try:
				items = dict(poller.poll(poller_timer))
			except:
				print("keyboard interrupt; root exiting\n%d pubs\n%d reqs\n%d reps" %
						(self.pubcnt, self.reqcnt, self.repcnt))
				return

			if self.rep in items:
				reqmsg = dcmsg.DCMsg.recv(self.rep)
				self.logger.info("C:POLO")
				self.reqcnt += 1
				assert reqmsg.name == b'POLO'

				repmsg = self.__assign(reqmsg.base_endpoint.decode())
				if repmsg is not None:
					repmsg.send(self.rep)
					self.repcnt += 1

	def __assign(self, given_endpoint):
		'''
		Method to handle assigning joining node to topology:
		* lookup node's group
		* promote to collector (if first in group)
		* assign its parent node

		Returns ASSIGN or WTF or None
		'''
		try:
			base_endpoint = str_to_ep(given_endpoint)
		except DCParsingError as e:
			errstr = 'invalid base endpoint received: %s' % e
			self.logger.error(errstr)
			self.logger.info("S:WTF")
			return dcmsg.WTF(0, errstr)

		parent_endpoint = None
		level = ''

		# lookup node group
		for (group, spec) in self.config.groups.items():
			if base_endpoint in spec.endpoints:
				self.logger.debug('found base group: %s' % group)
				if group in self.collectors:
					parent_endpoint = self.collectors[group]
					level = 'leaf'
				else:
					self.collectors[group] = base_endpoint
					parent_endpoint = self.endpoint
					level = 'branch'

		if parent_endpoint is None:
			# silently ignore unknown base endpoints
			self.logger.debug('no base group found for %s' % str(base_endpoint))
			self.logger.debug(base_endpoint)
			return None

		# create reply message
		repmsg = dcmsg.ASSIGN(parent_endpoint.encode())
		repmsg['parent'] = parent_endpoint
		repmsg['level'] = level
		self.logger.info("S:ASSIGN")
		return repmsg
