import logging, time, zmq

import dcamp.dcmsg as dcmsg
from dcamp.service.service import Service

class Management(Service):
	'''
	Management Service -- provides functionality for interacting with and controlling
	dCAMP.

	@todo figure out how the multicast/subnets work
	'''

	def __init__(self, context, config):
		super().__init__(context)

		self.config = config
		(self.host, self.port) = self.config.kvdict['/root/endpoint']
		self.nodes = []
		for (group, spec) in self.config.groups.items():
			self.nodes.extend(spec.endpoints)

	def setup(self):
		'''
		setup service for polling.

		@todo does this need to be a separate method?
			why not do it as part of __init__()?
		'''
		assert 0 != len(self.host)
		assert 0 != self.port
		assert self.ctx is not None

		self.bind_endpoint = 'tcp://*:%d' % (self.port)
		self.root_endpoint = 'tcp://%s:%d' % (self.host, self.port)

		self.rep = self.ctx.socket(zmq.REP)
		self.rep.bind(self.bind_endpoint)

		self.pub = self.ctx.socket(zmq.PUB)

		for n in self.nodes:
			base_endpoint = "tcp://%s:%d" % (n.host, n.port)
			self.pub.connect(base_endpoint)

		self.reqcnt = 0
		self.repcnt = 0

		self.pubint = self.config.kvdict['/root/heartbeat']
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

				repmsg = dcmsg.ASSIGN(reqmsg.base_endpoint)
				repmsg.send(self.rep)
				self.logger.info("S:ASSIGN")
				self.repcnt += 1
