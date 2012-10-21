import logging, time, zmq

import dcamp.dcmsg as dcmsg
from dcamp.service.service import Service

BASE = 0
JOIN = 1

class Node(Service):
	'''
	Node Service -- provides functionality for boot strapping into dCAMP system.
	'''

	def __init__(self,
			context=None,
			port=None,
			topics=None):
		super().__init__(context)

		self.port = 0 if port is None else port
		self.topics = [] if topics is None else topics

	def setup(self):
		'''
		setup service for polling.

		@todo does this need to be a separate method?
			why not do it as part of __init__()?
		'''

		assert 0 != self.port
		assert self.ctx is not None

		self.bind_endpoint = "tcp://*:%d" % self.port
		self.base_endpoint = "tcp://localhost:%d" % self.port

		self.sub = self.ctx.socket(zmq.SUB)

		for t in self.topics:
			self.sub.setsockopt_string(zmq.SUBSCRIBE, t)
		if len(self.topics) == 0:
			self.sub.setsockopt_string(zmq.SUBSCRIBE, '')

		self.sub.bind(self.bind_endpoint)

		self.req = None

		self.subcnt = 0
		self.reqcnt = 0
		self.repcnt = 0

		self.state = BASE

	def poll(self):
		while True:
			poller = zmq.Poller()
			poller.register(self.sub, zmq.POLLIN)

			if JOIN == self.state:
				assert self.req is not None
				poller.register(self.req, zmq.POLLIN)

			try:
				items = dict(poller.poll())
			except:
				# assume keyboard interrupt
				print("keyboard interrupt; base exiting\n%d subs\n%d reqs\n%d reps" %
						(self.subcnt, self.reqcnt, self.repcnt))
				return

			if self.sub in items:
				submsg = dcmsg.DCMsg.recv(self.sub)
				self.logger.info("S:MARCO")
				self.subcnt = self.subcnt + 1
				assert submsg.name == b'MARCO'

				if BASE == self.state:
					self.req = self.ctx.socket(zmq.REQ)
					self.req.connect(submsg.root_endpoint)
					reqmsg = dcmsg.POLO(self.base_endpoint.encode())
					reqmsg.send(self.req)
					self.logger.info("C:POLO")
					self.reqcnt = self.reqcnt + 1
					self.state = JOIN

			elif self.req in items:
				assert self.state == JOIN
				repmsg = dcmsg.DCMsg.recv(self.req)
				self.logger.info("S:ASSIGN")
				del(self.req)
				self.req = None
				self.repcnt = self.repcnt + 1
				self.state = BASE
				assert repmsg.name == b'ASSIGN'
