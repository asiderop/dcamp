import logging, time, zmq

import dcamp.dcmsg as dcmsg
from dcamp.service.service import Service

BASE = 0
JOIN = 1

class Node(Service):
	'''
	Node Service -- provides functionality for boot strapping into dCAMP system.

	@todo: need to timeout if the req fails / issue #28
	'''

	def __init__(self,
			pipe,
			endpoint,
			topics=None):
		super().__init__(pipe)

		self.endpoint = endpoint
		self.topics = [] if topics is None else topics

		####
		# setup service for polling.

		self.bind_endpoint = "tcp://*:%d" % (self.endpoint.port)

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

		self.poller.register(self.sub, zmq.POLLIN)

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d subs; %d reqs; %d reps" %
				(self.subcnt, self.reqcnt, self.repcnt))

		self.sub.close()
		if self.req:
			self.req.close()
		del self.sub, self.req
		super()._cleanup()

	def _post_poll(self, items):
		if self.sub in items:
			submsg = dcmsg.DCMsg.recv(self.sub)
			self.subcnt += 1
			assert submsg.name == b'MARCO'

			if BASE == self.state:
				self.req = self.ctx.socket(zmq.REQ)
				self.req.connect("tcp://%s" % submsg.root_endpoint)
				self.poller.register(self.req, zmq.POLLIN)
				reqmsg = dcmsg.POLO(self.endpoint)
				reqmsg.send(self.req)
				self.reqcnt += 1
				self.state = JOIN

		elif self.req in items:
			assert self.state == JOIN
			repmsg = dcmsg.DCMsg.recv(self.req)
			self.poller.unregister(self.req)
			self.req.close()
			del self.req
			self.req = None
			self.repcnt += 1
			self.state = BASE
			assert repmsg.name in [b'ASSIGN', b'WTF']
