'''
@author: Alexander
'''
import logging
import time

import zmq
from zmq.eventloop.ioloop import PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream

from zhelpers import dump

import dcmsg

class App:
	'''
	This is the main dCAMP application.
	'''

	def __init__(self, args):

		logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
		self.logger = logging.getLogger('dcamp')
		self.__args = args

		self.__setup_logging()
		self.__run()

	def __run(self):
		ctx = zmq.Context.instance()

		if self.__args.root_port:
			bind_endpoint = 'tcp://*:%s' % self.__args.root_port
			root_endpoint = 'tcp://localhost:%s' % self.__args.root_port
			base_endpoint = "tcp://localhost:%d" % self.__args.base_port

			rep = ctx.socket(zmq.REP)
			rep.bind(bind_endpoint)

			reqcnt = 0
			repcnt = 0

			pub = ctx.socket(zmq.PUB)
			pub.connect(base_endpoint)

			pubmsg = dcmsg.MARCO(root_endpoint.encode())
			pubint = 5.0 # seconds
			pubcnt = 0
			pubnext = time.time()

			while True:
				poller = zmq.Poller()
				poller.register(rep, zmq.POLLIN)

				if pubnext < time.time():
					pubmsg.send(pub)
					self.logger.info("S:MARCO")
					pubnext = time.time() + pubint
					pubcnt = pubcnt + 1

				poller_timer = 1e3 * max(0, pubnext - time.time())

				try:
					items = dict(poller.poll(poller_timer))
				except:
					print("keyboard interrupt; root exiting\n%d pubs\n%d reqs\n%d reps" %
							(pubcnt, reqcnt, repcnt))
					return

				if rep in items:
					reqmsg = dcmsg.DCMsg.recv(rep)
					self.logger.info("C:POLO")
					reqcnt = reqcnt + 1
					assert reqmsg.name == b'POLO'
					repmsg = dcmsg.ASSIGN(reqmsg.base_endpoint)
					repmsg.send(rep)
					self.logger.info("S:ASSIGN")
					repcnt = repcnt + 1

		elif self.__args.base_port:
			bind_endpoint = "tcp://*:%d" % self.__args.base_port
			base_endpoint = "tcp://localhost:%d" % self.__args.base_port

			sub = ctx.socket(zmq.SUB)
			sub.setsockopt_string(zmq.SUBSCRIBE, '')
			sub.bind(bind_endpoint)

			req = None

			subcnt = 0
			reqcnt = 0
			repcnt = 0

			BASE = 0
			JOIN = 1

			state = BASE
			while True:
				poller = zmq.Poller()
				poller.register(sub, zmq.POLLIN)

				if JOIN == state:
					assert req is not None
					poller.register(req, zmq.POLLIN)

				try:
					items = dict(poller.poll())
				except:
					# assume keyboard interrupt
					print("keyboard interrupt; base exiting\n%d subs\n%d reqs\n%d reps" %
							(subcnt, reqcnt, repcnt))
					return

				if sub in items:
					submsg = dcmsg.DCMsg.recv(sub)
					self.logger.info("S:MARCO")
					subcnt = subcnt + 1
					assert submsg.name == b'MARCO'

					if BASE == state:
						req = ctx.socket(zmq.REQ)
						req.connect(submsg.root_endpoint)
						reqmsg = dcmsg.POLO(base_endpoint.encode())
						reqmsg.send(req)
						self.logger.info("C:POLO")
						reqcnt = reqcnt + 1
						state = JOIN

				elif req in items:
					assert state == JOIN
					repmsg = dcmsg.DCMsg.recv(req)
					self.logger.info("S:ASSIGN")
					del(req)
					req = None
					repcnt = repcnt + 1
					state = BASE
					assert repmsg.name == b'ASSIGN'

	def __setup_logging(self):
		if (self.__args.verbose):
			self.logger.setLevel(logging.INFO)
			self.logger.debug('set logging level to verbose')
		elif (self.__args.debug):
			self.logger.setLevel(logging.DEBUG)
			self.logger.debug('set logging level to debug')
