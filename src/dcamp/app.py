'''
@author: Alexander
'''
import logging
import time

import zmq
from zmq.eventloop.ioloop import PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream

from zhelpers import dump

class App:
	'''
	This is the main dCAMP application.
	'''

	def __init__(self, args):

		logging.basicConfig()
		self.logger = logging.getLogger('dcamp')
		self.__args = args

		self.__setup_logging()
		self.__run()

	def __run(self):
		ctx = zmq.Context.instance()

		if self.__args.is_root:
			pub = ctx.socket(zmq.PUB)
			pub.connect("tcp://localhost:%d" % self.__args.port)
			while True:
				pub.send_string("hello subscriber")
				time.sleep(2)

		elif self.__args.is_base:
			sub = ctx.socket(zmq.SUB)
			sub.setsockopt_string(zmq.SUBSCRIBE, '')
			sub.bind("tcp://*:%d" % self.__args.port)
			count = 0
			while True:
				count = count + 1
				poller = zmq.Poller()
				poller.register(sub, zmq.POLLIN)
				try:
					items = dict(poller.poll(10000))
				except:
					raise
					break

				if sub in items:
					msg = sub.recv_string()
					print("[%d] %s" % (count, msg))
				else:
					print("[%d] no message" % count)


	def __setup_logging(self):
		if (self.__args.verbose):
			self.logger.setLevel(logging.DEBUG)
			self.logger.debug('set logging level to verbose')
