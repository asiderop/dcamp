import logging, threading
import zmq

class Role(object):
	logger = logging.getLogger('dcamp.role')

	def __init__(self):
		self.ctx = zmq.Context.instance()

	def play(self):
		# for each service, setup and start thread
		for s in self.services:
			s.setup()
			s.daemon = True
			s.start()

		try:
			for s in self.services:
				s.join()
		except KeyboardInterrupt:
			return
