import logging
import zmq

class Role(object):
	logger = logging.getLogger('dcamp.role')

	def __init__(self,
			context,
			pipe):
		self.ctx = context
		self.pipe = pipe

		self.pub = self.ctx.socket(zmq.PUB)
		self.pub.bind('inproc://%s' % self.__class__)

	def play(self):
		# start each service thread
		for s in self.services:
			s.daemon = True
			s.start()

		# listen for control commands from caller
		while True:
			try:
				msg = self.pipe.recv_string()
				if ('STOP' == msg):
					self.pipe.send_string('OKAY')
					self.logger.debug('received STOP control command')
					return self._stop()
				else:
					self.pipe.send_string('WTF')
					self.logger.error('unknown control command: %s' % msg)
			except zmq.ZMQError as e:
				if e.errno == zmq.ETERM:
					return self._stop() # interrupted

	def _stop(self):
		self.logger.debug('STOPPING')
		# @todo: send stop signal to all services and join

	def _some_alive(self):
		'''returns True if at least one service of this Role is still running'''
		for s in self.services:
			if s.is_alive:
				return True
		return False
