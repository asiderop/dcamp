import logging, zmq

class Role(object):
	logger = logging.getLogger('dcamp.role')

	def __init__(self,
			pipe):
		self.ctx = zmq.Context.instance()
		self.pipe = pipe

		# { pipe: service, ...}
		self.services = {}

	def play(self):
		# start each service thread
		for s in self.services.values():
			s.start()

		# @todo: wait for READY message from each service / issue #37

		self.logger.debug('waiting for control commands')
		# listen for control commands from caller
		while True:
			try:
				msg = self.pipe.recv_string()

				if ('STOP' == msg):
					self.pipe.send_string('OKAY')
					self.logger.debug('received STOP control command')
					break
				else:
					self.pipe.send_string('WTF')
					self.logger.error('unknown control command: %s' % msg)

			except zmq.ZMQError as e:
				if e.errno == zmq.ETERM:
					self.logger.debug('received ETERM')
					break
				else:
					raise
			except KeyboardInterrupt: # only for roles played by dcamp.App
				self.logger.debug('received KeyboardInterrupt')
				break

		# role is exiting; cleanup
		return self._cleanup()

	def _stop(self):
		# @todo: send stop signal to all services and join / issue #36
		pass

	def _cleanup(self):
		# try to stop all of our services
		self._stop()

		# shared context; will be term()'ed by caller

		# close all service sockets
		for p in self.services:
			p.close()
		del self.services

		# close our own control pipe
		self.pipe.close()
		del self.pipe

	def _some_alive(self):
		'''returns True if at least one service of this Role is still running'''
		for s in self.services.values():
			if s.is_alive:
				return True
		return False
