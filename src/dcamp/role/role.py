import logging, zmq

from dcamp.util.decorator import Runnable

@Runnable
class Role(object):
	logger = logging.getLogger('dcamp.role')

	MAX_SERVICE_STOPS = 5

	def __init__(self,
			pipe):
		self.ctx = zmq.Context.instance()
		self._pipe = pipe

		# { pipe: service, ...}
		self.services = {}

	def __str__(self):
		return self.__class__.__name__

	def play(self):
		# start each service thread
		for s in self.services.values():
			s.start()

		# @todo: wait for READY message from each service / issue #37

		self.run_state()
		self.logger.debug('waiting for control commands')

		# listen for control commands from caller
		while self.is_running:
			try:
				msg = self._pipe.recv_string()

				if ('STOP' == msg):
					self._pipe.send_string('OKAY')
					self.logger.debug('received STOP control command')
					self.stop_state()
					break
				else:
					self._pipe.send_string('WTF')
					self.logger.error('unknown control command: %s' % msg)

			except zmq.ZMQError as e:
				if e.errno == zmq.ETERM:
					self.logger.debug('received ETERM')
					self.error_state()
					break
				else:
					raise
			except KeyboardInterrupt: # only for roles played by dcamp.App
				self.logger.debug('received KeyboardInterrupt')
				self.stop_state()
				break

		# role is exiting; cleanup
		return self._cleanup()

	def _stop(self):
		'''try to stop all of this Role's services'''
		attempts = 0

		# send commands
		poller = zmq.Poller()
		for p in self.services:
			p.send_string('STOP')
			poller.register(p, zmq.POLLIN)

		while self._some_alive() and attempts < self.MAX_SERVICE_STOPS:
			attempts += 1

			# poll for any replies
			items = dict(poller.poll(100)) # wait 100ms for messages

			# mark responding services as stopped
			alive = list(self.services.keys()) # make copy of key list
			for p in alive:
				if p in items:
					reply = p.recv_string()
					if 'STOPPED' == reply:
						self.logger.debug('received STOPPED control reply')
						poller.unregister(p)
						p.close()
						del(self.services[p])
					else:
						self.logger.debug('unknown control reply: %s' % reply)

			# send stop command to remaining services
			for p in self.services:
				p.send_string('STOP')

	def _cleanup(self):
		# stop our services cleanly (if we can)
		if not self.is_errored:
			# @todo: this might raise an exception / issue #38
			self._stop()

		# shared context; will be term()'ed by caller

		# close all service sockets
		for p in self.services:
			p.close()
		del self.services

		# close our own control pipe
		self._pipe.close()
		del self._pipe

	def _some_alive(self):
		'''returns True if at least one service of this Role is still running'''
		for s in self.services.values():
			if s.is_alive:
				return True
		return False
