import logging, zmq
from zhelpers import zpipe

from dcamp.util.decorator import Runnable

@Runnable
class Role(object):

	MAX_SERVICE_STOP_ATTEMPTS = 5

	def __init__(self,
			pipe):
		self.ctx = zmq.Context.instance()
		self.__control_pipe = pipe

		self.logger = logging.getLogger('dcamp.role.'+ self.__class__.__name__)

		# { pipe: service, ...}
		self.__services = {}

	def __str__(self):
		return self.__class__.__name__

	def __send_control(self, message):
		self.__control_pipe.send_string(message)
	def __recv_control(self):
		return self.__control_pipe.recv_string()

	def _add_service(self, ServiceClass, *args, **kwargs):
		pipe, peer = zpipe(self.ctx) # create control socket pair
		service = ServiceClass(peer, *args, **kwargs) # create service, passing peer socket
		self.__services[pipe] = service # add to our dict, using pipe socket as key

	def play(self):
		# start each service thread
		for service in self.__services.values():
			service.start()

		# @todo: wait for READY message from each service / issue #37

		self.run_state()
		self.logger.debug('waiting for control commands')

		# listen for control commands from caller
		while self.is_running:
			try:
				msg = self.__recv_control()

				if ('STOP' == msg):
					self.__send_control('OKAY')
					self.logger.debug('received STOP control command')
					self.stop_state()
					break
				else:
					self.__send_control('WTF')
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
		return self.__cleanup()

	def __stop(self):
		'''try to stop all of this Role's services'''
		attempts = 0

		# send commands
		poller = zmq.Poller()
		for pipe in self.__services:
			pipe.send_string('STOP')
			poller.register(pipe, zmq.POLLIN)

		while self.__some_alive() and attempts < self.MAX_SERVICE_STOP_ATTEMPTS:
			attempts += 1

			# poll for any replies
			items = dict(poller.poll(100)) # wait 100ms for messages

			# mark responding services as stopped
			alive = list(self.__services.keys()) # make copy of key list
			for pipe in alive:
				if pipe in items:
					reply = pipe.recv_string()
					if 'STOPPED' == reply:
						self.logger.debug('received STOPPED control reply')
						poller.unregister(pipe)
						pipe.close()
						del(self.__services[pipe])
					else:
						self.logger.debug('unknown control reply: %s' % reply)

			# send stop command to remaining services
			for pipe in self.__services:
				pipe.send_string('STOP')

	def __cleanup(self):
		# stop our services cleanly (if we can)
		if not self.is_errored:
			# @todo: this might raise an exception / issue #38
			self.__stop()

		# shared context; will be term()'ed by caller

		# close all service sockets
		for pipe in self.__services:
			pipe.close()
		del self.__services

		# close our own control pipe
		self.__control_pipe.close()
		del self.__control_pipe

		self.logger.debug('role cleanup finished; exiting')

	def __some_alive(self):
		'''returns True if at least one service of this Role is still running'''
		for service in self.__services.values():
			if service.is_alive:
				return True
		return False
