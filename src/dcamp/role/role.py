from logging import getLogger
from time import sleep

from zmq import Context, Poller, POLLIN, ZMQError, ETERM # pylint: disable-msg=E0611
from zhelpers import zpipe

from dcamp.util.decorators import Runnable

@Runnable
class Role_Mixin(object):

	def __init__(self,
			pipe):
		self.ctx = Context.instance()
		self.__control_pipe = pipe

		self.logger = getLogger('dcamp.role.%s' % self)

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
		return service

	def play(self):
		# start each service thread
		for service in self.__services.values():
			service.start()

		# @todo: wait for READY message from each service / issue #37

		self.run_state()
		self.logger.debug('waiting for control commands')

		# listen for control commands from caller
		while self.in_running_state:
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

			except ZMQError as e:
				if e.errno == ETERM:
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

	def __cleanup(self):
		# stop our services cleanly (if we can)
		if not self.in_errored_state:
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

	def __stop(self):
		'''try to stop all of this Role's services'''

		# send commands
		poller = Poller()
		for (pipe, svc) in self.__services.items():
			pipe.send_string('STOP')
			self.logger.debug('sent STOP command to %s service' % svc)
			poller.register(pipe, POLLIN)

		# give services a few seconds to cleanup and exit before checking responses
		sleep(1)

		max_attempts = len(self.__services)
		attempts = 0

		while self.__some_alive() and attempts < max_attempts:
			attempts += 1

			# poll for any replies
			items = dict(poller.poll(60000)) # wait for messages

			# mark responding services as stopped
			alive = dict(self.__services) # make copy
			for (pipe, svc) in alive.items():
				if pipe in items:
					reply = pipe.recv_string()
					if 'STOPPED' == reply:
						self.logger.debug('received STOPPED control reply from %s service' % svc)
						svc.join(timeout=5) # the STOPPED response should be sent right before the service exits
						if svc.is_alive():
							self.logger.error('%s service is still alive; not waiting' % svc)
						else:
							self.logger.debug('%s service thread stopped' % svc)
						poller.unregister(pipe)
						pipe.close()
						del(self.__services[pipe])
					else:
						self.logger.debug('unknown control reply: %s' % reply)

	def __some_alive(self):
		'''returns True if at least one service of this Role is still running'''
		for service in self.__services.values():
			if service.is_alive():
				return True
		return False
