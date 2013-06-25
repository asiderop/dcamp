import logging, threading
import zmq

from dcamp.util.decorator import Runnable

@Runnable
class Service(threading.Thread):
	logger = logging.getLogger('dcamp.service')

	def __init__(self, pipe):
		super().__init__()
		self.ctx = zmq.Context.instance()
		self.__control_pipe = pipe

		self.poller = zmq.Poller()
		self.poller_timer = None

		self.poller.register(self.__control_pipe, zmq.POLLIN)

	def __send_control(self, message):
		self.__control_pipe.send_string(message)
	def __recv_control(self):
		return self.__control_pipe.recv_string()

	def _cleanup(self):
		# tell role we're done (if we can)
		if not self.is_errored:
			# @todo: this might raise an exception / issue #38
			self.__send_control('STOPPED')
			self.logger.debug('sent STOPPED control reply')

		# shared context; will be term()'ed by caller
		self.__control_pipe.close()
		del self.__control_pipe

	def _pre_poll(self):
		pass
	def _post_poll(self, items):
		raise NotImplemented('subclass must implement _post_poll()')

	def _do_control(self):
		'''
		Process control command on the pipe.
		'''
		msg = self.__recv_control()

		if ('STOP' == msg):
			self.logger.debug('received STOP control command')
			self.stop_state()
		else:
			self.__send_control('WTF')
			self.logger.error('unknown control command: %s' % msg)

	def run(self):
		self.run_state()
		while self.is_running:
			try:
				self._pre_poll()
				items = dict(self.poller.poll(self.poller_timer))
				self._post_poll(items)
				if self.__control_pipe in items:
					self._do_control()

			except zmq.ZMQError as e:
				if e.errno == zmq.ETERM:
					self.logger.debug('received ETERM: %s' % self.__class__)
					self.error_state()
				else:
					raise

		# thread is stopping; cleanup and exit
		return self._cleanup()
