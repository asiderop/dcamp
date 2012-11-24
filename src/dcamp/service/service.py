import logging, threading
import zmq

from dcamp.data import Runnable

@Runnable
class Service(threading.Thread):
	logger = logging.getLogger('dcamp.service')

	def __init__(self, pipe):
		super().__init__()
		self.ctx = zmq.Context.instance()
		self._pipe = pipe
		#self._pipe.linger = -1

		self.poller = zmq.Poller()
		self.poller_timer = None

		self.poller.register(self._pipe, zmq.POLLIN)

	def _cleanup(self):
		# tell role we're done (if we can)
		if not self.is_errored:
			# @todo: this might raise an exception
			self._pipe.send_string('STOPPED')
			self.logger.debug('sent STOPPED control reply')

		# shared context; will be term()'ed by caller
		self._pipe.close()
		del self._pipe

	def _pre_poll(self):
		pass
	def _post_poll(self, items):
		raise NotImplemented('subclass must implement _post_poll()')

	def _do_control(self):
		'''
		Process control command on the pipe.
		'''
		msg = self._pipe.recv_string()

		if ('STOP' == msg):
			self.logger.debug('received STOP control command')
			self.stop_state()
		else:
			self._pipe.send_string('WTF')
			self.logger.error('unknown control command: %s' % msg)

	def run(self):
		self.run_state()
		while self.is_running:
			try:
				self._pre_poll()
				items = dict(self.poller.poll(self.poller_timer))
				self._post_poll(items)
				if self._pipe in items:
					self._do_control()

			except zmq.ZMQError as e:
				if e.errno == zmq.ETERM:
					self.logger.debug('received ETERM: %s' % self.__class__)
					self.error_state()
				else:
					raise

		# thread is stopping; cleanup and exit
		return self._cleanup()
