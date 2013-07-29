#!/usr/bin/env python3

def Runnable(given_class):
	'''Class decorator turning given class into a "runnable" object'''

	# _state "enum" values
	given_class.INIT = 0
	given_class.RUNNING = 1
	given_class.STOPPED = 2
	given_class.ERRORED = 3

	given_class.STATES = [
		given_class.INIT,
		given_class.RUNNING,
		given_class.STOPPED,
		given_class.ERRORED,
	]

	original_init = given_class.__init__
	def __init__(self, *args, **kwargs):
		self._state = given_class.INIT
		original_init(self, *args, **kwargs)
	given_class.__init__ = __init__

	def set_state(self, state):
		assert state in given_class.STATES
		self._state = state
	given_class.set_state = set_state

	####
	# convenience setter methods

	def stop_state(self):
		self._state = given_class.STOPPED
	given_class.stop_state = stop_state

	def run_state(self):
		self._state = given_class.RUNNING
	given_class.run_state = run_state

	def error_state(self):
		self._state = given_class.ERRORED
	given_class.error_state = error_state

	####
	# convenience getter methods

	@property
	def is_stopped(self):
		return self._state == given_class.STOPPED
	given_class.is_stopped = is_stopped

	@property
	def is_running(self):
		return self._state == given_class.RUNNING
	given_class.is_running = is_running

	@property
	def is_errored(self):
		return self._state == given_class.ERRORED
	given_class.is_errored = is_errored

	return given_class

def Prefixable(given_class):
	'''
	Decorator provides given class with methods to get/pop/push a prefix.

	These methods always ensure a trailing delimiter.
	'''

	original_init = given_class.__init__
	def __init__(self, *args, **kwargs):
		self._prefix = []
		self._delimiter = '/'
		original_init(self, *args, **kwargs)
	given_class.__init__ = __init__

	def _get_prefix(self):
		result = self._delimiter
		for pre in self._prefix:
			result += pre + self._delimiter
		return result
	given_class._get_prefix = _get_prefix

	def _pop_prefix(self):
		# check for sole delimiter
		if len(self._prefix) > 0:
			self._prefix.pop()
		return self._get_prefix()
	given_class._pop_prefix = _pop_prefix

	def _push_prefix(self, pre):
		if self._delimiter in pre:
			self.logger.error('delimiter in pushed prefix')
		if pre.endswith(self._delimiter):
			pre = pre.rstrip(self._delimiter)
		self._prefix.append(pre)
		return self._get_prefix()
	given_class._push_prefix = _push_prefix

	return given_class
