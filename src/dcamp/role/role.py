import logging, threading

class Role(object):
	logger = logging.getLogger('dcamp.role')

	def __init__(self):
		pass

	def play(self):
		# for each service, setup and launch thread with poll()
		for s in self.services:
			s.setup()
			thread = threading.Thread(target=s.poll())
			thread.start()
