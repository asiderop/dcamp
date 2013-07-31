import logging

from dcamp.service.service import Service

class Configuration(Service):
	'''
	Configuration Service --

	'''

	def __init__(self,
			control_pipe, # control pipe for shutting down service
			svc_pipe,	# service pipe for telling other local services what to do
			action,
			parent_ep,	# from where we receive config updates/snapshots
			local_ep,	# this is us
			):
		Service.__init__(self, control_pipe)
		assert action in ['root', 'branch', 'leaf']

		self.svc_pipe = svc_pipe
		self.action = action
		self.parent = parent_ep
		self.endpoint = local_ep

		# XXX: need basic config service--subscribe to updates and print to debug log

		# 1) subscribe to udpates from parent
		# 2) request snapshot from parent

		if self.action == 'root':
			# close sub/req sockets; bind rep socket
			pass

		# XXX: need basic config service--publish fake config updates

		# 3) publish updates to children (bind)
		# 4) service snapshot requests to children (bind)
