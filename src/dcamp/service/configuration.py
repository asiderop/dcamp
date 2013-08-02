import logging, zmq
from time import time

import dcamp.data.messages as dcmsg
from dcamp.data.specs import EndpntSpec
from dcamp.service.service import Service

class Configuration(Service):
	'''
	Configuration Service --
	'''

	def __init__(self,
			control_pipe, # control pipe for shutting down service
			svc_pipe,	# service pipe for telling other local services what to do
			level,
			group,
			parent_ep,	# from where we receive config updates/snapshots
			local_ep,	# this is us
			):
		Service.__init__(self, control_pipe)
		assert level in ['root', 'branch', 'leaf']
		assert isinstance(parent_ep, (EndpntSpec, type(None)))
		assert isinstance(local_ep, EndpntSpec)

		(self.subcnt, self.pubcnt, self.reqcnt, self.repcnt) = (0, 0, 0, 0)

		self.pubnext = time()
		self.pubint = 3

		self.svc_pipe = svc_pipe
		self.level = level
		self.group = group
		self.parent = parent_ep
		self.endpoint = local_ep

		self.update_sub = None
		self.update_pub = None

		# XXX: need basic config service--subscribe to updates and print to debug log
		if self.level in ['branch', 'leaf']:
			assert self.parent is not None
			assert self.group is not None

			# 1) subscribe to udpates from parent
			self.update_sub = self.ctx.socket(zmq.SUB)
			#self.update_sub.setsockopt_string(zmq.SUBSCRIBE, '/config/'+group)
			self.update_sub.setsockopt_string(zmq.SUBSCRIBE, '')
			self.update_sub.connect(self.parent.connect_uri(EndpntSpec.CONFIG_UPDATE))

			self.poller.register(self.update_sub, zmq.POLLIN)

			# 2) request snapshot from parent
			pass

		# XXX: need basic config service--publish fake config updates
		if self.level in ['root', 'branch']:
			# 3) publish updates to children (bind)
			self.update_pub = self.ctx.socket(zmq.PUB)
			self.update_pub.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_UPDATE))

			# 4) service snapshot requests to children (bind)
			pass

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d subs; %d pubs; %d reqs; %d reps" %
				(self.subcnt, self.pubcnt, self.reqcnt, self.repcnt))

		if self.update_sub is not None:
			self.update_sub.close()
		if self.update_pub is not None:
			self.update_pub.close()
		del self.update_sub, self.update_pub
		super()._cleanup()

	def _pre_poll(self):
		if 'root' == self.level:
			if self.pubnext < time():
				new_update = dcmsg.KVPUB('foobar', 'haha')
				new_update.send(self.update_pub)
				self.pubnext = time() + self.pubint
				self.pubcnt += 1

			self.poller_timer = 1e3 * max(0, self.pubnext - time())

	def _post_poll(self, items):
		if self.update_sub in items:
			update = dcmsg.DCMsg.recv(self.update_sub)
			self.subcnt += 1
			assert update.name == 'CONFIG'

			self.logger.debug('received config update')

			if self.update_pub is not None:
				update.send(self.update_pub)
				self.pubcnt += 1

