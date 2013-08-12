import logging, zmq, random
from time import time

import dcamp.types.messages.configuration as ConfigMsg
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import Service

class Configuration(Service):
	'''
	Configuration Service --
	'''

	# states
	STATE_SYNC = 0
	STATE_GOGO = 1

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

		self.svc_pipe = svc_pipe
		self.level = level
		self.parent = parent_ep
		self.endpoint = local_ep

		# { key : ( value, seq-num ) }
		self.kvdict = {}
		self.kv_seq = -1

		self.state = None
		self.pending_updates = []

		self.update_sub = None
		self.update_pub = None
		self.kvsync_req = None
		self.kvsync_rep = None

		topics = []
		if 'leaf' == self.level:
			assert group is not None
			topics.append('/config/%s' % group)
		elif 'branch' == self.level:
			topics.append('/config')
			topics.append('/topo')

		if self.level in ['branch', 'leaf']:
			assert self.parent is not None
			assert len(topics) > 0

			# 1) subscribe to udpates from parent
			self.update_sub = self.ctx.socket(zmq.SUB)
			for t in topics:
				self.update_sub.setsockopt_string(zmq.SUBSCRIBE, t)
			self.update_sub.connect(self.parent.connect_uri(EndpntSpec.CONFIG_UPDATE))

			self.poller.register(self.update_sub, zmq.POLLIN)

			# 2) request snapshot from parent
			#self.kvsync_req = self.ctx.socket(zmq.DEALER)
			#icanhaz = ConfigMsg.ICANHAZ()
			#icanhaz.send(self.kvsync_req, prefix=[b'']) # DEALER socket needs empty first frame
			self.state = Configuration.STATE_SYNC

			# XXX: just for testing
			self.__setup_outbound()
			#self.poller.register(self.kvsync_req)

		else:
			assert 'root' == self.level
			self.pubnext = time() + 1
			self.__setup_outbound()

	def __setup_outbound(self):
		if self.level in ['branch', 'root']:
			# 3) publish updates to children (bind)
			# XXX: only publish config updates to non-collector children
			self.update_pub = self.ctx.socket(zmq.PUB)
			self.update_pub.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_UPDATE))

			# 4) service snapshot requests to children (bind)
			self.kvsync_rep = self.ctx.socket(zmq.ROUTER)
			self.kvsync_rep.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_SNAPSHOT))

			# XXX: process pending updates
			# for update in self.pending_updates:
			#	__process_update_message()

			self.state = Configuration.STATE_GOGO

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d subs; %d pubs; %d reqs; %d reps" %
				(self.subcnt, self.pubcnt, self.reqcnt, self.repcnt))

		if self.update_sub is not None:
			self.update_sub.close()
		if self.update_pub is not None:
			self.update_pub.close()
		if self.kvsync_req is not None:
			self.kvsync_req.close()
		if self.kvsync_rep is not None:
			self.kvsync_rep.close()
		del self.update_sub, self.update_pub, self.kvsync_req, self.kvsync_rep
		super()._cleanup()

	def _pre_poll(self):
		# send fake updates to random groups, just for testing
		if self.level == 'root':
			if self.pubnext < time():
				self.kv_seq += 1
				pubmsg = ConfigMsg.KVPUB('/config/group%d/test-key' % random.randint(1,3),
						'test-val', self.kv_seq - random.randint(0, 2)) # randomize the seq number
				pubmsg.send(self.update_pub)
				self.pubnext = time() + 5
				self.pubcnt += 1

			self.poller_timer = 1e3 * max(0, self.pubnext - time())

	def _post_poll(self, items):
		if self.update_sub in items:
			self.__recv_update()
		if self.kvsync_req in items:
			self.__recv_snapshot()
		if self.kvsync_rep in items:
			self.__send_snapshot()

	def __recv_snapshot(self, child):
		assert self.state == Configuration.STATE_SYNC
		# XXX: if KTHXBYE, __setup_outbound()
		pass

	def __send_snapshot(self, child):
		assert self.state == Configuration.STATE_GOGO
		pass

	def __recv_update(self):
		update = ConfigMsg.CONFIG.recv(self.update_sub)
		self.subcnt += 1

		if update.is_error:
			self.logger.error('received error message from parent: %s' % update)
			return

		if Configuration.STATE_SYNC == self.state:
			self.pending_updates.append(update)
		elif Configuration.STATE_GOGO == self.state:
			self.__process_update_message(update)
		else:
			raise NotImplementedError('unknown state')

	def __process_update_message(self, update):
		# if not greater than current kv-sequence, skip this one
		if update.sequence <= self.kv_seq:
			self.logger.warn('KVPUB out of sequence (cur=%d, recvd=%d); dropping' % (
					self.kv_seq, update.sequence))
			return

		self.kv_seq = update.sequence
		self.kvdict[update.key] = (update.value, update.sequence)
		if len(update.value) == 0:
			del self.kvdict[update.key]

		if self.update_pub is not None:
			update.send(self.update_pub)
			self.pubcnt += 1
