import logging, zmq, random
from time import time, sleep

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

		# { topic : final-seq-num }
		self.kvsync_completed = {}

		self.state = None
		self.pending_updates = []

		self.update_sub = None
		self.update_pub = None
		self.kvsync_req = None
		self.kvsync_rep = None

		self.topics = []
		if 'leaf' == self.level:
			assert group is not None
			self.topics.append('/config/%s' % group)
		elif 'branch' == self.level:
			self.topics.append('/config')
			self.topics.append('/topo')

		if self.level in ['branch', 'leaf']:
			assert self.parent is not None
			assert len(self.topics) > 0

			# 1) subscribe to udpates from parent
			self.update_sub = self.ctx.socket(zmq.SUB)
			for t in self.topics:
				self.update_sub.setsockopt_string(zmq.SUBSCRIBE, t)
			self.update_sub.connect(self.parent.connect_uri(EndpntSpec.CONFIG_UPDATE))

			self.poller.register(self.update_sub, zmq.POLLIN)

			# 2) request snapshot(s) from parent
			self.kvsync_req = self.ctx.socket(zmq.DEALER)
			self.kvsync_req.connect(self.parent.connect_uri(EndpntSpec.CONFIG_SNAPSHOT))
			for t in self.topics:
				icanhaz = ConfigMsg.ICANHAZ(t)
				icanhaz.send(self.kvsync_req)

			self.poller.register(self.kvsync_req, zmq.POLLIN)

			self.state = Configuration.STATE_SYNC

		else:
			assert 'root' == self.level

			# XXX: just for testing
			self.kvdict['/config/group1/endpoints'] = ("localhost:5600, siderop1-losx.local:5630, localhost:5660", 0)
			self.kvdict['/config/group1/filters'] = ("None", 1)
			self.kvdict['/config/group1/metrics'] = ("CPU(rate='5s', threshold='30s')", 2)
			self.kvdict['/config/group2/endpoints'] = ("localhost:5700, localhost:5730, localhost:5760", 3)
			self.kvdict['/config/group2/filters'] = ("-'192.168.1.0/24', +'.netapp.com'", 4)
			self.kvdict['/config/group2/metrics'] = ("CPU(rate='5s', threshold='30s'), DISK(rate='10s', threshold='>5000')", 5)
			self.kvdict['/config/group3/endpoints'] = ("192.168.1.102:5800, 192.168.1.102:5830, 192.168.1.102:5860", 6)
			self.kvdict['/config/group3/filters'] = ("None", 7)
			self.kvdict['/config/group3/metrics'] = ("NETWORK(rate='60s', threshold='None')", 8)
			self.kvdict['/config/root/endpoint'] = ("localhost:5500", 9)
			self.kvdict['/config/root/heartbeat'] = ("5", 10)

			self.kv_seq = 10

			self.pubnext = time() + 1
			self.__setup_outbound()

	def __setup_outbound(self):
		if self.level in ['branch', 'root']:
			# 3) publish updates to children (bind)
			self.update_pub = self.ctx.socket(zmq.PUB)
			self.update_pub.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_UPDATE))

			# 4) service snapshot requests to children (bind)
			self.kvsync_rep = self.ctx.socket(zmq.ROUTER)
			self.kvsync_rep.bind(self.endpoint.bind_uri(EndpntSpec.CONFIG_SNAPSHOT))
			self.poller.register(self.kvsync_rep, zmq.POLLIN)

			# process pending updates
			for update in self.pending_updates:
				self.__process_update_message(update)

			self.state = Configuration.STATE_GOGO

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d subs; %d pubs; %d reqs; %d reps" %
				(self.subcnt, self.pubcnt, self.reqcnt, self.repcnt))

		# print each key-value pair; value is really (value, seq-num)
		sleep(1)
		self.logger.debug('kv-seq: %d' % self.kv_seq)
		for (k, (v, s)) in sorted(self.kvdict.items()):
			self.logger.debug('%s: %s (%d)' % (k, v, s))

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
		# XXX: send fake updates to random groups, just for testing
		if self.level == 'root':
			if self.pubnext < time():
				pubmsg = ConfigMsg.KVPUB('/config/group%d/test-key-%d' % (random.randint(1,3), random.randint(0,30)),
						'test-val', self.kv_seq + 1 + random.randint(0, 0)) # randomize the seq number
				self.__process_update_message(pubmsg) # add to our kvdict and publish update
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

	def __recv_update(self):
		update = ConfigMsg.CONFIG.recv(self.update_sub)
		self.subcnt += 1

		if update.is_error:
			self.logger.error('received error message from parent: %s' % update)
			return

		if Configuration.STATE_SYNC == self.state:
			# TODO: could just not read the message; let them queue up on the socket
			#       itself...
			self.pending_updates.append(update)
		elif Configuration.STATE_GOGO == self.state:
			self.__process_update_message(update)
		else:
			raise NotImplementedError('unknown state')

	def __process_update_message(self, update, ignore_sequence=False):
		# if not greater than current kv-sequence, skip this one
		if not ignore_sequence and update.sequence <= self.kv_seq:
			self.logger.warn('KVPUB out of sequence (cur=%d, recvd=%d); dropping' % (
					self.kv_seq, update.sequence))
			return

		# during kvsync, we allow out of sequence updates; we only set our seq-num when
		# not doing a kvsync
		if not ignore_sequence:
			self.kv_seq = update.sequence

		self.kvdict[update.key] = (update.value, update.sequence)
		if len(update.value) == 0:
			del self.kvdict[update.key]

		# this should be None if still in SYNC state
		if self.update_pub is not None:
			update.send(self.update_pub)
			self.pubcnt += 1

	def __recv_snapshot(self):
		assert Configuration.STATE_SYNC == self.state

		# should either be KVSYNC or KTHXBAI
		response = ConfigMsg.CONFIG.recv(self.kvsync_req)

		if response.is_error:
			self.logger.error(response)
			return

		if ConfigMsg.CONFIG.KTHXBAI == response.ctype:
			if response.value not in self.topics:
				self.logger.error('received KTHXBAI of unexpected subtree: %s' % response.value)
				return

			# add given subtree to completed list; return if still waiting for other
			# subtree kvsync sessions
			self.kvsync_completed[response.value] = response.sequence
			if len(self.kvsync_completed) != len(self.topics):
				return

			self.kv_seq = max(self.kvsync_completed.values())
			self.__setup_outbound()
		else:
			self.__process_update_message(response, ignore_sequence=True)

	def __send_snapshot(self):
		assert Configuration.STATE_GOGO == self.state

		request = ConfigMsg.CONFIG.recv(self.kvsync_rep)

		if request.is_error:
			self.logger.error(request)
			return

		peer_id = request._peer_id
		subtree = request.value # subtree stored as value in ICANHAZ message

		# send all the key-value pairs in our dict
		max_seq = -1
		for (k, (v, s)) in self.kvdict.items():
			# skip keys not in the requested subtree
			if not k.startswith(subtree):
				continue
			max_seq = max([max_seq, s])
			snap = ConfigMsg.KVSYNC(k, v, s, peer_id)
			snap.send(self.kvsync_rep)

		# send final message, closing the kvsync session
		snap = ConfigMsg.KTHXBAI(self.kv_seq, peer_id, subtree)
		snap.send(self.kvsync_rep)
