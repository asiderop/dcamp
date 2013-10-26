import logging, zmq, tempfile, sys, psutil

import dcamp.types.messages.data as DataMsg
from dcamp.types.specs import EndpntSpec, MetricCollection
from dcamp.service.service import Service
from dcamp.util.functions import now_secs, now_msecs

class Filter(Service):

	def __init__(self,
			control_pipe,
			level,
			config_service,
			local_ep,
			parent_ep):
		Service.__init__(self, control_pipe)
		assert level in ['root', 'branch', 'leaf']
		assert isinstance(parent_ep, (EndpntSpec, type(None)))
		assert isinstance(local_ep, EndpntSpec)

		self.level = level
		self.config_service = config_service
		self.parent = parent_ep
		self.endpoint = local_ep

		# goal: sort by next collection time
		# [ ( next-collection-epoch-secs, spec ), ... ]
		self.metric_specs = []
		self.metric_seqid = -1

		(self.pull_cnt, self.pubs_cnt, self.hugz_cnt) = (0, 0, 0)

		# pull metrics on this socket; all levels will pull (either internally from the
		# sensor service or externally from child filter service's)
		self.pull_socket = self.ctx.socket(zmq.PULL)
		self.pull_socket.bind(self.endpoint.bind_uri(EndpntSpec.DATA_PUSH_PULL, 'inproc'))
		self.poller.register(self.pull_socket)

		self.data_file = tempfile.NamedTemporaryFile(mode='w', delete=False,
				prefix='{}.'.format(self.level), suffix='.dcamp-data', dir='.')
		self.logger.debug('writing data to %s' % self.data_file.name)

		# pub metrics on this sockets; only non-root level nodes will pub (to the parent)
		self.pubs_socket = None
		if self.level in ['branch', 'leaf']:
			self.pubs_socket = self.ctx.socket(zmq.PUB)
			self.pubs_socket.connect(self.parent.connect_uri(EndpntSpec.DATA_SUB))

		# sub metrics from child(ren) and push them to pull socket; only non-leaf levels
		# will sub->push
		self.proxy = None
		if self.level in ['branch', 'root']:
			self.proxy = zmq.devices.ThreadProxy(zmq.SUB, zmq.PUSH)
			self.proxy.setsockopt_in(zmq.SUBSCRIBE, b'')
			self.proxy.bind_in(self.endpoint.bind_uri(EndpntSpec.DATA_SUB))
			self.proxy.connect_out(self.endpoint.connect_uri(EndpntSpec.DATA_PUSH_PULL, 'inproc'))
			self.proxy.start()

		self.next_push = sys.maxsize # units: seconds
		self.hug_int = 5 # units: seconds
		self.next_hug = 0 # units: seconds

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d pulls; %d pubs; %d hugz; %s metrics" %
				(self.pull_cnt, self.pubs_cnt, self.hugz_cnt, self.metric_specs))

		self.data_file.close()
		self.pull_socket.close()

		if self.level in ['branch', 'leaf']:
			self.pubs_socket.close()

		if self.level in ['branch', 'root']:
			assert self.proxy is not None
			self.proxy.join()

		del self.pull_socket, self.pubs_socket, self.proxy

		super()._cleanup()

	def _pre_poll(self):
		self.__check_config_for_metric_updates()

		now = now_secs()
		if self.next_push <= now:
			self.__push_metrics()
		elif self.next_hug <= now:
			self.__send_hug()

		self.poller_timer = self.__get_next_wakeup()

	def __send_hug(self):
		if self.level in ['branch', 'leaf']:
			metric = DataMsg.DATA(self.endpoint, 'HUGZ', time1=now_msecs())
			metric.send(self.pubs_socket)
			self.hugz_cnt += 1

	def _post_poll(self, items):
		if self.pull_socket in items:
			while True:
				data = None

				# read all messages on socket, i.e. keep reading until there is nothing
				# left to process
				try: data = DataMsg.DATA.recv(self.pull_socket)
				except zmq.Again as e: break

				self.pull_cnt += 1
				self.data_file.write(data.log_str() + '\n')

				# forward metric to parent
				if self.level in ['branch', 'leaf']:
					'''
					XXX: need full metric spec to do filtering; how do we share metric
						 specs across Sensor/Filter service such that the data messages
						 can be mapped back to the metric spec? The Configuration service
						 is shared...perhaps use the seq-id as a unique identifier?

					if check_threshold()
					    send-data()
					def check_threshold(t, value):
					    if t is None:
					        return True
					    elif t.limit is not None:
					        return t.limit.op(t.limit.value, value)
					    else:
					        assert t.time is not None
					        save-value(t, value)
					'''
					data.send(self.pubs_socket)
					self.pubs_cnt += 1

			del data

	def __push_metrics(self):

		collected = []
		while True:
			collection = self.metric_specs.pop(0)
			assert collection.epoch <= now_secs(), 'next metric is not scheduled for collection'

			(msg, collection) = self.__do_sample(collection)
			if msg is not None:
				msg.send(self.metrics_socket)
				self.push_cnt += 1
			collected.append(collection)

			if len(self.metric_specs) == 0:
				# no more work
				break

			if self.metric_specs[0].epoch > now_secs():
				# no more work scheduled
				break

		# add the collected metrics back into our list
		self.metric_specs = sorted(self.metric_specs + collected)
		# set the new collection wakeup
		self.next_push = self.metric_specs[0].epoch

	def __check_config_for_metric_updates(self):
		(specs, seq) = self.config_service.get_metric_specs()
		if seq > self.metric_seqid:
			self.metric_specs = specs[:] # copy spec list
			self.metric_seqid = seq
			self.logger.debug('new metric specs: %s' % self.metric_specs)
			# XXX: trigger new metric setup

	def __get_next_wakeup(self):
		'''
		Method calculates the next wake-up time, using HUGZ if the next metric collection
		is farther away then the hug interval.

		@returns next wakeup time
		@pre should only be called immediately after sending a message
		'''

		# just sent a message, so reset hugz
		wakeup = self.next_hug = now_secs() + self.hug_int
		if len(self.metric_specs) > 0:
			wakeup = min(self.next_push, self.next_hug)

		# wakeup is in secs; subtract current msecs to get next wakeup epoch
		val = max(0, (wakeup * 1e3) - now_msecs())
		self.logger.debug('next wakeup in %dms' % val)
		return val
