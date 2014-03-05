import logging, tempfile, sys, psutil, os

from zmq import PUB, SUB, SUBSCRIBE, PUSH, PULL, Again # pylint: disable-msg=E0611
from zmq.devices import ThreadProxy

import dcamp.types.messages.data as DataMsg
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import Service_Mixin
from dcamp.util.functions import now_secs, now_msecs

class Filter(Service_Mixin):

	def __init__(self,
			control_pipe,
			level,
			config_service,
			local_ep,
			parent_ep):
		Service_Mixin.__init__(self, control_pipe)
		assert level in ['root', 'branch', 'leaf']
		assert isinstance(parent_ep, (EndpntSpec, type(None)))
		assert isinstance(local_ep, EndpntSpec)

		self.level = level
		self.config_service = config_service
		self.parent = parent_ep
		self.endpoint = local_ep

		# { config-name: (metric-spec, cached-list }
		self.metric_specs = {}
		self.metric_seqid = -1

		(self.pull_cnt, self.pubs_cnt, self.hugz_cnt) = (0, 0, 0)

		# pull metrics on this socket; all levels will pull (either internally from the
		# sensor service or externally from child filter service's)
		self.pull_socket = self.ctx.socket(PULL)
		self.pull_socket.bind(self.endpoint.bind_uri(EndpntSpec.DATA_INTERNAL, 'inproc'))
		self.poller.register(self.pull_socket)

		os.makedirs('./logs/', exist_ok=True)
		self.data_file = tempfile.NamedTemporaryFile(mode='w', delete=False,
				prefix='{}-{}.'.format(self.level, self.endpoint), suffix='.dcamp-data', dir='./logs/')
		self.logger.debug('writing data to %s' % self.data_file.name)

		# pub metrics on this sockets; only non-root level nodes will pub (to the parent)
		self.pubs_socket = None
		if self.level in ['branch', 'leaf']:
			self.pubs_socket = self.ctx.socket(PUB)
			self.pubs_socket.connect(self.parent.connect_uri(EndpntSpec.DATA_EXTERNAL))

		# sub metrics from child(ren) and push them to pull socket; only non-leaf levels
		# will sub->push
		self.proxy = None
		if self.level in ['branch', 'root']:
			self.proxy = ThreadProxy(SUB, PUSH)
			self.proxy.setsockopt_in(SUBSCRIBE, b'')
			self.proxy.bind_in(self.endpoint.bind_uri(EndpntSpec.DATA_EXTERNAL))
			self.proxy.connect_out(self.endpoint.connect_uri(EndpntSpec.DATA_INTERNAL, 'inproc'))
			self.proxy.start()

		self.hug_int = 5 # units: seconds
		self.next_hug = now_secs() + self.hug_int # units: seconds
		self.last_pub = now_secs() # units: seconds

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d pulls; %d pubs; %d hugz; metrics= [\n%s]" %
				(self.pull_cnt, self.pubs_cnt, self.hugz_cnt, self.metric_specs))

		self.data_file.close()
		self.pull_socket.close()

		if self.level in ['branch', 'leaf']:
			self.pubs_socket.close()

		if self.level in ['branch', 'root']:
			assert self.proxy is not None
			self.proxy.join()

		del self.pull_socket, self.pubs_socket, self.proxy

		Service_Mixin._cleanup(self)

	def _pre_poll(self):
		self.__check_config_for_metric_updates()

		if self.level in ['branch', 'leaf']:
			if self.next_hug <= now_secs():
				self.__send_hug()

			self.poller_timer = self.__get_next_wakeup()

	def __send_hug(self):
		assert(self.level in ['branch', 'leaf'])

		hug = DataMsg.DATA_HUGZ(self.endpoint)
		hug.send(self.pubs_socket)
		self.hugz_cnt += 1
		self.last_pub = now_secs()

	def _post_poll(self, items):
		if self.pull_socket in items:
			while True:
				data = None

				# read all messages on socket, i.e. keep reading until there is nothing
				# left to process
				try: data = DataMsg._DATA.recv(self.pull_socket)
				except Again as e: break
				self.pull_cnt += 1

				if data.is_error:
					self.logger.error('received error message: %s' % data)
					continue

				self.data_file.write(data.log_str() + '\n')

				if data.is_hugz:
					# noted. moving on...
					self.logger.debug('received hug.')
					continue

				# process message (i.e. do the filtering) and then forward to parent
				if self.level in ['branch', 'leaf']:
					# if unknown metric, just drop it
					if data.config_seqid != self.metric_seqid:
						self.logger.warn('unknown config seq-id (%d); dropping data'
								% data.config_seqid)
						continue

					# lookup metric spec, default is None and an empty cache list
					(metric, cache) = self.metric_specs.get(data.config_name, (None, []))

					if metric is None:
						self.logger.warn('unknown metric config-name (%s); dropping data'
								% data.config_name)
						continue

					cache.append(data)
					self.logger.debug('cache size: %d' % len(cache))
					self.__filter_and_send(metric, cache)

	def __filter_and_send(self, metric, cache):
		assert(self.level in ['branch', 'leaf'])
		do_send = True
		saved = cache[-1] # save most recent message

		if metric.threshold is not None:
			value = None
			if metric.threshold.is_timed:
				# add to local cache and run time check with first message
				assert '*' == metric.threshold.op, 'only "hold" operation supported'
				value = cache[0].time # time-based threshold compares against the earliest time

			elif metric.threshold.is_limit:
				# if first collection, just add to cache and skip further processing
				if len(cache) == 1:
					return

				assert len(cache) == 2
				value = cache[0].calculate(cache[1])

			# check threshold
			assert value is not None
			if not metric.threshold.check(value):
				self.logger.debug('%s failed filter (%s): %.2f' % (metric.config_name,
						metric.threshold, value))
				do_send = False

		if do_send:
			# forward message(s) to parent
			for message in cache:
				message.send(self.pubs_socket)
				self.pubs_cnt += 1

			self.last_pub = now_secs()

			# clear cache since we just sent all the messages
			cache.clear()

		# limit-based thresholds need the previous data message for calculations
		if metric.threshold is None or metric.threshold.is_limit:
			cache.clear()
			cache.append(saved)

	def __check_config_for_metric_updates(self):
		(specs, seq) = self.config_service.get_metric_specs()
		if seq > self.metric_seqid:
			# TODO: instead of clearing the list, try to keep old specs that have not
			#       changed?
			self.metric_specs = {}
			for s in specs:
				self.metric_specs[s.config_name] = (s, [])
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
		assert(self.level in ['branch', 'leaf'])

		# reset hugz given that we just sent a message (maybe)
		self.next_hug = self.last_pub + self.hug_int

		# next_hug is in secs; subtract current msecs to get next wakeup epoch
		val = max(0, (self.next_hug * 1e3) - now_msecs())
		self.logger.debug('next wakeup in %dms' % val)
		return val
