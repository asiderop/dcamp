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

		# { config-name: (metric-spec, cached-or-None }
		self.metric_specs = {}
		self.metric_seqid = -1

		(self.pull_cnt, self.pubs_cnt, self.hugz_cnt) = (0, 0, 0)

		# pull metrics on this socket; all levels will pull (either internally from the
		# sensor service or externally from child filter service's)
		self.pull_socket = self.ctx.socket(PULL)
		self.pull_socket.bind(self.endpoint.bind_uri(EndpntSpec.DATA_PUSH_PULL, 'inproc'))
		self.poller.register(self.pull_socket)

		os.makedirs('./logs/', exist_ok=True)
		self.data_file = tempfile.NamedTemporaryFile(mode='w', delete=False,
				prefix='{}.'.format(self.level), suffix='.dcamp-data', dir='./logs/')
		self.logger.debug('writing data to %s' % self.data_file.name)

		# pub metrics on this sockets; only non-root level nodes will pub (to the parent)
		self.pubs_socket = None
		if self.level in ['branch', 'leaf']:
			self.pubs_socket = self.ctx.socket(PUB)
			self.pubs_socket.connect(self.parent.connect_uri(EndpntSpec.DATA_SUB))

		# sub metrics from child(ren) and push them to pull socket; only non-leaf levels
		# will sub->push
		self.proxy = None
		if self.level in ['branch', 'root']:
			self.proxy = ThreadProxy(SUB, PUSH)
			self.proxy.setsockopt_in(SUBSCRIBE, b'')
			self.proxy.bind_in(self.endpoint.bind_uri(EndpntSpec.DATA_SUB))
			self.proxy.connect_out(self.endpoint.connect_uri(EndpntSpec.DATA_PUSH_PULL, 'inproc'))
			self.proxy.start()

		self.next_pub = sys.maxsize # units: seconds
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

		Service_Mixin._cleanup(self)

	def _pre_poll(self):
		self.__check_config_for_metric_updates()

		now = now_secs()
		if self.next_hug <= now:
			self.__send_hug()

		self.poller_timer = self.__get_next_wakeup()

	def __send_hug(self):
		if self.level in ['branch', 'leaf']:
			hug = DataMsg.HUGZ(self.endpoint)
			hug.send(self.pubs_socket)
			self.hugz_cnt += 1

	def _post_poll(self, items):
		if self.pull_socket in items:
			while True:
				data = None

				# read all messages on socket, i.e. keep reading until there is nothing
				# left to process
				try: data = DataMsg._DATA.recv(self.pull_socket)
				except Again as e: break

				self.pull_cnt += 1
				self.data_file.write(data.log_str() + '\n')

				# forward metric to parent
				if self.level in ['branch', 'leaf']:
					# if unknown metric, just drop it
					if data['config-seqid'] != self.metric_seqid:
						self.logger.warn('unknown config seq-id (%d); dropping data'
								% data['config-seq-id'])
						continue

					# lookup metric spec
					(metric, cache) = self.metric_specs.get(data['config-name'], (None, None))

					if metric is None:
						self.logger.warn('unknown metric config-name (%s); dropping data'
								% data['config-name'])
						continue

					# do threshold filtering
					if metric.threshold is not None:
						if metric.threshold.is_timed:
							# add to local cache and get updated message
							if cache is not None:
								assert '+' == metric.threshold.op, 'Issue 49'
								data = cache.accumulate(data)
							self.metric_specs[data['config-name']] = (metric, data)

							# XXX: clear cache when sending?

						# check threshold and drop messages
						if not metric.threshold.check(data):
							self.logger.debug('thresholded by %s : %s'
									% (metric.threshold, data))
							data = None

					# forward message to parent
					if data is not None:
						data.send(self.pubs_socket)
						self.pubs_cnt += 1

				del data

	def __check_config_for_metric_updates(self):
		(specs, seq) = self.config_service.get_metric_specs()
		if seq > self.metric_seqid:
			# TODO: instead of clearing the list, try to keep old specs that have not
			#       changed?
			self.metric_specs = {}
			for s in specs:
				self.metric_specs[s.config_name] = (s, None)
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
		self.next_hug = now_secs() + self.hug_int

		# next_hug is in secs; subtract current msecs to get next wakeup epoch
		val = max(0, (next_hug * 1e3) - now_msecs())
		self.logger.debug('next wakeup in %dms' % val)
		return val
