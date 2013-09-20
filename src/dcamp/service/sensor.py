import logging, zmq, sys
from time import time
from random import randint
from collections import namedtuple as NamedTuple

import dcamp.types.messages.data as DataMsg
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import Service

MetricCollection = NamedTuple('MetricCollection', 'epoch, spec')

# round down to most recent second -- designed for collection scheduling
now_secs = lambda: int(time())
# round up/down to nearest millisecond -- designed for collection records
now_msecs = lambda: int(round(time() * 1000))

class Sensor(Service):

	def __init__(self,
			control_pipe,
			config_service,
			endpoint):
		Service.__init__(self, control_pipe)

		self.config_service = config_service
		self.endpoint = endpoint

		# goal: sort by next collection time
		# [ ( next-collection-epoch-secs, spec ), ... ]
		self.metric_specs = []
		self.metric_seqid = -1

		(self.push_cnt, self.hugz_cnt) = (0, 0)

		# we push metrics on this socket (to filter service)
		self.metrics_socket = self.ctx.socket(zmq.PUSH)
		self.metrics_socket.bind(self.endpoint.bind_uri(EndpntSpec.DATA_PUSH_PULL, 'inproc'))

		self.next_collection = sys.maxsize # units: seconds
		self.hug_int = 5 # units: seconds
		self.next_hug = 0 # units: seconds

	def _cleanup(self):
		# service exiting; return some status info and cleanup
		self.logger.debug("%d pushes + %d hugz\n%s metrics" %
				(self.push_cnt, self.hugz_cnt, self.metric_specs))

		self.metrics_socket.close()
		del self.metrics_socket

		#self.pr.disable()
		#self.pr.dump_stats('sensor.stats')

		super()._cleanup()

	def _pre_poll(self):
		self.__check_config_for_metric_updates()

		now = now_secs()
		if self.next_collection <= now:
			self.__collect_and_push_metrics()
		elif self.next_hug <= now:
			self.__send_hug()

		self.poller_timer = self.__get_next_wakeup()

	def _post_poll(self, items):
		pass

	def __send_hug(self):
		metric = DataMsg.DATA(self.endpoint, 'HUGZ')
		metric.send(self.metrics_socket)
		self.hugz_cnt += 1

	def __collect_and_push_metrics(self):

		collected = []
		while True:
			M = self.metric_specs.pop(0)
			assert M.epoch <= now_secs(), 'next metric is not scheduled for collection'

			metric = DataMsg.DATA(self.endpoint, 'basic', M.spec.metric, time1=now_msecs(), value1=randint(0, 1000))
			metric.send(self.metrics_socket)
			self.push_cnt += 1

			# add to list of collected metrics along with its next collection
			collected.append(MetricCollection(now_secs() + M.spec.rate, M.spec))

			if len(self.metric_specs) == 0:
				# no more work
				break

			if self.metric_specs[0].epoch > now_secs():
				# no more work scheduled
				break

		# add the collected metrics back into our list
		self.metric_specs = sorted(self.metric_specs + collected)
		# set the new collection wakeup
		self.next_collection = self.metric_specs[0].epoch

	def __check_config_for_metric_updates(self):
		# TODO: optimize this to only check the seq-id
		(specs, seq) = self.config_service.get_metric_specs()
		if seq > self.metric_seqid:

			new_specs = []

			# add all old metric specs, continue with its next collection time
			for M in self.metric_specs:
				if M.spec in specs:
					new_specs.append(M)
					specs.remove(M.spec)

			# add all new metric specs, starting collection now
			new_specs = [MetricCollection(0, elem) for elem in specs]

			self.metric_specs = sorted(new_specs)
			self.metric_seqid = seq

			self.logger.debug('new metric specs: %s' % self.metric_specs)

			# reset next collection wakeup with new values
			if len(self.metric_specs) > 0:
				self.next_collection = self.metric_specs[0].epoch
			else:
				self.next_collection = sys.maxsize

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
			wakeup = min(self.next_collection, self.next_hug)

		# wakeup is in secs; subtract current msecs to get next wakeup epoch
		val = max(0, (wakeup * 1e3) - now_msecs())
		self.logger.debug('next wakeup in %dms' % val)
		return val
