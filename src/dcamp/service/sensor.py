import logging, zmq, sys, psutil
from time import time
from collections import namedtuple as NamedTuple

import dcamp.types.messages.data as DataMsg
from dcamp.types.specs import EndpntSpec
from dcamp.service.service import Service

MetricCollection = NamedTuple('MetricCollection', 'epoch, spec, last_time, last_value')

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
		metric = DataMsg.DATA(self.endpoint, 'HUGZ', time1=now_msecs())
		metric.send(self.metrics_socket)
		self.hugz_cnt += 1

	def __collect_and_push_metrics(self):

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
		self.next_collection = self.metric_specs[0].epoch

	def __check_config_for_metric_updates(self):
		# TODO: optimize this to only check the seq-id
		(specs, seq) = self.config_service.get_metric_specs()
		if seq > self.metric_seqid:

			new_specs = []

			# add all old metric specs, continue with its next collection time
			for collection in self.metric_specs:
				if collection.spec in specs:
					new_specs.append(collection)
					specs.remove(collection.spec)

			# add all new metric specs, starting collection now
			new_specs = [MetricCollection(0, elem, 0, 0) for elem in specs]

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

	def __do_sample(self, collection):
		''' returns tuple of (data-msg, metric-collection) '''
		# TODO: move this to another class?

		(time1, value1, time2, value2) = (None, None, None, None)
		mtype = None

		# local vars for easier access
		detail = collection.spec.detail
		last_t = collection.last_time
		last_v = collection.last_value

		first = (last_t, last_v) == (0, 0)

		if 'CPU' == detail:
			mtype = 'percent'

			time1 = now_msecs()
			# percent is accurate to one decimal point
			value1 = int(psutil.cpu_percent(interval=0) * 10)
			value2 = 1000

			# fake last time/value since they are not needed
			(last_t, last_v) = (1, 1)

		elif 'DISK' == detail:
			mtype = 'rate'

			time1 = last_t
			value1 = last_v

			time2 = now_msecs()
			disk = psutil.disk_io_counters()
			value2 = disk.read_bytes + disk.write_bytes

			last_t = time2
			last_v = value2

		elif 'NETWORK' == detail:
			mtype = 'rate'

			time1 = last_t
			value1 = last_v

			time2 = now_msecs()
			net = psutil.net_io_counters()
			value2 = net.bytes_sent + net.bytes_recv

			last_t = time2
			last_v = value2

		elif 'MEMORY' == detail:
			mtype = 'percent'

			time1 = now_msecs()
			vmem = psutil.virtual_memory()
			value1 = vmem.used
			value2 = vmem.total

			# fake last time/value since they are not needed
			(last_t, last_v) = (1, 1)

		m = None
		if not first:
			m = DataMsg.DATA(self.endpoint, mtype, detail,
					time1=time1, value1=value1,
					time2=time2, value2=value2,
				)

		# create new collection with next collection time
		c = MetricCollection(now_secs() + collection.spec.rate, collection.spec, last_t, last_v)

		return (m, c)
