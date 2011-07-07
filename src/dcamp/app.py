'''
@author: Alexander
'''

import logging, time

class App:
	'''
	This is the main dCAMP application, handling configuration file parsing and
	managing the proper services.
	'''
	
	def __init__(self, options):
		from .config import Config
		self.__config = Config(options.input)
		self.__servers = dict()
		logging.basicConfig()
		self.logger = logging.getLogger('dcamp')
		self.__options = options
		
		self.__setup_logging()
		self.__init_servers()
		self.__run()

	def __run(self):
		import sched
		
		scheduler = sched.scheduler(time.time, time.sleep)
		timestamp = 0 # time stamp of the configuration used
		
		# while the base server is running
		while self.__servers.has_key('base') and self.__servers['base'].isAlive():
			# if metrics are configured, we should try to collect them
			if self.__config.ismetrics:
				# if configuration settings have changed, start over
				if not timestamp == self.__config.timestamp: 
					leaf_sample_rate = float(self.__config.sample_rate(level='leaf'))
					leaf_report_rate = float(self.__config.report_rate(level='leaf'))
					root_report_rate = float(self.__config.report_rate(level='root'))
					
					t = time.time()
					
					events = list() # create initial list of events to be scheduled
					if self.__config.isbase:
						events.append((self.__servers['base'].server.sample,
										leaf_sample_rate, t + leaf_sample_rate))
						events.append((self.__servers['base'].server.report,
										leaf_report_rate, t + leaf_report_rate))
					if self.__config.isroot:
						events.append((self.__servers['root'].server.report,
										root_report_rate, t + root_report_rate))
					events.sort(lambda x, y: cmp(x[2], y[2]))
					
					timestamp = self.__config.timestamp

				# schedule and run the next event
				next_event = events.pop(0)
				scheduler.enterabs(next_event[2], 1, next_event[0], ())
				scheduler.run()
				
				# add the event back onto the list
				events.append((next_event[0], next_event[1],
								time.time() + next_event[1]))
				events.sort(lambda x, y: cmp(x[2], y[2]))
				
		# base server is no longer active, so quit
		self.__servers['base'].join()
		self.__servers.pop('base', None)
		
		# check for any running servers and kill them
		if len(self.active_servers()) > 0:
			self.logger.info('some services still active')
			self.stop_server('all')
		self.logger.info('dcamp is now exiting')
	
	def __setup_logging(self):
		LEVELS = {
				'debug': logging.DEBUG,
				'info': logging.INFO,
				'warning': logging.WARNING,
				'error': logging.ERROR,
				'critical': logging.CRITICAL}
		level = LEVELS.get(self.__config.dcamp('loglevel'), logging.NOTSET)
		if (level == logging.NOTSET):
			level = logging.WARNING
		if (self.__options.verbose):
			level = logging.DEBUG
		self.logger.setLevel(level)
		self.logger.debug('set logging level to '+ logging.getLevelName(level))
		
	def __init_servers(self):
		'''
		starts up servers based on the current config
		'''
		from service.basesrvc import BaseThread
		from service.campsrvc import CAMPThread
		from service.rootsrvc import RootThread
		from service.aggrsrvc import AggregateThread
		
		# base service is always running
		if not self.__servers.has_key('base'):
			self.logger.info('starting base service')
			base = BaseThread(self.__config, self)
			base.start()
			self.__servers['base'] = base
		
		# camp server is configured and is not started
		if self.__config.iscamp and not self.__servers.has_key('camp'): 
			self.logger.info('starting camp service')
			camp = CAMPService(self.__config, self)
			camp.start()
			self.__servers['camp'] = camp
		# camp server is not configured
		elif not self.__config.iscamp:
			self.stop_server('camp')
		
		# aggregate server is configured and is not started
		if self.__config.isaggregate and not self.__servers.has_key('aggregate'):
			self.logger.info('starting aggregate service')
			aggr = AggregateThread(self.__config, self)
			aggr.start()
			self.__servers['aggregate'] = aggr
		# aggregate server is not configured
		elif not self.__config.isaggregate:
			self.stop_server('aggregate')
		
		# root server is configured and is not started
		if self.__config.isroot and not self.__servers.has_key('root'):
			self.logger.info('starting root service')
			root = RootThread(self.__config, self)
			root.start()
			self.__servers['root'] = root
			root.server.config_nodes()
		# root server is not configured
		elif not self.__config.isroot:
			self.stop_server('root')
	
	def reconfig(self, config):
		self.__config.reconfig(config)
		self.__setup_logging()
		self.__init_servers()
	
	def deconfig(self):
		self.__config.deconfig()
		self.__setup_logging()
		self.__init_servers()
	
	def stop_server(self, server='all'):
		for k, v in self.__servers.iteritems():
			if k == server or server == 'all':
				self.logger.info('stopping ' + v.getName())
				v.stop()
				v.join()
				self.__servers.pop(k, None)
	stop_service = stop_server
	
	def active_servers(self):
		names = []
		for v in self.__servers.values():
			names.append(v.getName())
		return names
	active_services = active_servers
