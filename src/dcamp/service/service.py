from logging import getLogger
from threading import Thread

from zmq import Context, Poller, POLLIN, ZMQError, ETERM  # pylint: disable-msg=E0611

from dcamp.util.decorators import runnable


@runnable
class ServiceMixin(Thread):
    def __init__(self, pipe):
        Thread.__init__(self)
        self.ctx = Context.instance()
        self.__control_pipe = pipe

        self.logger = getLogger('dcamp.service.%s' % self)

        self.poller = Poller()
        self.poller_timer = None

        self.poller.register(self.__control_pipe, POLLIN)

    def __str__(self):
        return self.__class__.__name__

    def __send_control(self, message):
        self.__control_pipe.send_string(message)

    def __recv_control(self):
        return self.__control_pipe.recv_string()

    def _cleanup(self):
        # tell role we're done (if we can)
        if not self.in_errored_state:
            # @todo: this might raise an exception / issue #38
            self.__send_control('STOPPED')
            self.logger.debug('sent STOPPED control reply')

        # shared context; will be term()'ed by caller
        self.__control_pipe.close()
        del self.__control_pipe

        self.logger.debug('service cleanup finished; exiting')

    def _pre_poll(self):
        pass

    def _post_poll(self, items):
        raise NotImplementedError('subclass must implement _post_poll()')

    def _do_control(self):
        """
        Process control command on the pipe.
        """
        msg = self.__recv_control()

        if 'STOP' == msg:
            self.logger.debug('received STOP control command')
            self.stop_state()
        else:
            self.__send_control('WTF')
            self.logger.error('unknown control command: %s' % msg)

    def run(self):
        self.run_state()
        while self.in_running_state:
            try:
                self._pre_poll()
                items = dict(self.poller.poll(self.poller_timer))
                self._post_poll(items)
                if self.__control_pipe in items:
                    self._do_control()

            except ZMQError as e:
                if e.errno == ETERM:
                    self.logger.debug('received ETERM: %s' % self.__class__)
                    self.error_state()
                else:
                    raise

        # thread is stopping; cleanup and exit
        return self._cleanup()
