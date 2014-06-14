from logging import getLogger
from threading import Thread
from uuid import UUID

from zmq import Context, Poller, POLLIN, ZMQError, ETERM  # pylint: disable-msg=E0611

from dcamp.types.specs import EndpntSpec
from dcamp.util.decorators import runnable


@runnable
class ServiceMixin(Thread):
    def __init__(self, pipe, local_ep, local_uuid, config_svc):
        Thread.__init__(self, name='dcamp.service.{}'.format(self))
        self.ctx = Context.instance()
        self.__control_pipe = pipe

        assert isinstance(local_ep, EndpntSpec)
        self.__endpoint = local_ep

        assert isinstance(local_uuid, UUID)
        self.__uuid = local_uuid

        self.__cfgsvc = config_svc

        self.logger = getLogger('dcamp.service.%s' % self)

        self.poller = Poller()
        self.poller_timer = None

        self.poller.register(self.__control_pipe, POLLIN)

    def __str__(self):
        return self.__class__.__name__

    @property
    def cfgsvc(self):
        return self.__cfgsvc

    @property
    def endpoint(self):
        return self.__endpoint

    @property
    def uuid(self):
        return self.__uuid

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

        # wait for configuration service to init
        if self.__cfgsvc is not None:
            self.__cfgsvc.wait_for_gogo()

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
