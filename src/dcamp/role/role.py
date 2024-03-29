from logging import getLogger
from time import sleep
from uuid import UUID

from zmq import Context, Poller, POLLIN, ZMQError, ETERM  # pylint: disable-msg=E0611
from zhelpers import zpipe

from dcamp.service.configuration import Configuration
from dcamp.types.messages.control import SOS
from dcamp.types.specs import EndpntSpec
from dcamp.util.decorators import runnable


@runnable
class RoleMixin(object):
    def __init__(
            self,
            pipe,
            ep,
            uuid,
    ):
        self.ctx = Context.instance()
        self.__control_pipe = pipe

        assert isinstance(ep, EndpntSpec)
        self.__endpoint = ep

        assert isinstance(uuid, UUID)
        self.__uuid = uuid

        self.__config_service = None

        self.logger = getLogger('dcamp.role.%s' % self)

        # { pipe: service, ...}
        self.__services = {}

    def __str__(self):
        return self.__class__.__name__

    def __send_control_str(self, message):
        self.__control_pipe.send_string(message)

    def __recv_control(self):
        return self.__control_pipe.recv_string()

    def get_config_service(self):
        return self.__config_service

    def get_config_service_kvdict(self):
        assert self.__config_service is not None
        return self.__config_service.copy_kvdict()

    def _add_service(self, cls, *args, **kwargs):
        pipe, peer = zpipe(self.ctx)  # create control socket pair
        # create service, passing local values along with rest of given args
        service = cls(peer, self.__endpoint, self.__uuid, self.__config_service, *args, **kwargs)
        self.__services[pipe] = service  # add to our dict, using pipe socket as key
        if Configuration == cls:
            self.__config_service = service

    def sos(self):
        SOS(self.__endpoint, self.__uuid).send(self.__control_pipe)

    def play(self):
        # start each service thread
        for service in self.__services.values():
            service.start()

        # @todo: wait for READY message from each service / issue #37

        self.run_state()
        self.logger.debug('waiting for control commands')

        # listen for control commands from caller
        while self.in_running_state:
            try:
                msg = self.__recv_control()

                if 'STOP' == msg:
                    self.__send_control_str('OKAY')
                    self.logger.debug('received STOP control command')
                    self.stop_state()
                    break
                else:
                    self.__send_control_str('WTF')
                    self.logger.error('unknown control command: %s' % msg)

            except ZMQError as e:
                if e.errno == ETERM:
                    self.logger.debug('received ETERM')
                    self.error_state()
                    break
                else:
                    raise
            except KeyboardInterrupt:  # only for roles played by dcamp.App
                self.logger.debug('received KeyboardInterrupt')
                self.stop_state()
                break

        # role is exiting; cleanup
        return self.__cleanup()

    def __cleanup(self):
        # stop our services cleanly (if we can)
        if not self.in_errored_state:
            # @todo: this might raise an exception / issue #38
            self.__stop()

        # shared context; will be term()'ed by caller

        # close all service sockets
        for pipe in self.__services:
            pipe.close()
        del self.__services

        # close our own control pipe
        self.__control_pipe.close()
        del self.__control_pipe

        self.logger.debug('role cleanup finished; exiting')

    def __stop(self):
        """ try to stop all of this Role's services """

        # send commands
        poller = Poller()
        for (pipe, svc) in self.__services.items():
            pipe.send_string('STOP')
            self.logger.debug('sent STOP command to %s service' % svc)
            poller.register(pipe, POLLIN)

        # give services a few seconds to cleanup and exit before checking responses
        sleep(1)

        max_attempts = len(self.__services)
        attempts = 0

        while self.__some_alive() and attempts < max_attempts:
            attempts += 1

            # poll for any replies
            items = dict(poller.poll(60000))  # wait for messages

            # mark responding services as stopped
            alive = dict(self.__services)  # make copy
            for (pipe, svc) in alive.items():
                if pipe in items:
                    reply = pipe.recv_string()
                    if 'STOPPED' == reply:
                        self.logger.debug('received STOPPED control reply from %s service' % svc)
                        svc.join(timeout=5)  # STOPPED response should be sent right before svc exit
                        if svc.is_alive():
                            self.logger.error('%s service is still alive; not waiting' % svc)
                        else:
                            self.logger.debug('%s service thread stopped' % svc)
                        poller.unregister(pipe)
                        pipe.close()
                        del (self.__services[pipe])
                    else:
                        self.logger.debug('unknown control reply: %s' % reply)

            # log some useful info
            if len(self.__services) > 0:
                msg = '%s services still alive after %d cycles; ' % (
                    [str(s) for s in self.__services.values()], attempts)
                if attempts < max_attempts:
                    msg += 'waiting'
                else:
                    msg += 'giving up'
                self.logger.debug(msg)

    def __some_alive(self):
        """returns True if at least one service of this Role is still running"""
        for service in self.__services.values():
            if service.is_alive():
                return True
        return False
