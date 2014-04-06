import threading

from zmq import DEALER, SUB, SUBSCRIBE, POLLIN  # pylint: disable-msg=E0611
from zhelpers import zpipe

import dcamp.types.messages.topology as topo

from dcamp.role.root import Root
from dcamp.role.collector import Collector
from dcamp.role.metric import Metric

from dcamp.service.service import ServiceMixin
from dcamp.types.specs import EndpntSpec


class Node(ServiceMixin):
    """
    @todo: need to timeout if the req fails / issue #28
    """

    BASE = 0
    BASE_OPEN = 1
    PLAY = 4
    PLAY_OPEN = 5

    STATES = [
        BASE,
        BASE_OPEN,
        PLAY,
        PLAY_OPEN,
    ]

    def __init__(
            self,
            control_pipe,
            config_svc,  # must be None
            local_ep
    ):

        ServiceMixin.__init__(self, control_pipe, config_svc)
        assert config_svc is None

        self.endpoint = local_ep
        self.uuid = topo.gen_uuid()
        self.polo_msg = topo.POLO(self.endpoint, self.uuid)

        ####
        # setup service for polling.

        self.topo_endpoint = self.endpoint.bind_uri(EndpntSpec.BASE)
        self.logger.debug('binding to %s' % self.topo_endpoint)

        # @todo these sockets need a better naming convention.
        self.topo_socket = self.ctx.socket(SUB)
        self.topo_socket.setsockopt_string(SUBSCRIBE, '')

        self.topo_socket.bind(self.topo_endpoint)

        self.control_socket = None
        self.control_uuid = None
        self.control_ep = None

        (self.subcnt,  self.reqcnt,  self.repcnt) = (0, 0, 0)

        self.role = None
        self.role_pipe = None
        self.role_thread = None
        self.state = None

        self.set_state(Node.BASE)

        self.poller.register(self.topo_socket, POLLIN)

    @property
    def in_play_state(self):
        return self.state in (Node.PLAY, Node.PLAY_OPEN)

    @property
    def in_open_state(self):
        return self.state in (Node.BASE_OPEN, Node.PLAY_OPEN)

    def set_state(self, state):
        assert state in Node.STATES
        self.state = state

    def close_state(self):
        assert self.in_open_state

        new_state = None
        if Node.BASE_OPEN == self.state:
            new_state = Node.BASE
        elif Node.PLAY_OPEN == self.state:
            new_state = Node.PLAY

        self.set_state(new_state)

    def open_state(self):
        assert not self.in_open_state

        new_state = None
        if Node.BASE == self.state:
            new_state = Node.BASE_OPEN
        elif Node.PLAY == self.state:
            new_state = Node.PLAY_OPEN

        self.set_state(new_state)

    def _cleanup(self):
        # service exiting; return some status info and cleanup
        self.logger.debug("%d subs; %d reqs; %d reps" %
                          (self.subcnt, self.reqcnt, self.repcnt))

        self.topo_socket.close()
        self.topo_socket = None

        if self.control_socket:
            self.control_socket.close()
        self.control_socket = None

        ServiceMixin._cleanup(self)

    def _post_poll(self, items):
        if self.topo_socket in items:
            marco_msg = topo.MARCO.recv(self.topo_socket)
            self.subcnt += 1

            if marco_msg.is_error:
                self.logger.error('topo message error: %s' % marco_msg.errstr)
                return

            if self.control_uuid == marco_msg.uuid:
                self.logger.debug('already POLOed this endpoint; ignoring')
                return

            # @todo: add some security here so not just anyone can shutdown the root node
            self.control_uuid = marco_msg.uuid
            self.control_ep = marco_msg.endpoint

            self.control_socket = self.ctx.socket(DEALER)
            self.control_socket.connect(self.control_ep.connect_uri(EndpntSpec.CONTROL))
            self.poller.register(self.control_socket, POLLIN)

            self.polo_msg.send(self.control_socket)
            self.reqcnt += 1

            self.open_state()

        elif self.role_pipe in items:
            message = self.role_pipe.recv_string()
            assert 'SOS' == message

            self.__do_sos()

        elif self.control_socket in items:
            assert self.in_open_state
            self.close_state()

            response = topo.CONTROL.recv(self.control_socket)
            self.poller.unregister(self.control_socket)
            self.control_socket.close()
            self.control_socket = None
            self.repcnt += 1

            if response.is_error:
                self.logger.error(response)

            elif 'assignment' == response.command:
                self.__handle_assignment(response)

            elif 'stop' == response.command:
                if not self.in_play_state:
                    self.logger.error('role not running; nothing to stop')
                    return

                self.role_pipe.send_string('STOP')
                response = self.role_pipe.recv_string()
                assert 'OKAY' == response

                self.logger.debug('received STOP OKAY from %s role' % self.role)

                self.role_pipe.close()
                self.poller.unregister(self.role_pipe)
                self.role_pipe = None

                # wait for thread to exit
                self.role_thread.join(timeout=60)
                if self.role_thread.isAlive():
                    self.logger.error('!!! %s role is still alive !!!' % self.role)
                else:
                    self.logger.debug('%s role stopped' % self.role)

                self.role_thread = None
                self.role = None

                self.logger.debug('node stopped; back to BASE')
                self.set_state(Node.BASE)

            else:
                self.logger.error('unknown control command: %s' % response.command)
                return

    def __handle_assignment(self, response):
        if self.in_play_state:
            # @todo need to handle re-assignment
            self.logger.warning('received re-assignment; ignoring')
            return

        if 'level' not in response.properties:
            self.logger.error('property missing: level')
            return

        level = response['level']
        if 'root' == level:
            assert 'config-file' in response.properties
            self.role_pipe, peer = zpipe(self.ctx)
            self.role = Root(
                peer,
                self.endpoint,
                response['config-file'],
            )

        elif 'branch' == level:
            self.role_pipe, peer = zpipe(self.ctx)
            self.role = Collector(
                peer,
                self.endpoint,
                response['parent'],
                response['group'],
            )

        elif 'leaf' == level:
            self.role_pipe, peer = zpipe(self.ctx)
            self.role = Metric(
                peer,
                self.endpoint,
                response['parent'],
                response['group'],
            )

        else:
            self.logger.error('unknown assignment level: %s' % level)
            return

        self.__play_role()

    def __play_role(self):
        # start thread
        assert self.role is not None
        assert self.role_pipe is not None

        self.poller.register(self.role_pipe, POLLIN)

        self.logger.debug('starting Role: %s' % self.role)
        self.role_thread = threading.Thread(target=self.role.play)
        self.role_thread.start()
        self.set_state(Node.PLAY)

    def __do_sos(self):
        """
        handles parent death

        @todo save time of successful sos; throttle future attempts?
        """
        if self.role.__class__ == Metric:
            # collector died, notify Root
            self.logger.error('group collector node died; contacting Root...')

            self.control_socket = self.ctx.socket(DEALER)
            self.control_socket.connect(self.control_ep.connect_uri(EndpntSpec.CONTROL))

            polo = topo.POLO(self.endpoint, self.uuid, content='SOS')
            polo.send(self.control_socket)
            self.reqcnt += 1

            events = self.control_socket.poll(5000)
            if 0 != events:
                response = topo.CONTROL.recv(self.control_socket)
                self.repcnt += 1

                if response.is_error:
                    self.logger.error(response)
                elif 'keepcalm' == response.command:
                    self.logger.debug('root notified; keeping calm')
                else:
                    self.logger.error('unknown command from root: %s' % response.command)

            else:
                self.logger.warn('root did not respond within time limit; ohmg!')

            self.control_socket.close()
            self.control_socket = None

        elif self.role.__class__ == Collector:
            # root died, start election
            self.logger.error('EEEEEEKK!!! root node died... starting an election...')

        else:
            raise NotImplementedError('unknown role class: %s' % self.role.__class__.__name__)
