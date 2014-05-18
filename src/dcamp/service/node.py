import threading

from zmq import DEALER, SUB, SUBSCRIBE, UNSUBSCRIBE, POLLIN  # pylint: disable-msg=E0611
from zhelpers import zpipe

from dcamp.role.collector import Collector
from dcamp.role.metric import Metric
from dcamp.role.root import Root
from dcamp.service.recovery import MetricSOS, CollectorSOS, RECOVERY_SILENCE_PERIOD_MS
from dcamp.service.service import ServiceMixin
from dcamp.types.messages.control import POLO, CONTROL, SOS
from dcamp.types.messages.topology import TOPO
from dcamp.types.specs import EndpntSpec
from dcamp.util.functions import now_msecs


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
            local_ep,
            local_uuid,
            config_svc,  # must be None
    ):
        ServiceMixin.__init__(self, control_pipe, local_ep, local_uuid, config_svc)
        assert config_svc is None

        self.polo_msg = POLO(self.endpoint, self.uuid)

        ####
        # setup service for polling.

        self.topo_endpoint = self.endpoint.bind_uri(EndpntSpec.BASE)
        self.logger.debug('binding to %s' % self.topo_endpoint)

        # @todo these sockets need a better naming convention.
        self.topo_socket = self.ctx.socket(SUB)
        self.topo_socket.setsockopt_string(SUBSCRIBE, TOPO.marco_key())
        self.topo_socket.bind(self.topo_endpoint)

        self.recovery = None

        self.control_socket = None
        self.control_uuid = None
        self.control_ep = None

        (self.subcnt,  self.reqcnt,  self.repcnt) = (0, 0, 0)

        self.role = None
        self.role_pipe = None
        self.role_thread = None
        self.state = None

        self.level = None
        self.group = None

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

        if self.control_socket is not None:
            self.control_socket.close()
        self.control_socket = None

        if self.role_pipe is not None:
            self.role_pipe.close()
        self.role_pipe = None

        ServiceMixin._cleanup(self)

    def _post_poll(self, items):
        if self.topo_socket in items:
            topo_msg = TOPO.recv(self.topo_socket)
            self.subcnt += 1

            if topo_msg.is_error:
                self.logger.error('topo message error: %s' % topo_msg.errstr)
                return

            if topo_msg.is_recovery:
                self.__handle_recovery(topo_msg)
                return

            if topo_msg.is_marco and self.control_uuid == topo_msg.uuid:
                self.logger.debug('already POLOed this endpoint; ignoring')
                return

            # @todo: add some security here so not just anyone can shutdown the root node
            self.control_uuid = topo_msg.uuid
            self.control_ep = topo_msg.endpoint

            self.control_socket = self.ctx.socket(DEALER)
            self.control_socket.connect(self.control_ep.connect_uri(EndpntSpec.CONTROL))
            self.poller.register(self.control_socket, POLLIN)

            self.polo_msg.send(self.control_socket)
            self.reqcnt += 1

            self.open_state()

        elif self.role_pipe in items:
            message = SOS.recv(self.role_pipe)

            if message.is_error or message.command != 'sos':
                self.logger.error('unexpected message from Role: {}'.format(message))
                return

            if 'branch' == self.level:
                # root died, start election
                self.__handle_recovery(message)
            elif 'leaf' == self.level:
                # collector died, notify root
                self.__handle_sos()
            else:
                raise NotImplementedError('unknown role class: %s' % self.role)

        elif self.control_socket in items:
            assert self.in_open_state
            self.close_state()

            response = CONTROL.recv(self.control_socket)
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
                reply = self.role_pipe.recv_string()
                assert 'OKAY' == reply

                self.logger.debug('received STOP OKAY from %s role' % self.role)

                if 'branch' == self.level:
                    self.topo_socket.setsockopt_string(SUBSCRIBE, TOPO.recovery_key())
                if self.group is not None:
                    self.topo_socket.setsockopt_string(UNSUBSCRIBE, TOPO.group_key(self.group))

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
                self.level = None

                self.logger.debug('node stopped; back to BASE')
                self.set_state(Node.BASE)

            else:
                self.logger.error('unknown control command: %s' % response.command)
                return

    def __handle_recovery(self, msg):

        if 'branch' != self.level:
            self.logger.error('received RECOVERY message but not Collector')
            return

        if self.recovery is not None:
            with self.recovery.lock:
                if self.recovery.is_alive():
                    self.recovery.add_to_queue(msg)
                    return

        self.recovery = CollectorSOS(
            self.ctx,
            self.endpoint,
            self.uuid,
            self.role.get_config_service()
        )

        self.recovery.add_to_queue(msg)
        self.recovery.start()

        # TODO: how to notify Node service of election outcome; use callback within Node service to
        #       shutdown old Collector role and start new Root role

    def __handle_sos(self):
        assert 'leaf' == self.level

        if self.recovery is not None:
            # TODO: move this logic into the recovery class?
            # TODO: use locking to ensure SOSs are not missed
            self.logger.info('already processed SOS: {}'.format(self.recovery.result))

            if self.recovery.is_alive():
                # still working
                self.logger.debug('recovery thread still working; skipping this SOS')
                return

            else:
                assert self.recovery.stop_time is not None
                elapsed = now_msecs() - self.recovery.stop_time

                if self.recovery.result == 'success' and elapsed < RECOVERY_SILENCE_PERIOD_MS:
                    # need to wait longer before trying again
                    self.logger.warn('last successful SOS attempt too recent: {}ms'.format(elapsed))
                    return

        # TODO: use real root ep instead of control_ep; how to get from config service...
        self.recovery = MetricSOS(self.ctx, self.endpoint, self.uuid, self.control_ep)
        self.recovery.start()

    def __handle_assignment(self, response):
        if self.in_play_state:
            # @todo need to handle re-assignment
            self.logger.warning('received re-assignment; ignoring')
            return

        if 'level' not in response.properties:
            self.logger.error('property missing: level')
            return

        level = response['level']

        if level not in ['root', 'branch', 'leaf']:
            self.logger.error('unknown assignment level: %s' % level)
            return

        self.role_pipe, peer = zpipe(self.ctx)

        if 'root' == level:
            assert 'config-file' in response.properties

            self.role = Root(
                peer,
                self.endpoint,
                self.uuid,
                response['config-file'],
            )

        else:
            assert 'parent' in response.properties
            assert 'group' in response.properties

            self.group = response['group']

            self.topo_socket.setsockopt_string(SUBSCRIBE, TOPO.group_key(response['group']))

            if 'branch' == level:
                self.topo_socket.setsockopt_string(SUBSCRIBE, TOPO.recovery_key())

                self.role = Collector(
                    peer,
                    self.endpoint,
                    self.uuid,
                    response['parent'],
                    response['group'],
                )

            else:
                self.role = Metric(
                    peer,
                    self.endpoint,
                    self.uuid,
                    response['parent'],
                    response['group'],
                )

        peer = None  # closed by peer/role
        self.level = level
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
