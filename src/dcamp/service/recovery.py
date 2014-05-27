from logging import getLogger
from threading import RLock, Thread
from time import sleep
from uuid import UUID

from zmq import ROUTER, DEALER, PUB, POLLIN, ZMQError, Poller, Again  # pylint: disable-msg=E0611

from dcamp.types.messages.control import CONTROL, SOS, YO
from dcamp.types.messages.topology import RECOVERY, gen_uuid, TOPO
from dcamp.types.specs import EndpntSpec
from dcamp.util.functions import now_msecs, isInstance_orNone

RECOVERY_SILENCE_PERIOD_MS = 60 * 1000  # wait a full minute before retrying recovery activity
RECOVERY_ELECTION_WAIT_MS = 15 * 1000  # wait fifteen seconds before confirming new leader
RECOVERY_IWIN_WAIT_MS = 5 * 1000  # wait five seconds before declaring victory


class Election(object):
    def __init__(self, node_ep, node_uuid, election_uuid=None):

        assert isinstance(node_ep, EndpntSpec)
        self.node_ep = node_ep

        assert isinstance(node_uuid, UUID)
        self.node_uuid = node_uuid

        assert isInstance_orNone(node_uuid, UUID)
        if election_uuid is None:
            # creating new election
            self.elec_uuid = gen_uuid()
            self.wutup_time = None
            self.result = None
        else:
            # tracking existing election from WUTUP
            self.elec_uuid = election_uuid
            self.wutup_time = now_msecs()
            self.result = 'pending'

        self.iwin_time = None
        self.iwin_ep = None
        self.iwin_uuid = None

    def __str__(self):
        return 'Election({}, result={}, start={}, end={}, winner={}:{})'.format(
            self.elec_uuid,
            self.result,
            self.wutup_time,
            self.iwin_time,
            self.iwin_ep,
            self.iwin_uuid,
        )

    def __hash__(self):
        return self.elec_uuid

    def __eq__(self, other):
        return isinstance(other, Election) and self.elec_uuid == other.elec_uuid

    def wutup(self, from_ep, from_uuid):
        assert from_uuid == self.node_uuid
        self.wutup_time = now_msecs()
        self.result = 'pending'
        return RECOVERY('wutup', from_ep, from_uuid, content=str(self.elec_uuid))

    def iwin(self, from_ep, from_uuid):
        assert from_uuid >= self.node_uuid
        self.iwin_time = now_msecs()
        self.iwin_ep = from_ep
        self.iwin_uuid = from_uuid
        self.result = 'won'
        return RECOVERY('iwin', from_ep, from_uuid, content=str(self.elec_uuid))

    def yo(self, from_ep, from_uuid):
        assert from_uuid > self.node_uuid
        self.result = 'lost'
        return YO(from_ep, from_uuid, self.elec_uuid)


class RecoveryThread(Thread):
    def __init__(self, ctx, ep, uuid):
        Thread.__init__(self)
        self.ctx = ctx
        self.endpoint = ep
        self.uuid = uuid

        self.result = 'pending'
        self.start_time = None
        self.stop_time = None

        self.lock = RLock()
        self.__msg_queue = []

        self.logger = getLogger('dcamp.service.node.Recovery')

    def add_to_queue(self, msg):
        with self.lock:
            self.__msg_queue.append(msg)

    def _get_from_queue(self):
        with self.lock:
            if len(self.__msg_queue) > 0:
                return self.__msg_queue.pop(0)
            raise Again

    def run(self):
        with self.lock:
            self.start_time = now_msecs()

        try:
            self.result = self._run()
        except ZMQError:
            # nothing to do with exceptions; just catch and continue to the _cleanup() call
            pass
        self._cleanup()

        with self.lock:
            self.stop_time = now_msecs()

    def _run(self):
        raise NotImplementedError('subclass must implement _run()')

    def _cleanup(self):
        raise NotImplementedError('subclass must implement _cleanup()')


class MetricSOS(RecoveryThread):
    def __init__(self, ctx, ep, uuid, root_ep):
        RecoveryThread.__init__(self, ctx, ep, uuid)

        self.root_ep = root_ep
        self.recovery_socket = None

    def _run(self):
        self.recovery_socket = self.ctx.socket(DEALER)
        self.recovery_socket.connect(self.root_ep.connect_uri(EndpntSpec.CONTROL))

        msg = SOS(self.endpoint, self.uuid)
        msg.send(self.recovery_socket)

        self.logger.error('group collector node died; contacting Root...')

        events = self.recovery_socket.poll(5000)
        if 0 != events:
            response = CONTROL.recv(self.recovery_socket)

            if response.is_error:
                self.logger.error(response)
                return 'failure: {}'.format(response.errstr)

            elif 'keepcalm' == response.command:
                self.logger.debug('root notified; keeping calm')
                return 'success'

            else:
                self.logger.error('unknown command from root: %s' % response.command)
                return 'unknown: {}'.format(response.command)

        else:
            self.logger.warn('root did not respond within time limit; ohmg!')
            return 'failure: no response'

    def _cleanup(self):
        if self.recovery_socket is not None:
            self.recovery_socket.close()
        del self.recovery_socket


class CollectorSOS(RecoveryThread):
    def __init__(self, ctx, ep, uuid, config_svc):
        RecoveryThread.__init__(self, ctx, ep, uuid)

        self.cfgsvc = config_svc

        # three sockets to send and receive election messages; it's awkward, but election PUBs
        # are received on the Node's SUB socket and passed to us via _get_from_queue()
        self.control_out = None
        self.control_in = None
        self.pub = None
        self.poller = Poller()

        # need separate endpoint for recovery activity
        self.recovery_ep = None

        # { election-uuid : Election }
        self.elections = {}
        self.last_message_time = None
        self.elected_leader = None

    def __init_sockets(self):
        # we send election commands (in response to a SUB'ed message) on this socket
        self.control_out = self.ctx.socket(DEALER)

        # we receive election commands (in response to a PUB'ed message) on this socket
        self.control_in = self.ctx.socket(ROUTER)
        bind_addr = self.control_in.bind_to_random_port("tcp://*")
        self.poller.register(self.control_in, POLLIN)

        # subtract CONTROL offset so the port calculated by the remote node matches the
        # random port to which we just bound
        self.recovery_ep = EndpntSpec("localhost", bind_addr - EndpntSpec.CONTROL)
        self.logger.debug('binding recovery socket to {}'.format(self.recovery_ep))

        # we send election PUBs on this socket
        self.pub = self.ctx.socket(PUB)

        for c in self.cfgsvc.topo_get_all_collectors():
            if c.endpoint == self.endpoint:
                self.logger.debug('skipping adding self in election')
                continue  # don't add ourself
            try:
                self.pub.connect(c.endpoint.connect_uri(EndpntSpec.BASE))
                self.logger.debug('connecting to {} for election'.format(c))
                sleep(0.1)  # dumb, but works; give socket time to connect
            except ZMQError as e:
                self.logger.error('unable to connect to endpoint {}: {}'.format(c.endpoint, e))

    def _run(self):
        self.logger.error('EEEEEEKK!!! root node died... starting an election...')

        # create sockets in method called by recovery Thread instead of contructor which is called
        # by Node service thread. this avoids 0mq termination issues as described here:
        # http://zeromq.org/whitepapers:0mq-termination
        self.__init_sockets()

        # we can only exit once a leader has been elected
        while self.elected_leader is None:

            # the below loop may raise exceptions during poll() and recv(), but the parent class
            # will catch these and then call _cleanup()

            self.last_message_time = now_msecs()  # just to seed the loop
            while (now_msecs() - self.last_message_time) < RECOVERY_ELECTION_WAIT_MS:
                # 1) process all messages on queue
                while True:
                    try:
                        msg = self._get_from_queue()
                        self.__process_message(msg)
                    except Again:
                        break

                # 2) poll (timeout after 1s)
                items = dict(self.poller.poll(timeout=1000))

                # 3) process all messages on socket
                if self.control_in in items:
                    while True:
                        try:
                            msg = CONTROL.recv(self.control_in)
                            self.__process_message(msg)
                        except Again:
                            break

                # 4) check if won any self-started elections
                for elect in self.elections.values():
                    if elect.node_uuid != self.uuid:
                        continue
                    if elect.result != 'pending':
                        continue
                    if (now_msecs() - elect.wutup_time) > RECOVERY_IWIN_WAIT_MS:
                        self.last_message_time = now_msecs()
                        elect.iwin(self.endpoint, self.uuid).send(self.pub)

            # if loop exited, time expired; check elections and find winner or start new election
            elected = self.__tally_results()
            if elected is None:
                self.logger.error('timed out waiting for leader; starting new election')
                self.__start_election()
            else:
                self.logger.info('new leader elected: {}'.format(elected))
                self.elected_leader = elected

        return 'success'

    def __tally_results(self):
        # winner is election with most recent stop_time and "won" result
        # TODO: is time enough to determine winner? even if older winner has higher uuid?
        winner = None
        for elect in self.elections.values():
            if elect.result == 'won':
                if winner is None:
                    winner = elect
                else:
                    winner = max(elect, winner, key=lambda e: e.iwin_time)

        return winner

    def __process_message(self, msg):

        # Expected Message Types:
        #     SOS (CONTROL) : local message from Configuration service
        #     WUTUP (TOPO) : remote message from other Collector
        #     YO (CONTROL) : remote message from other Collector (response to WUTUP PUB)
        #     IWIN (TOPO) : remote message from other Collector
        #
        # +   SOS will come from the shared message queue since this is a local message from the
        #     Configuration service.
        # +   WUTUP and IWIN will also come from the shared message queue  since these are received
        #     on the SUB socket owned by the Node service.
        # +   YO will come directly on the control_in socket

        if msg.is_error:
            self.logger.error('received error: {}'.format(msg))
            return

        if isinstance(msg, TOPO):
            if msg.is_recovery:
                if msg.is_wutup:
                    # track existing election

                    self.last_message_time = now_msecs()

                    elect_uuid = UUID(msg.content)
                    elect = Election(msg.endpoint, msg.uuid, elect_uuid)
                    self.elections[elect.elec_uuid] = elect

                    if self.uuid > elect.node_uuid:
                        peer_ep = msg.endpoint.connect_uri(EndpntSpec.CONTROL)
                        self.control_out.connect(peer_ep)
                        self.logger.debug('sending YO response to {}'.format(peer_ep))
                        sleep(0.1)  # dumb, but works; give socket time to connect
                        elect.yo(self.endpoint, self.uuid).send(self.control_out)
                        self.control_out.disconnect(peer_ep)
                    return

                elif msg.is_iwin:
                    # record winner

                    self.last_message_time = now_msecs()

                    elect_uuid = UUID(msg.content)
                    if elect_uuid not in self.elections:
                        self.logger.warn('received IWIN for unknown election')
                        elect = Election(msg.endpoint, msg.uuid, elect_uuid)
                        self.elections[elect_uuid] = elect

                    elect = self.elections[elect_uuid]
                    elect.iwin(msg.endpoint, msg.uuid)

                    if self.uuid > elect.node_uuid:
                        # another node thinks they should be root, but we have higher uuid; here
                        # comes the bully...
                        # TODO: optimize to not start a new election if already winning another;
                        #       re-use logic in #4 of _run() loop
                        self.__start_election()
                    return

        elif isinstance(msg, CONTROL):
            if msg.is_sos:
                # sos is a local detection of the root failure
                assert msg.uuid == self.uuid

                # check for active election or start new election
                if len(self.elections) > 0:
                    return  # if any elections, we're done here

                self.__start_election()
                return

            elif msg.is_yo:
                elect_uuid = UUID(msg.properties['election-uuid'])

                if elect_uuid not in self.elections:
                    self.logger.error('received YO for unknown election; ignoring')
                    return

                elect = self.elections[elect_uuid]

                if elect.node_uuid != self.uuid:
                    self.logger.error('received YO for unowned election; ignoring')
                    return

                self.last_message_time = now_msecs()
                elect.yo(msg.endpoint, msg.uuid)
                return

        self.logger.error('unexpected message: {}'.format(msg))
        return

    def __start_election(self):
        self.last_message_time = now_msecs()

        elect = Election(self.endpoint, self.uuid)
        self.elections[elect.elec_uuid] = elect
        # send recovery endpoint so replies are sent to us instead of Node service
        elect.wutup(self.recovery_ep, self.uuid).send(self.pub)

    def _cleanup(self):
        if self.control_out is not None:
            self.control_out.close()
        del self.control_out

        if self.control_in is not None:
            self.control_in.close()
        del self.control_in

        if self.pub is not None:
            self.pub.close()
        del self.pub

        self.logger.debug(str(self.elections))
