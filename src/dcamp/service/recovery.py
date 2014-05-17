from logging import getLogger
from threading import RLock, Thread

from zmq import ROUTER, DEALER, PUB, POLLIN, ZMQError, Poller, Again  # pylint: disable-msg=E0611

from dcamp.types.messages.control import CONTROL, SOS
from dcamp.util.functions import now_msecs
from dcamp.types.specs import EndpntSpec

RECOVERY_SILENCE_PERIOD_MS = 60 * 1000  # wait a full minute before retrying recovery activity
RECOVERY_ELECTION_WAIT_MS = 15 * 1000  # wait fifteen seconds before declaring new leader


class Election(object):
    def __init__(self, uuid, initiating_node):
        self.uuid = uuid

        self.initter = initiating_node
        self.iwinner = None

        self.result = 'pending'
        self.init_time = now_msecs()
        self.iwin_time = None


class RecoveryThread(Thread):
    def __init__(self, ctx, ep, uuid):
        Thread.__init__(self)
        self.ctx = ctx
        self.ep = ep
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

        msg = SOS(self.ep, self.uuid)
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

        self.cur_elections = []
        self.won_elections = []
        self.last_message_time = None

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

        # we send election PUBs on this socket
        self.pub = self.ctx.socket(PUB)
        self.pub.set_hwm(1)  # don't hold onto more than 1 pub

        for c in self.cfgsvc.topo_get_all_collectors():
            if c.endpoint == self.ep:
                continue  # don't add ourself
            try:
                self.pub.connect(c.endpoint.connect_uri(EndpntSpec.BASE))
            except ZMQError as e:
                self.logger.error('unable to connect to endpoint {}: {}'.format(c.endpoint, e))

    def _run(self):
        self.logger.error('EEEEEEKK!!! root node died... starting an election...')

        # create sockets in method called by recovery Thread instead of contructor which is called
        # by Node service thread. this avoids 0mq termination issues as described here:
        # http://zeromq.org/whitepapers:0mq-termination
        self.__init_sockets()

        # the below loop may raise exceptions during poll() and recv(), but the parent class will
        # catch these and then call _cleanup()

        self.last_message_time = now_msecs()  # just to seed the loop
        while RECOVERY_ELECTION_WAIT_MS > (now_msecs() - self.last_message_time):
            # 1) process all messages on queue
            while True:
                try:
                    msg = self._get_from_queue()
                    self.__process_message(msg)
                except Again:
                    break

            # 2) poll (timeout after 1s)
            wakeup = now_msecs() + 1000
            items = dict(self.poller.poll(wakeup))

            # 3) process all messages on socket
            if self.control_in in items:
                while True:
                    try:
                        msg = CONTROL.recv(self.control_in)
                        self.__process_message(msg)
                    except Again:
                        break

        # TODO: if loop exited, time must have expired; check elections and find winner

        return 'fake'

    def __process_message(self, msg):

        # TODO: handle WTF?

        self.last_message_time = now_msecs()

        # sos is a local detection of the root failure
        if msg == 'SOS':
            # TODO: check for active election or start election?
            pass

        # else, msg is CONTROL type
        # TODO: respond to or record election status

    def __start_election(self):
        pass

    def _cleanup(self):
        # TODO
        self.control_out = None
        self.control_in = None
        self.pub = None
