import logging

from zmq import Context, PUB, REP, ZMQError  # pylint: disable-msg=E0611
from zhelpers import zpipe

import dcamp.types.messages.topology as TopoMsg

from dcamp.types.config_file import DCConfig_Mixin
from dcamp.types.specs import EndpntSpec
from dcamp.role.base import Base


class App:
    """
    This is the main dCAMP application.
    """

    def __init__(self, args):
        self.ctx = Context.instance()

        # set default options for all sockets
        self.ctx.linger = 0

        self.logger = logging.getLogger('dcamp.app')
        self.args = args

    def exec(self):
        """
        if base role command:
            start Base role, erroring if already running
        if root command:
            execute command, erroring if base role not running
        """
        result = 0
        if 'base' == self.args.cmd:
            result = self._exec_base()
        elif 'root' == self.args.cmd:
            result = self._exec_root()

        self.ctx.term()
        exit(result)

    def _exec_root(self):
        config = DCConfig_Mixin()
        config.read_file(self.args.configfile)

        # 1) MARCO "root" base endpoint (multiple times?)
        # 2) if POLO'ed, send assignment CONTROL

        # @todo: this can raise exceptions

        pub = self.ctx.socket(PUB)
        root_ep = config.root['endpoint']
        connect_str = root_ep.connect_uri(EndpntSpec.BASE)
        pub.connect(connect_str)

        rep = self.ctx.socket(REP)
        bind_addr = rep.bind_to_random_port("tcp://*")

        # subtract TOPO_JOIN offset so the port calculated by the remote node matches the
        # random port to which we just bound
        ep = EndpntSpec("localhost", bind_addr - EndpntSpec.TOPO_JOIN)
        self.logger.debug('bound to %s + 1' % str(ep))

        marco = TopoMsg.MARCO(ep, TopoMsg.gen_uuid())
        polo = None
        tries = 0
        while tries < 5:
            tries += 1
            marco.send(pub)
            result = rep.poll(timeout=1000)
            if 0 != result:
                polo = TopoMsg.POLO.recv(rep)
                break

        if None == polo:
            self.logger.error('Unable to contact root address: %s' % str(root_ep))
            self.logger.error('Is the base node running?')
            return -1

        if polo.is_error:
            self.logger.error('Received error message from root address: %s' % polo)
            return -1

        if 'start' == self.args.action:
            repmsg = TopoMsg.ASSIGN(root_ep, 'root', None)
            repmsg['config-file'] = self.args.configfile.name

        elif 'stop' == self.args.action:
            repmsg = TopoMsg.STOP()

        else:
            raise NotImplementedError('unknown root action')

        repmsg.send(rep)

        pub.close()
        rep.close()
        del pub, rep

    def _exec_base(self):
        # pair socket for controlling Role; not used here
        pipe, peer = zpipe(self.ctx)

        try:
            role = Base(peer, self.args.address)
        except ZMQError as e:
            self.logger.debug('exception while starting base role:', exc_info=True)
            self.logger.error('Unable to start base node: %s' % e)
            self.logger.error('Is one already running on the given address?')
            return -1

        # start playing role
        # NOTE: this should only return when exiting
        role.play()

        # cleanup
        pipe.close()
        del pipe, peer
