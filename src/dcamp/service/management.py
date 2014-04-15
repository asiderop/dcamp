from time import time

from zmq import ROUTER, PUB, POLLIN, Again  # pylint: disable-msg=E0611

from dcamp.service.service import ServiceMixin
from dcamp.types.messages.common import WTF
from dcamp.types.messages.topology import gen_uuid, MARCO, CONTROL, POLO, STOP, GROUP
from dcamp.types.specs import EndpntSpec
from dcamp.types.topo import TopoTreeMixin, TopoNode
from dcamp.util.functions import now_secs


class Management(ServiceMixin):
    """
    Management Service -- provides functionality for interacting with and controlling
    dCAMP.

    @todo: how to handle joining nodes not part of config / issue #26
    """

    def __init__(
            self,
            control_pipe,
            config_svc,
            local_ep
    ):

        ServiceMixin.__init__(self, control_pipe, config_svc)
        assert isinstance(local_ep, EndpntSpec)

        self.config_service = config_svc
        self.endpoint = local_ep
        self.uuid = gen_uuid()

        # 1) start tree with self as root
        # 2) add each node to tree as topo-node
        # tree.nodes contains { node-endpoint: topo-node }
        # topo-node contains (endpoint, role, group, parent, children, last-seen)
        # topo keys come from tree.get_topo_key(node)

        self.tree = TopoTreeMixin(self.endpoint, self.uuid)
        self.config_service.root(self.endpoint)
        self.config_service[self.tree.get_topo_key(self.tree.root)] = 0

        # { group: collector-topo-node }
        self.collectors = {}
        # { group: ( set(node-uuid), last-ping-time ) }
        self.sos_pings = {}

        ####
        # setup service for polling.

        # we receive join requests on this socket
        self.join_socket = self.ctx.socket(ROUTER)
        self.join_socket.bind(self.endpoint.bind_uri(EndpntSpec.CONTROL))

        # we receive special control messages on this socket during recovery
        self.control_sock = None

        # we send topo discovery messages on this socket
        self.disc_socket = self.ctx.socket(PUB)
        self.disc_socket.set_hwm(1)  # don't hold onto more than 1 pub

        for group in self.config_service.get_groups():
            for ep in self.config_service.get_endpoints(group):
                if ep == local_ep:
                    continue  # don't add ourself
                self.disc_socket.connect(ep.connect_uri(EndpntSpec.BASE))

        self.reqcnt = 0
        self.repcnt = 0

        self.pubint = self.config_service.hb_int
        self.pubcnt = 0

        self.marco_msg = MARCO(self.endpoint, self.uuid)
        self.pubnext = time()

        self.poller.register(self.join_socket, POLLIN)

    def _cleanup(self):
        if not self.in_errored_state:
            self.__stop_all_nodes()

        # service exiting; return some status info and cleanup
        self.logger.debug("%d pubs; %d reqs; %d reps" %
                          (self.pubcnt, self.reqcnt, self.repcnt))

        self.tree.print()
        for (key, node) in self.tree.walk():
            self.logger.debug('%s last seen %s' % (str(node.endpoint), node.last_seen))

        self.join_socket.close()
        del self.join_socket

        self.disc_socket.close()
        del self.disc_socket

        if self.control_sock is not None:
            self.control_sock.close()
        del self.control_sock

        ServiceMixin._cleanup(self)

    def _pre_poll(self):
        if self.pubnext < time():
            self.marco_msg.send(self.disc_socket)
            self.pubnext = time() + self.pubint
            self.pubcnt += 1

        self.poller_timer = 1e3 * max(0, self.pubnext - time())

    def _post_poll(self, items):
        if self.join_socket in items:
            msg = CONTROL.recv(self.join_socket)
            self.reqcnt += 1

            (repmsg, remote, sos_group) = self.__get_rep(msg)

            repmsg.copy_peer_id_from(msg)
            repmsg.send(self.join_socket)
            self.repcnt += 1

            if remote is not None:
                remote.touch()

            if sos_group is not None:
                # do group reset
                self.__stop_group(sos_group)

    def __get_rep(self, msg):
        (repmsg, remote, sos_group) = (None, None, None)

        if msg.is_error:
            errstr = 'invalid base endpoint received: {}'.format(msg.errstr)
            self.logger.error(errstr)
            repmsg = WTF(0, errstr)

        elif msg.command in ['polo', 'sos']:
            remote = self.tree.find_node_by_endpoint(msg.endpoint)

            if 'polo' == msg.command:
                if remote is None:
                    repmsg = self.__assign(msg)

                elif remote.uuid != msg.uuid:
                    # node recovered before it was recognized as down; just resend the
                    # same assignment info as before
                    self.logger.debug('%s rePOLOed with new UUID' % str(remote.endpoint))
                    remote.uuid = msg.uuid
                    repmsg = remote.assignment()

                else:
                    repmsg = WTF(0, 'too chatty; already POLOed')

            elif 'sos' == msg.command:
                if remote is None:
                    repmsg = WTF(0, 'I know not you!')

                else:
                    repmsg = CONTROL('keepcalm', self.endpoint, self.uuid)
                    sos_group = self.__check_sos(remote)

            else:
                repmsg = WTF(0, 'I know not what to do!')

        # unknown command
        else:
            repmsg = WTF(0, 'I know not what to do!')

        return repmsg, remote, sos_group

    def __stop_group(self, stop_group):
        collector = self.collectors[stop_group]
        size = len(collector.children)

        # remove group's branch from the tree and local state
        self.tree.remove_branch(collector)
        del(self.collectors[stop_group])
        del(self.sos_pings[stop_group])

        self.logger.debug('attempting to stop %d nodes in group %s' % (size, stop_group))
        num = self.__stop_nodes(size, stop_group)
        self.logger.debug('%d nodes in group %s stopped' % (num, stop_group))

    def __stop_all_nodes(self):
        size = len(self.tree) - 1  # don't count this (root) node
        self.logger.debug('attempting to stop {} nodes'.format(size))
        num = self.__stop_nodes(size)
        self.logger.debug('{} nodes stopped'.format(num))

    def __stop_nodes(self, expected, stop_group=None):

        stop_msg = STOP(self.endpoint, self.uuid)
        num_rep = 0

        self.control_sock = self.ctx.socket(ROUTER)
        bind_addr = self.control_sock.bind_to_random_port("tcp://*")

        # subtract TOPO_JOIN offset so the port calculated by the remote node matches the
        # random port to which we just bound
        ep = EndpntSpec("localhost", bind_addr - EndpntSpec.CONTROL)

        if stop_group is None:
            pub_msg = MARCO(ep, gen_uuid())  # new uuid so nodes response
        else:
            pub_msg = GROUP(stop_group, ep, gen_uuid())

        # pub to all connected nodes
        pub_msg.send(self.disc_socket)

        # poll for answers
        #
        # TODO: this will block the service for half a minute; combine this
        #       with self.poller in case we are not stopping? blocking the mgmt
        #       service might actually be good: nodes are not re-connected to
        #       the system until all nodes have stopped?
        timeout = now_secs() + 30
        while timeout >= now_secs() and num_rep < expected:
            if self.control_sock.poll(timeout=1000) != 0:
                while True:
                    try:
                        polo = POLO.recv(self.control_sock)
                    except Again:
                        break  # nothing to read; go back to polling

                    assert polo is not None
                    if polo.is_error:
                        self.logger.error('received error from node: %s' % polo)
                        continue

                    # send STOP command
                    stop_msg.copy_peer_id_from(polo)
                    stop_msg.send(self.control_sock)
                    num_rep += 1

        self.control_sock.close()
        self.control_sock = None
        return num_rep

    def __assign(self, polo_msg):
        """
        Method to handle assigning joining node to topology:
        * lookup node's group
        * promote to collector (if first in group)
        * assign its parent node

        Returns CONTROL or None
        """

        # lookup node group
        # @todo need to keep track of nodes which have already POLO'ed / issue #39
        for group in self.config_service.get_groups():
            if polo_msg.endpoint in self.config_service.get_endpoints(group):
                self.logger.debug('found base group: %s' % group)

                if group in self.collectors:
                    # group already exists, make sensor (leaf) node
                    parent = self.collectors[group]
                    level = 'leaf'
                else:
                    # first node in group, make collector
                    parent = self.tree.root
                    level = 'branch'

                node = TopoNode(polo_msg.endpoint, polo_msg.uuid, level, group)
                if parent == self.tree.root:
                    self.collectors[group] = node

                node.touch()
                node = self.tree.insert_node(node, parent)
                self.config_service[self.tree.get_topo_key(node)] = node.last_seen

                # create reply message
                return node.assignment()

        # silently ignore unknown base endpoints
        self.logger.debug('no base group found for %s' % str(polo_msg.endpoint))
        # @todo: cannot return None--using strict REQ/REP pattern / issue #26
        return None

    def __check_sos(self, remote):
        group = remote.group
        nodes = set()

        # add sos to cache
        if group in self.sos_pings:
            (nodes, last_ping) = self.sos_pings[group]
            if (now_secs() - last_ping) > 60:
                self.logger.warn('longer than 60 seconds since last SOS; resetting SOS state')
                nodes = set()

        nodes.add(remote)
        last_ping = now_secs()

        self.sos_pings[group] = (nodes, last_ping)

        # check if over 1/3 threshold
        if len(nodes) < (len(self.collectors[remote.group].children) / 3):
            # not enough nodes have detected the down node; ignore for now
            return None
        else:
            # otherwise, we need to reset the group
            # clear sos cache, first
            return group
