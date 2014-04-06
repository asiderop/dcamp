from time import time

from zmq import ROUTER, PUB, POLLIN, Again  # pylint: disable-msg=E0611

from dcamp.types.messages.common import WTF
import dcamp.types.messages.topology as topo

from dcamp.service.service import ServiceMixin
from dcamp.types.specs import EndpntSpec
from dcamp.types.topo import TopoTreeMixin, TopoNode
from dcamp.util.functions import now_secs


class Management(ServiceMixin):
    """
    Management Service -- provides functionality for interacting with and controlling
    dCAMP.

    @todo: how to handle joining nodes not part of config / issue #26
    """

    def __init__(self,
                 role_pipe,
                 config_service,
                 local_ep,
                 ):
        ServiceMixin.__init__(self, role_pipe)
        assert isinstance(local_ep, EndpntSpec)

        self.config_service = config_service
        self.endpoint = local_ep
        self.uuid = topo.gen_uuid()

        # wait for config service to be fully synched before doing anything;
        # this should return immediately since we are root
        self.config_service.wait_for_gogo()

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

        self.marco_msg = topo.MARCO(self.endpoint, self.uuid)
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
        self.disc_socket.close()
        del self.join_socket, self.disc_socket
        ServiceMixin._cleanup(self)

    def _pre_poll(self):
        if self.pubnext < time():
            self.marco_msg.send(self.disc_socket)
            self.pubnext = time() + self.pubint
            self.pubcnt += 1

        self.poller_timer = 1e3 * max(0, self.pubnext - time())

    def _post_poll(self, items):
        if self.join_socket in items:
            sos_group = None
            polo_msg = topo.POLO.recv(self.join_socket)
            self.reqcnt += 1

            if polo_msg.is_error:
                errstr = 'invalid base endpoint received: {}'.format(polo_msg.errstr)
                self.logger.error(errstr)
                repmsg = WTF(0, errstr)

            else:
                remote = self.tree.find_node_by_endpoint(polo_msg.endpoint)

                if polo_msg.content is None:
                    if remote is None:
                        repmsg = self.__assign(polo_msg)

                    elif remote.uuid != polo_msg.uuid:
                        # node recovered before it was recognized as down; just resend the
                        # same assignment info as before
                        self.logger.debug('%s rePOLOed with new UUID' % str(remote.endpoint))
                        remote.uuid = polo_msg.uuid
                        repmsg = remote.assignment()

                    else:
                        repmsg = WTF(0, 'too chatty; already POLOed')

                elif 'SOS' == polo_msg.content:
                    if remote is None:
                        repmsg = WTF(0, 'I know not you!')
                    else:
                        repmsg = topo.CONTROL('keepcalm')
                        sos_group = self.__check_sos(remote)

                else:
                    repmsg = WTF(0, 'I know not what to do!')

                if remote is not None:
                    remote.touch()

            repmsg.copy_peer_id_from(polo_msg)
            repmsg.send(self.join_socket)
            self.repcnt += 1

            if sos_group is not None:
                # do group reset
                self.__stop_group(sos_group)

    def __stop_group(self, stop_group):
        stop_socket = self.ctx.socket(PUB)

        collector = self.collectors[stop_group]
        size = len(collector.children)

        # connect collector and all its children to the socket
        #stop_socket.connect(collector.endpoint.connect_uri(EndpntSpec.BASE))
        for node in collector.children:
            stop_socket.connect(node.endpoint.connect_uri(EndpntSpec.BASE))

        # remove group's branch from the tree and local state
        self.tree.remove_branch(collector)
        del(self.collectors[stop_group])
        del(self.sos_pings[stop_group])

        self.logger.debug('attempting to stop %d nodes in group %s' % (size, stop_group))
        num = self.__stop_nodes(stop_socket, size)
        stop_socket.close()
        self.logger.debug('%d nodes in group %s stopped' % (num, stop_group))

    def __stop_all_nodes(self):
        size = len(self.tree) - 1  # don't count this (root) node
        self.logger.debug('attempting to stop {} nodes'.format(size))
        num = self.__stop_nodes(self.disc_socket, size)
        self.logger.debug('{} nodes stopped'.format(num))

    def __stop_nodes(self, pub_sock, expected):
        assert PUB == pub_sock.socket_type

        stop = topo.STOP()
        control_sock = self.ctx.socket(ROUTER)
        bind_addr = control_sock.bind_to_random_port("tcp://*")
        num_rep = 0

        # subtract TOPO_JOIN offset so the port calculated by the remote node matches the
        # random port to which we just bound
        ep = EndpntSpec("localhost", bind_addr - EndpntSpec.CONTROL)
        marco = topo.MARCO(ep, topo.gen_uuid())  # new uuid so nodes response

        # pub to all connected nodes
        marco.send(pub_sock)

        # poll for answers
        #
        # TODO: this will block the service for half a minute; combine this
        #       will self.poller in case we are not stopping? blocking the mgmt
        #       service might actually be good: nodes are not re-connected to
        #       the system until all nodes have stopped?
        timeout = now_secs() + 30
        while timeout >= now_secs() and num_rep < expected:
            if control_sock.poll(timeout=1000) != 0:
                while True:
                    try:
                        polo = topo.POLO.recv(control_sock)
                    except Again:
                        break  # nothing to read; go back to polling

                    assert polo is not None
                    if polo.is_error:
                        self.logger.error('received error from node: %s' % polo)
                        continue

                    # send STOP command
                    stop.copy_peer_id_from(polo)
                    stop.send(control_sock)
                    num_rep += 1

        control_sock.close()
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
