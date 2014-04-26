import logging
from uuid import UUID
from sys import stdout
from functools import total_ordering
from datetime import datetime

from dcamp.util.decorators import prefixable
from dcamp.types.specs import EndpntSpec
from dcamp.types.messages.topology import ASSIGN

__all__ = [
    'TopoError',
    'DuplicateNodeError',
    'TopoNode',
    'TopoTreeMixin',
]


class TopoError(Exception):
    pass


class DuplicateNodeError(TopoError):
    pass


@total_ordering
class TopoNode(object):
    def __init__(self, endpoint, uuid, level, group):
        assert isinstance(endpoint, EndpntSpec)
        assert isinstance(uuid, UUID)
        assert level in ['root', 'branch', 'leaf']

        self.endpoint = endpoint
        self.uuid = uuid
        self.level = level
        self.group = group

        self.parent = None
        self.children = []
        self.last_seen = 0

    def __eq__(self, given):
        if not isinstance(given, TopoNode):
            return False
        return self.endpoint == given.endpoint

    def __lt__(self, given):
        if given is None:
            return False
        return self.endpoint < given.endpoint

    def __hash__(self):
        return hash(self.endpoint)

    def print(self):
        # "root node for tree: localhost:9090"
        # "collector node for groupA: localhost:5454"
        return "%s node for %s last seen %s" % (
            self.level, 'tree' if self.level == 'root' else self.group, str(self.last_seen))

    def __str__(self):
        return str(self.endpoint)

    def __repr__(self):
        return "TopoSpec(endpoint='%s', uuid=%s, level='%s', group='%s')" % (
            self.endpoint, self.uuid, self.level, self.group)

    def add_child(self, child):
        assert isinstance(child, TopoNode)
        assert child not in self.children
        self.children.append(child)

    def del_child(self, child):
        assert isinstance(child, TopoNode)
        assert child in self.children
        self.children.remove(child)

    def touch(self):
        self.last_seen = datetime.now()
        return self.get_key_value()

    def assignment(self):
        return ASSIGN(self.endpoint, self.uuid, self.parent.endpoint, self.level, self.group)

    def get_key_value(self):
        return self.get_key(), self

    def get_key(self):
        """
        /topo/root = RootNode
        /topo/<group>/collector = BranchNode
        /topo/<group>/nodes/<ep> = LeafNode
        """
        root_key = '/topo/root'
        bran_key = '/topo/{}/collector'.format(self.group)
        leaf_key = '/topo/{}/nodes/{}'.format(self.group, self.endpoint)

        if 'root' == self.level:
            return root_key
        elif 'branch' == self.level:
            return bran_key
        elif 'leaf' == self.level:
            return leaf_key

        raise NotImplementedError('unknown level')


@prefixable
class TopoTreeMixin(object):
    pass

    def __init__(self):
        self.logger = logging.getLogger('dcamp.types.topo')
        self.__root = None
        # { endpoint : TopoNode }
        self.__nodes = {}

    def __len__(self):
        return len(self.__nodes)

    def insert_root(self, root_ep, root_id):
        """
        plants the topo tree root

        N.B.: assumes new root is NOT in tree
        """
        assert root_ep not in self.__nodes

        kvlist = []

        # create new node for the new root; the old node should no longer exist in the tree
        new_root = TopoNode(root_ep, root_id, level='root', group=None)
        old_root = self.__root
        self.__nodes[new_root.endpoint] = new_root
        self.__root = new_root

        # if replacing the current root node, update child-parent relationships
        if old_root is not None:
            self.logger.warn('root node {} replaced by {}'.format(old_root, new_root))
            for c in old_root.children:
                c.parent = new_root
                new_root.add_child(c)

            # remote old root from dict and add kv update for deletion
            del self.__nodes[old_root.endpoint]
            kvlist.append((old_root.get_key(), None))

        # add kv update for new root
        kvlist.append(self.__root.get_key_value())

        # return kv updates
        return kvlist

    def insert_node(self, node, parent):
        assert isinstance(parent, TopoNode)
        assert parent in self.__nodes.values()
        if node.endpoint in self.__nodes or node in self.__nodes.values():
            raise DuplicateNodeError('node already exists: %s' % self.__nodes[node.endpoint])

        parent.add_child(node)
        node.parent = parent
        self.__nodes[node.endpoint] = node

        return [node.get_key_value()]

    def remove_branch(self, node):
        assert node.endpoint in self.__nodes
        assert node.level == 'branch'

        kvlist = []

        # first remove node from its parent's children list
        node.parent.del_child(node)

        # then remove each of node's children from the tree
        for c in node.children:
            del self.__nodes[c.endpoint]
            kvlist.append((c.get_key(), None))

        # and lastly remove node from the tree
        del self.__nodes[node.endpoint]
        kvlist.append((node.get_key(), None))

        # return kv updates
        return kvlist

    def find_node_by_endpoint(self, endpoint):
        return None if endpoint not in self.__nodes else self.__nodes[endpoint]

    def walk(self, node=None):
        if node is None:
            node = self.__root

        yield node.get_key_value()

        for child in node.children:
            yield from self.walk(child)

    def print(self, out=stdout, node=None):
        for n in self.walk(node):
            out.write('%s = %s\n' % n.get_key_value())
