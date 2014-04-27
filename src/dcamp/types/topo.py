import logging
from re import compile
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
        /TOPO/root = RootNode
        /TOPO/<group>/collector = BranchNode
        /TOPO/<group>/leaves/<ep> = LeafNode
        """
        root_key = '/TOPO/root'
        bran_key = '/TOPO/{}/collector'.format(self.group)
        leaf_key = '/TOPO/{}/leaves/{}'.format(self.group, self.endpoint)

        if 'root' == self.level:
            return root_key
        elif 'branch' == self.level:
            return bran_key
        elif 'leaf' == self.level:
            return leaf_key

        raise NotImplementedError('unknown level')


@prefixable
class TopoTreeMixin(object):
    """
    Topology Tree Representation

    All non-const methods return list of (k,v) tuples to be replicated via Configuration service.

    PUB/SUB Key Patterns:
        /TOPO/root = RootNode
        /TOPO/<group>/collector = BranchNode
        /TOPO/<group>/leaves/<ep> = LeafNode
    """
    root_pattern = compile(r'/TOPO/root')
    bran_pattern = compile(r'/TOPO/(?P<group>[\w\-.]+)/collector')
    leaf_pattern = compile(r'/TOPO/(?P<group>[\w\-.]+)/leaves/(?P<endpoint>[\w\-.]+:\d+)')

    def __init__(self):
        self.logger = logging.getLogger('dcamp.types.topo')
        # { endpoint : TopoNode }
        self.__nodes = {}  # every node in tree, by endpoint
        self.__root = None  # root node
        # { group : TopoNode }
        self.__collectors = {}  # collector nodes, by group

    def __len__(self):
        return len(self.__nodes)

    def walk(self, node=None):
        if node is None:
            node = self.__root

        yield node.get_key_value()

        for child in node.children:
            yield from self.walk(child)

    def print(self, out=stdout, node=None):
        for (k, n) in self.walk(node):
            out.write('%s = %s\n' % (k, n))

    def kv_update(self, k, v):
        assert k.startswith('/TOPO')

        if not isinstance(v, TopoNode):
            self.logger.error('tree given non-TopoNode value: {}'.format(type(v)))
            return False

        # these fields are not marshallable
        v.parent = None
        v.children = []

        # check key in common-case order

        m = TopoTreeMixin.leaf_pattern.match(k)
        if m is not None:
            return self.__kv_update_leaf(m, v)

        m = TopoTreeMixin.bran_pattern.match(k)
        if m is not None:
            return self.__kv_update_branch(m, v)

        m = TopoTreeMixin.root_pattern.match(k)
        if m is not None:
            return self.__insert_root_node(v)

        self.logger.error('unknown topo key')
        return False

    def __kv_update_leaf(self, m, v):
        (m_g, m_e) = (m.group('group'), m.group('endpoint'))
        if (m_g, m_e, 'leaf') != (v.group, str(v.endpoint), v.level):
            self.logger.error('key / value do not match; {} != {}'.format(
                (m_g, m_e), (v.group, v.endpoint, v.level)))
            return False

        # if new node
        if v.endpoint not in self.__nodes:
            assert v not in self.__nodes.values()
            # v.parent is not valid at this point; lookup parent by group
            parent = self.get_collector(v.group)
            if parent is None:
                self.logger.error('group collector node missing: {}'.format(v.group))
                return False

            self.insert_node(v, parent)
            return True

        # otherwise, update

        # v.parent and v.children are not valid at this point; get from
        # current node before replacing
        old = self.__nodes[v.endpoint]
        v.parent = old.parent
        v.children = old.children

        self.__nodes[v.endpoint] = v
        return True

    def __kv_update_branch(self, m, v):
        m_g = m.group('group')
        if (m_g, 'branch') != (v.group, v.level):
            self.logger.error('key / value do not match; {} != {}'.format(m_g, (v.group, v.level)))
            return False

        # group currently exists
        if v.group in self.__collectors:
            old = self.__collectors[v.group]

            if v == old:
                # same node; just update
                assert v.endpoint in self.__nodes
                assert v in self.__nodes.values()
                # v.parent and v.children are not valid at this point; get from
                # current node before replacing
                v.parent = old.parent
                v.children = old.children

                self.__nodes[v.endpoint] = v
                self.__collectors[v.endpoint] = v
                return True

            else:
                # replacing old with new; delete branch first
                self.logger.warn('old collector still exists in tree; deleting branch')
                self.remove_collector(old)

        # otherwise, new group
        assert v.endpoint not in self.__nodes
        assert v not in self.__nodes.values()
        self.insert_node(v, self.__root)
        return True

    #####
    # root access

    def root(self):
        return self.__root

    def insert_root(self, root_ep, root_id):
        assert root_ep not in self.__nodes
        new_root = TopoNode(root_ep, root_id, level='root', group=None)

        return self.__insert_root_node(new_root)

    def __insert_root_node(self, new_root):
        assert new_root not in self.__nodes.values()

        kvlist = []

        if new_root == self.__root:
            self.logger.warn('new root is same as old; no-op')
            return kvlist

        # the new root node should not exist in the tree; that is, the old collector node
        # should already have been removed from the topology
        old_root = self.__root
        self.__nodes[new_root.endpoint] = new_root
        self.__root = new_root

        # if replacing the current root node, update child-parent relationships
        if old_root is not None:
            self.logger.warn('root node {} replaced by {}'.format(old_root, new_root))
            for c in old_root.children:
                c.parent = new_root
                new_root.add_child(c)

            # remove old root from dict; no kv update needed for deletion since it is being replaced
            del self.__nodes[old_root.endpoint]

        # add kv update for new root
        kvlist.append(self.__root.get_key_value())

        # return kv updates
        return kvlist

    #####
    # branch/collector access

    def get_collector(self, group):
        return None if group not in self.__collectors else self.__collectors[group]

    def remove_collector(self, node):
        assert node.endpoint in self.__nodes
        assert node.level == 'branch'

        kvlist = []

        if node.endpoint not in self.__collectors:
            self.logger.error('node {} is not in collectors list'.format(node))
            return kvlist

        # first remove node from its parent's children list
        node.parent.del_child(node)

        # then remove each of node's children from the tree
        for c in node.children:
            del self.__nodes[c.endpoint]
            kvlist.append((c.get_key(), None))

        # and lastly remove node from the tree
        del self.__nodes[node.endpoint]
        del self.__collectors[node.endpoint]
        kvlist.append((node.get_key(), None))

        # return kv updates
        return kvlist

    #####
    # leaf/node access

    def get_node(self, endpoint):
        return None if endpoint not in self.__nodes else self.__nodes[endpoint]

    def insert_node(self, node, parent):
        assert isinstance(parent, TopoNode)
        assert parent in self.__nodes.values()

        kvlist = []

        if node.endpoint in self.__nodes or node in self.__nodes.values():
            raise DuplicateNodeError('node already exists: %s' % self.__nodes[node.endpoint])

        parent.add_child(node)
        node.parent = parent
        self.__nodes[node.endpoint] = node

        if 'branch' == node.level:
            assert node.parent == self.__root
            if node.group in self.__collectors:
                self.logger.warn('branch node {} replaced by {}'.format(self.__collectors[node.group], node))
            # no "delete" kv update needed for replacement
            self.__collectors[node.group] = node

        # add kv update for new node
        kvlist.append(node.get_key_value())

        # return kv updates
        return kvlist

    def update_node(self, node):
        assert node in self.__nodes.values()
        assert node.endpoint in self.__nodes
        # no assignment actually needed since TopoNode is reference to node already in tree
        return [node.get_key_value()]

    def touch_node(self, node):
        assert node in self.__nodes.values()
        assert node.endpoint in self.__nodes
        # no assignment actually needed since TopoNode is reference to node already in tree
        node.touch()
        return [node.get_key_value()]
