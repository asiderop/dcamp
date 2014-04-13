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

    def __str__(self):
        # "root node for tree: localhost:9090"
        # "collector node for groupA: localhost:5454"
        return "%s node for %s last seen %s" % (self.level,
                                                'tree' if self.level == 'root' else self.group,
                                                str(self.last_seen))

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

    def assignment(self):
        return ASSIGN(self.endpoint, self.uuid, self.parent.endpoint, self.level, self.group)


@prefixable
class TopoTreeMixin(object):
    pass

    def __init__(self, root_ep, root_id):
        self.root = TopoNode(root_ep, root_id, level='root', group=None)
        self.nodes = {root_ep: self.root}

    def __len__(self):
        return len(self.nodes)

    def insert_endpoint(self, node_ep, node_id, level, group, parent):
        if node_ep in self.nodes:
            raise DuplicateNodeError('endpoint already exists: %s' % self.nodes[node_ep])

        node = TopoNode(node_ep, node_id, level, group)
        return self.insert_node(node, parent)

    def insert_node(self, node, parent):
        assert isinstance(parent, TopoNode)
        assert parent in self.nodes.values()
        if node.endpoint in self.nodes or node in self.nodes.values():
            raise DuplicateNodeError('node already exists: %s' % self.nodes[node.endpoint])

        parent.add_child(node)
        node.parent = parent
        self.nodes[node.endpoint] = node
        return node

    def remove_branch(self, node):
        assert node.endpoint in self.nodes
        assert node.level == 'branch'

        # first remove node from its parent's children list
        node.parent.del_child(node)

        # then remove each of node's children from the tree
        for c in node.children:
            del(self.nodes[c.endpoint])

        # and lastly remove node from the tree
        del(self.nodes[node.endpoint])

    def find_node_by_endpoint(self, endpoint):
        return None if endpoint not in self.nodes else self.nodes[endpoint]

    def get_topo_key(self, node):
        # TODO: these topo keys are bogus: how do we handle parent endpoints failing?
        #       branches are rebuilt. okay. root? maybe use "root" instead of root
        #       endpoint?
        assert node in self.nodes.values()
        assert node.endpoint in self.nodes

        key = str(node.endpoint)
        parent = node.parent
        while parent is not None:
            key = str(parent.endpoint) + self._delimiter + key
            parent = parent.parent
        return self._delimiter + 'topo' + self._delimiter + key

    def walk(self):
        self._push_prefix('topo')

        self._push_prefix(str(self.root.endpoint))
        yield from self.__walk()
        self._pop_prefix()

        # remove "topo" prefix
        self._pop_prefix()

        # verify we popped as many times as we pushed
        assert len(self._get_prefix()) == 1

    def __walk(self, node=None):
        if node is None:
            node = self.root

        yield (self._get_prefix(), node)

        for child in node.children:
            self._push_prefix(str(child.endpoint))
            yield from self.__walk(child)
            self._pop_prefix()

    def print(self, out=stdout):
        self._push_prefix('topo')

        self._push_prefix(str(self.root.endpoint))
        self.__print(out=out)
        self._pop_prefix()

        # remove "topo" prefix
        self._pop_prefix()

        # verify we popped as many times as we pushed
        assert len(self._get_prefix()) == 1

    def __print(self, out, node=None):
        if node is None:
            node = self.root

        out.write('%s = %s\n' % (self._get_prefix(), node))
        for child in node.children:
            self._push_prefix(str(child.endpoint))
            self.__print(out, child)
            self._pop_prefix()
