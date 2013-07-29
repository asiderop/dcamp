from sys import stdout
from functools import total_ordering
from datetime import datetime

from dcamp.util.decorators import Prefixable
from dcamp.data.specs import EndpntSpec

class TopoError(Exception):
	pass
class DuplicateNodeError(TopoError):
	pass

@total_ordering
class TopoNode(object):
	def __init__(self, endpoint, role, group):
		assert isinstance(endpoint, EndpntSpec)

		self.endpoint = endpoint
		self.role = role
		self.group = group

		self.parent = None
		self.children = []
		self.last_seen = 0

	def __eq__(self, given):
		return self.endpoint == given.endpoint
	def __lt__(self, given):
		return self.endpoint < given.endpoint
	def __hash__(self):
		return hash(self.endpoint)
	def __str__(self):
		# "root node for tree: localhost:9090"
		# "collector node for groupA: localhost:5454"
		return "%s node for %s: %s" % (self.role,
				'tree' if self.role == 'root' else self.group,
				str(self.endpoint))

	def __repr__(self):
		return "TopoSpec(endpoint='%s', role='%s', group='%s')" % (self.endpoint,
				self.role, self.group)

	def add_child(self, child):
		assert isinstance(child, TopoNode)
		assert child not in self.children
		self.children.append(child)

	def touch(self):
		self.last_seen = datetime.now()

@Prefixable
class TopoTree(object):
	pass

	def __init__(self, root_ep):
		self.root = TopoNode(root_ep, role='root', group=None)
		self.nodes = { root_ep: self.root }

	def insert_endpoint(self, node_ep, role, group, parent):
		if node_ep in self.nodes:
			raise DuplicateNodeError('endpoint already exists: %s' % self.nodes[node_ep])

		node = TopoNode(node_ep, role, group)
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

	def find_node_by_endpoint(self, endpoint):
		return None if endpoint not in self.nodes else self.nodes[endpoint]

	def get_topo_key(self, node):
		assert node in self.nodes.values()
		assert node.endpoint in self.nodes

		key = str(node.endpoint)
		parent = node.parent
		while parent != None:
			key = str(parent.endpoint) + self._delimiter + key
		return self._delimiter + key

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
