'''
dCAMP Topology Protocol
'''
import logging
from uuid import UUID, uuid4

from dcamp.types.messages.common import DCMsg, _PROPS, WTF
from dcamp.types.specs import EndpntSpec

# @todo: need to include UUIDs in each message so nodes can distinguish between multiple
#        invocations of the same endpoint

__all__ = [
		'gen_uuid',

		'MARCO',
		'POLO',
		'CONTROL',
		'STOP',
		'ASSIGN',
	]

def gen_uuid():
	return uuid4()

class TOPO(DCMsg):
	def __init__(self, ep, id):
		DCMsg.__init__(self)
		assert isinstance(ep, EndpntSpec)
		assert isinstance(id, UUID)
		self.endpoint = ep
		self.uuid = id

	def __str__(self):
		return '%s (%s)' % (self.endpoint, self.uuid)

	@property
	def frames(self):
		return [
				self.endpoint.encode(),
				self._encode_uuid(self.uuid),
			]

	@classmethod
	def from_msg(cls, msg):
		assert isinstance(msg, list)

		# make sure we have exactly two frames
		if 2 != len(msg):
			raise ValueError('wrong number of frames')

		ep = EndpntSpec.decode(msg[0])
		id = DCMsg._decode_uuid(msg[1])
		return cls(ep, id)

# @todo: The MARCO and POLO message types are really the same message structure. These two
#        message classes should just be combined.

class MARCO(TOPO):
	def __init__(self, root_endpoint, root_uuid):
		TOPO.__init__(self, root_endpoint, root_uuid)

class POLO(TOPO):
	def __init__(self, base_endpoint, base_uuid):
		TOPO.__init__(self, base_endpoint, base_uuid)

class CONTROL(DCMsg, _PROPS):
	def __init__(self, command, properties=None):
		assert command in ['assignment', 'stop']
		DCMsg.__init__(self)
		_PROPS.__init__(self, properties)
		self.command = command

	@property
	def frames(self):
		return [
				self.command.encode(),
				self._encode_dict(self.properties),
			]

	@classmethod
	def from_msg(cls, msg):
		# make sure we have two frames
		assert isinstance(msg, list)

		if 2 != len(msg):
			raise ValueError('wrong number of frames')

		cmd = msg[0].decode()
		props = _PROPS._decode_dict(msg[1])

		return cls(command=cmd, properties=props)

def STOP():
	return CONTROL(command='stop')

def ASSIGN(parent_endpoint, level, group):
	assert level in ['root', 'branch', 'leaf']
	if not isinstance(parent_endpoint, EndpntSpec):
		assert isinstance(parent_endpoint, str)
		parent_endpoint = EndpntSpec.from_str(parent_endpoint)

	props = {}
	props['parent'] = parent_endpoint
	props['level'] = level
	props['group'] = group

	return CONTROL(command='assignment', properties=props)
