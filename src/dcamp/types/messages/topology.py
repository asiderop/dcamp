'''
dCAMP message module
'''
import logging

from dcamp.types.messages.common import DCMsg, _PROPS, WTF
from dcamp.types.specs import EndpntSpec

# @todo: need to include UUIDs in each message so nodes can distinguish between multiple
#        invocations of the same endpoint

class MARCO(DCMsg):
	def __init__(self, root_endpoint):
		DCMsg.__init__(self)
		assert isinstance(root_endpoint, EndpntSpec)
		self.root_endpoint = root_endpoint

	@property
	def frames(self):
		return [ self.root_endpoint.encode() ]

	@classmethod
	def from_msg(cls, msg):
		assert isinstance(msg, list)

		# make sure we have exactly one frame
		if 1 != len(msg):
			raise ValueError('wrong number of frames')
		return cls(EndpntSpec.decode(msg[0]))

class POLO(DCMsg):
	def __init__(self, base_endpoint):
		DCMsg.__init__(self)
		assert isinstance(base_endpoint, EndpntSpec)
		self.base_endpoint = base_endpoint

	@property
	def frames(self):
		return [ self.base_endpoint.encode() ]

	@classmethod
	def from_msg(cls, msg):
		assert isinstance(msg, list)

		# make sure we have exactly one frame
		if 1 != len(msg):
			raise ValueError('wrong number of frames')
		return cls(EndpntSpec.decode(msg[0]))

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
