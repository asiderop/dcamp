'''
dCAMP message module
'''

class DCMsg(object):
	'''
	Base dCAMP message
	'''

	def __init__(self, frames=None):
		self._frames = list() if frames is None else frames

	def send(self, socket):
		assert isinstance(self._frames, list)
		socket.send_multipart(self._frames)

	@classmethod
	def recv(cls, socket):
		return cls.from_msg(socket.recv_multipart())

	@classmethod
	def from_msg(cls, msg):
		# assert we have at least two frames
		assert isinstance(msg, list)
		assert 2 <= len(msg)

		key = msg[0]
		for c in cls.__subclasses__():
			if c.__name__ == key:
				return c.from_msg(msg) # class found, so return it

		return cls(msg) # if class not found, return generic

class MARCO(DCMsg):
	def __init__(self, root_endpoint):
		assert isinstance(root_endpoint, str)

	@property
	def frames(self):
		return [self.__class__.__name__, root_endpoint]

	@classmethod
	def from_msg(cls, msg):
		# assert we have at two frames and correct key
		assert isinstance(msg, list)
		assert 2 == len(msg)
		assert cls.__name__ == msg[0]
		return cls(msg[1])

class POLO(DCMsg):
	def __init__(self, base_endpoint):
		assert isinstance(base_endpoint, str)
		self.base_endpoint = base_endpoint

	@property
	def frames(self):
		return [self.__class__.__name__, base_endpoint]

	@classmethod
	def from_msg(cls, msg):
		# assert we have at two frames and correct key
		assert isinstance(msg, list)
		assert 2 == len(msg)
		assert cls.__name__ == msg[0]
		return cls(msg[1])

class ASSIGN(DCMsg):
	# zmq.jsonapi ensures bytes, instead of unicode:
	import zmq.utils.jsonapi as json

	def __init__(self, parent_endpoint, properties=None):
		assert isinstance(parent_endpoint, str)
		assert properties is None or isinstance(properties, dict)
		self.parent_endpoint = parent_endpoint
		self.properties = dict() if properties is None else properties

	# dictionary access maps to properties:
	def __getitem__(self, k):
		return self.properties[k]

	def __setitem__(self, k, v):
		self.properties[k] = v

	def get(self, k, default=None):
		return self.properties.get(k, default)

	@property
	def frames(self):
		return [self.__class__.__name__,
				parent_endpoint,
				json.dumps(self.properties)]

	@classmethod
	def from_msg(cls, msg):
		# assert we have at three frames and correct key
		assert isinstance(msg, list)
		assert 3 == len(msg)
		assert cls.__name__ == msg[0]

		return cls(msg[1], properties=json.loads(msg[2]))

class WTF(DCMsg):
	def __init__(self, errcode, errstr=None):
		assert isinstance(errcode, int)
		assert errstr is None or isinstance(errstr, str)
		self.errcode = errcode
		self.errstr = '' if errstr is None else errstr

	@property
	def frames(self):
		return [self.__class__.__name__, errcode, errstr]

	@classmethod
	def from_msg(cls, msg):
		# assert we have at two frames and correct key
		assert isinstance(msg, list)
		assert 2 == len(msg)
		assert cls.__name__ == msg[0]
		return cls(msg[1])
