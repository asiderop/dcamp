'''
dCAMP message module
'''
import logging
# zmq.jsonapi ensures bytes, instead of unicode:
import zmq.utils.jsonapi as json

class DCMsg(object):
	'''
	Base dCAMP message
	'''

	logger = logging.getLogger("dcamp.dcmsg")

	@property
	def frames(self):
		return list();

	@property
	def name(self):
		return self.__class__.__name__.encode()

	def send(self, socket):
		socket.send_multipart(self.frames)

	def __iter__(self):
		return iter(self.frames)

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
			if c.__name__.encode() == key:
				cls.logger.debug("found subclass match: %s", c.__name__)
				return c.from_msg(msg) # class found, so return it

		cls.logger.debug("no subclass matches found")
		return cls() # if class not found, return generic

class MARCO(DCMsg):
	def __init__(self, root_endpoint):
		assert isinstance(root_endpoint, bytes)
		self.root_endpoint = root_endpoint

	@property
	def frames(self):
		return [self.name, self.root_endpoint]

	@classmethod
	def from_msg(cls, msg):
		# assert we have at two frames and correct key
		assert isinstance(msg, list)
		assert 2 == len(msg)
		assert cls.__name__.encode() == msg[0]
		return cls(msg[1])

class POLO(DCMsg):
	def __init__(self, base_endpoint):
		assert isinstance(base_endpoint, bytes)
		self.base_endpoint = base_endpoint

	@property
	def frames(self):
		return [self.name, self.base_endpoint]

	@classmethod
	def from_msg(cls, msg):
		# assert we have at two frames and correct key
		assert isinstance(msg, list)
		assert 2 == len(msg)
		assert cls.__name__.encode() == msg[0]
		return cls(msg[1])

class ASSIGN(DCMsg):
	def __init__(self, parent_endpoint, properties=None):
		assert isinstance(parent_endpoint, bytes)
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
		return [self.name,
				self.parent_endpoint,
				json.dumps(self.properties)]

	@classmethod
	def from_msg(cls, msg):
		# assert we have at three frames and correct key
		assert isinstance(msg, list)
		assert 3 == len(msg)
		assert cls.__name__.encode() == msg[0]

		return cls(msg[1], properties=json.loads(msg[2]))

class WTF(DCMsg):
	def __init__(self, errcode, errstr=None):
		assert isinstance(errcode, int)
		assert errstr is None or isinstance(errstr, bytes)
		self.errcode = errcode
		self.errstr = '' if errstr is None else errstr

	@property
	def frames(self):
		return [self.name, errcode, errstr]

	@classmethod
	def from_msg(cls, msg):
		# assert we have at two frames and correct key
		assert isinstance(msg, list)
		assert 2 == len(msg)
		assert cls.__name__.encode() == msg[0]
		return cls(msg[1])
