'''
dCAMP message module
'''
import logging
import struct
# zmq.jsonapi ensures bytes, instead of unicode:
import zmq.utils.jsonapi as json

from dcamp.config import EndpntSpec, str_to_ep, DCParsingError

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
		self.logger.debug('S:%s' % self.__class__.__name__)
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
				c.logger.debug('R:%s' % c.__name__)
				return c.from_msg(msg) # class found, so return it

		cls.logger.fatal("no subclass matches found")
		return cls() # if class not found, return generic

class MARCO(DCMsg):
	def __init__(self, root_endpoint):
		assert isinstance(root_endpoint, EndpntSpec)
		self.root_endpoint = root_endpoint

	@property
	def frames(self):
		return [self.name,
				self.root_endpoint.encode()]

	@classmethod
	def from_msg(cls, msg):
		# assert we have two frames and correct key
		assert isinstance(msg, list)
		assert 2 == len(msg)
		assert cls.__name__.encode() == msg[0]
		return cls(str_to_ep(msg[1].decode()))

class POLO(DCMsg):
	def __init__(self, base_endpoint):
		assert isinstance(base_endpoint, EndpntSpec)
		self.base_endpoint = base_endpoint

	@property
	def frames(self):
		return [self.name,
				self.base_endpoint.encode()]

	@classmethod
	def from_msg(cls, msg):
		# assert we have two frames and correct key
		assert isinstance(msg, list)
		assert 2 == len(msg)
		assert cls.__name__.encode() == msg[0]
		return cls(str_to_ep(msg[1].decode()))

class ASSIGN(DCMsg):
	def __init__(self, parent_endpoint, properties=None):
		assert isinstance(parent_endpoint, EndpntSpec)
		assert properties is None or isinstance(properties, dict)
		self.parent_endpoint = parent_endpoint
		self.properties = {} if properties is None else properties

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
				self.parent_endpoint.encode(),
				json.dumps(self.properties)]

	@classmethod
	def from_msg(cls, msg):
		# assert we have three frames and correct key
		assert isinstance(msg, list)
		assert 3 == len(msg)
		assert cls.__name__.encode() == msg[0]
		ep = str_to_ep(msg[1].decode())
		return cls(ep, properties=json.loads(msg[2]))

class WTF(DCMsg):
	def __init__(self, errcode, errstr=''):
		assert isinstance(errcode, int)
		assert isinstance(errstr, str)
		self.errcode = errcode
		self.errstr = errstr

	@property
	def frames(self):
		return [self.name,
				struct.pack('!i', self.errcode),
				self.errstr.encode()]

	@classmethod
	def from_msg(cls, msg):
		# assert we have either two or three frames and correct key
		assert isinstance(msg, list)
		assert len(msg) in [2, 3]
		assert cls.__name__.encode() == msg[0]

		code = struct.unpack('!i', msg[1])[0]
		errstr = ''
		if len(msg) == 3:
			errstr = msg[2].decode()
		return cls(code, errstr)
