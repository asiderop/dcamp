'''
dCAMP message module
'''
import logging
import struct
# zmq.jsonapi ensures bytes, instead of unicode:
import zmq.utils.jsonapi as json

from dcamp.data.config import EndpntSpec

verbose_debug = True

class DCMsg(object):
	'''
	Base dCAMP message
	'''

	V0_1 = '0.1'
	V_CURRENT = V0_1

	logger = logging.getLogger("dcamp.dcmsg")

	def __init__(self, version):
		self._version = version

	@property
	def frames(self):
		return [self.name.encode(),
				self.version.encode()]

	@property
	def version(self):
		return self._version

	@property
	def name(self):
		return self.__class__.__name__

	def send(self, socket):
		self.logger.debug('S:%s (v%s)' % (self.name, self.version))
		if verbose_debug:
			for part in str(self).split('\n')[2:]: # skip name and version
				self.logger.debug('  '+ part)
		socket.send_multipart(self.frames)

	def __iter__(self):
		return iter(self.frames)

	def __str__(self):
		result = ''
		count = 0
		for f in self.frames:
			result += 'Frame %d: %s\n' % (count, f.decode())
			count += 1
		return result[:len(result)-1] # drop the trailing new-line

	@classmethod
	def recv(cls, socket):
		return cls.from_msg(socket.recv_multipart())

	@classmethod
	def from_msg(cls, msg):
		# assert we have at least two frames
		assert isinstance(msg, list)
		assert 2 <= len(msg)

		name = msg[0].decode()
		ver = msg[1].decode()

		if ver > cls.V_CURRENT:
			cls.logger.warning('message version (%s) is greater than code version (%s)' %
					(ver, cls.V_CURRENT))

		for c in cls.__subclasses__():
			if c.__name__ == name:
				result = c.from_msg(ver, msg[2:]) # class found, so return it
				c.logger.debug('R:%s (v%s)' % (name, ver))
				if verbose_debug:
					for part in str(result).split('\n')[2:]: # skip name and version
						cls.logger.debug('  '+ part)
				return result

		cls.logger.fatal("no subclass matches found: %s" % name)
		return cls() # if class not found, return generic

class MARCO(DCMsg):
	def __init__(self, root_endpoint, version=DCMsg.V_CURRENT):
		DCMsg.__init__(self, version)
		if not isinstance(root_endpoint, EndpntSpec):
			assert isinstance(root_endpoint, str)
			root_endpoint = EndpntSpec.from_str(root_endpoint)
		self.root_endpoint = root_endpoint

	@property
	def frames(self):
		return super().frames + [
				self.root_endpoint.encode()
			]

	@classmethod
	def from_msg(cls, ver, msg):
		# make sure we have exactly one frame
		assert isinstance(msg, list)
		assert 1 == len(msg)
		return cls(msg[0].decode(), version=ver)

class POLO(DCMsg):
	def __init__(self, base_endpoint, version=DCMsg.V_CURRENT):
		DCMsg.__init__(self, version)
		if not isinstance(base_endpoint, EndpntSpec):
			assert isinstance(base_endpoint, str)
			base_endpoint = EndpntSpec.from_str(base_endpoint)
		self.base_endpoint = base_endpoint

	@property
	def frames(self):
		return super().frames + [
				self.base_endpoint.encode()
			]

	@classmethod
	def from_msg(cls, ver, msg):
		# make sure we have exactly one frame
		assert isinstance(msg, list)
		assert 1 == len(msg)
		return cls(msg[0].decode(), version=ver)

class CONTROL(DCMsg):
	def __init__(self, parent_endpoint, properties=None, version=DCMsg.V_CURRENT):
		DCMsg.__init__(self, version)
		if not isinstance(parent_endpoint, EndpntSpec):
			assert isinstance(parent_endpoint, str)
			parent_endpoint = EndpntSpec.from_str(parent_endpoint)
		self.parent_endpoint = parent_endpoint
		assert properties is None or isinstance(properties, dict)
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
		return super().frames + [
				self.parent_endpoint.encode(),
				json.dumps(self.properties)
			]

	@classmethod
	def from_msg(cls, ver, msg):
		# make sure we have two frames
		assert isinstance(msg, list)
		assert 2 == len(msg)
		return cls(msg[0].decode(), properties=json.loads(msg[1]), version=ver)

class WTF(DCMsg):
	def __init__(self, errcode, errstr='', version=DCMsg.V_CURRENT):
		DCMsg.__init__(self, version)
		assert isinstance(errcode, int)
		assert isinstance(errstr, str)
		self.errcode = errcode
		self.errstr = errstr

	@property
	def frames(self):
		return super().frames + [
				struct.pack('!i', self.errcode),
				self.errstr.encode()
			]

	@classmethod
	def from_msg(cls, ver, msg):
		# make sure we have either two or three frames
		assert isinstance(msg, list)
		assert len(msg) in [1, 2]

		code = struct.unpack('!i', msg[0])[0]
		errstr = ''
		if len(msg) == 2:
			errstr = msg[1].decode()
		return cls(code, errstr, version=ver)
