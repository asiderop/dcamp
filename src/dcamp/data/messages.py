'''
dCAMP message module
'''
import logging, struct

# zmq.jsonapi ensures bytes, instead of unicode:
import zmq.utils.jsonapi as jsonapi

from dcamp.data.specs import SerializableSpecTypes, EndpntSpec

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

	@staticmethod
	def _encode_int(val):
		# pack as 8-byte int using network order
		return struct.pack('!q', val)

	@staticmethod
	def _decode_int(buffer):
		# unpack as 8-byte int using network order
		return struct.unpack('!q', buffer)[0]

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

class _PROPS(object):
	def __init__(self, properties=None):
		assert properties is None or isinstance(properties, dict)
		self.properties = {} if properties is None else properties

	# dictionary access maps to properties:
	def __getitem__(self, k):
		return self.properties[k]

	def __setitem__(self, k, v):
		self.properties[k] = v

	def get(self, k, default=None):
		return self.properties.get(k, default)

	def _encode_props(self):
		# { key : [ (value-type-name, value), ... ] }
		props = dict()

		for (key, value) in self.properties.items():
			if type(value) == list:
				new_list = list()
				for val in value:
					new_list.append(_PROPS.__type_tuple_from_value(val))
				props[key] = new_list
			else:
				props[key] = _PROPS.__type_tuple_from_value(value)

		return jsonapi.dumps(props)

	@staticmethod
	def __type_tuple_from_value(value):
		type_name = type(value).__name__
		# special case spec types (namedtuple) to use dict as values instead of list
		if type_name in SerializableSpecTypes:
			value = value._asdict()
		return (type_name, value)

	@staticmethod
	def __value_from_type_tuple(given):
		assert 2 == len(given)
		name = given[0]
		value = given[1]
		if name in SerializableSpecTypes:
			return SerializableSpecTypes[name](**value)
		else:
			return value

	@staticmethod
	def _decode_props(msg_str):
		# unpack the json string into actual data types
		props = dict()
		decoded = jsonapi.loads(msg_str)
		for (key, value_list) in decoded.items():
			# each element will either be a list (tuple)...
			if len(value_list) == 2 and type(value_list[0]) != list:
				props[key] = _PROPS.__value_from_type_tuple(value_list)

			# ...or a list of lists (tuples)
			else:
				for value in value_list:
					if type(value) == list:
						new_list = list()
						for val in value:
							new_list.append(_PROPS.__value_from_type_tuple(value))
						props[key] = new_list

		return props

class CONTROL(DCMsg, _PROPS):
	def __init__(self, command, properties=None, version=DCMsg.V_CURRENT):
		DCMsg.__init__(self, version)
		_PROPS.__init__(self, properties)
		assert command in ['assignment', 'stop']
		self.command = command

	@property
	def frames(self):
		return super().frames + [
				self.command.encode(),
				self._encode_props()
			]

	@classmethod
	def from_msg(cls, ver, msg):
		# make sure we have two frames
		assert isinstance(msg, list)
		assert 2 == len(msg)

		cmd = msg[0].decode()
		props = _PROPS._decode_props(msg[1])

		return cls(command=cmd, properties=props, version=ver)

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

class CONFIG(DCMsg, _PROPS):
	def __init__(self, key, sequence, uuid=None, value=None, properties=None, version=DCMsg.V_CURRENT):
		DCMsg.__init__(self, version)
		_PROPS.__init__(self, properties)
		assert isinstance(key, str)
		assert isinstance(sequence, int)

		self.key = key
		self.sequence = sequence
		self.uuid = '' if uuid is None else uuid
		self.value = '' if value is None else value

	@property
	def frames(self):
		return super().frames + [
				self.key.encode(),
				DCMsg._encode_int(self.sequence),
				self.uuid.encode(),
				self._encode_props(),
				self.value.encode()
			]

	@classmethod
	def from_msg(cls, ver, msg):
		# make sure we have two frames
		assert isinstance(msg, list)
		assert 5 == len(msg)

		key = msg[0].decode()
		seq = DCMsg._decode_int(msg[1])
		uuid = msg[2].decode()
		props = _PROPS._decode_props(msg[3])
		val = msg[4].decode()

		return cls(key, seq, uuid, val, properties=props, version=ver)

def KVPUB(key, value):
	return CONFIG(key, 0, None, value, None)

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
				DCMsg._encode_int(self.errcode),
				self.errstr.encode()
			]

	@classmethod
	def from_msg(cls, ver, msg):
		# make sure we have either two or three frames
		assert isinstance(msg, list)
		assert len(msg) in [1, 2]

		code = DCMsg._decode_int(msg[0])
		errstr = ''
		if len(msg) == 2:
			errstr = msg[1].decode()
		return cls(code, errstr, version=ver)
