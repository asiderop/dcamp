'''
dCAMP message module
'''
import logging, struct

# zmq.jsonapi ensures bytes, instead of unicode:
import zmq.utils.jsonapi as jsonapi
from zmq import DEALER, ROUTER

from dcamp.types.specs import SerializableSpecTypes, EndpntSpec

# TODO: use a new log level instead
verbose_debug = True

class DCMsg(object):
	'''
	Base dCAMP message
	'''

	logger = logging.getLogger("dcamp.dcmsg")

	def __init__(self, peer_id=None):
		self._peer_id = peer_id

	def __iter__(self):
		return iter(self.frames)

	def __str__(self):
		result = ''
		for (count, f) in enumerate(self.frames):
			result += 'Frame %d: %s\n' % (count, f.decode())
		return result[:len(result)-1] # drop the trailing new-line

	@property
	def name(self):
		return self.__class__.__name__

	@property
	def is_error(self):
		return isinstance(self, WTF)

	@property
	def frames(self):
		raise NotImplementedError('subclass must implement method')

	@classmethod
	def from_msg(cls, msg):
		raise NotImplementedError('subclass must implement method')

	@staticmethod
	def _encode_int(val):
		# pack as 8-byte int using network order
		return struct.pack('!q', val)

	@staticmethod
	def _decode_int(buffer):
		# unpack as 8-byte int using network order
		return struct.unpack('!q', buffer)[0]

	def send(self, socket):
		self.logger.debug('S:%s' % (self.name))
		if verbose_debug:
			for part in str(self).split('\n'):
				self.logger.debug('  '+ part)

		parts = self.frames
		if DEALER == socket.socket_type:
			parts.insert(0, b'') # DEALER needs empty first frame (i.e. delimiter)
		elif ROUTER == socket.socket_type:
			assert self._peer_id is not None
			parts.insert(0, self._peer_id) # ROUTER needs peer identity in first frame

		socket.send_multipart(parts)

	@classmethod
	def recv(cls, socket):
		frames = socket.recv_multipart()

		peer_id = None
		if ROUTER == socket.socket_type:
			peer_id = frames.pop(0) # first frame from ROUTER is peer identity
			assert b'' == frames.pop(0) # second frame is empty (i.e. delimiter)

		try:
			# try to decode message with given class
			msg = cls.from_msg(frames)
		except (ValueError, struct.error) as e:
			try:
				# otherwise, try decoding WTF message
				msg = WTF.from_msg(frames)
			except:
				# finally, return WTF with original error string
				msg = WTF(1, str(e))

		msg._peer_id = peer_id

		cls.logger.debug('R:%s' % (msg.name))
		if verbose_debug:
			for part in str(msg).split('\n'):
				cls.logger.debug('  '+ part)

		return msg

class _PROPS(object):
	''' helper class for messages with property dictionaries '''
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

	@staticmethod
	def _encode_dict(given):
		# { key : [ (value-type-name, value), ... ] }
		result = dict()
		for (key, value) in given.items():
			if type(value) == list:
				new_list = list()
				for val in value:
					new_list.append(_PROPS.__type_tuple_from_value(val))
				result[key] = new_list
			else:
				result[key] = _PROPS.__type_tuple_from_value(value)

		return jsonapi.dumps(result)

	@staticmethod
	def _decode_dict(given):
		# unpack the json string into actual data types
		result = dict()
		decoded = jsonapi.loads(given)
		for (key, value_list) in decoded.items():
			if type(value_list) != list:
				raise ValueError('expected json list but found ' % type(value_list))

			# each element will either be a list (a single tuple)...
			if len(value_list) == 2 and type(value_list[0]) != list:
				result[key] = _PROPS.__value_from_type_tuple(value_list)

			# ...or a list of lists (tuples)
			else:
				for value in value_list:
					if type(value) != list:
						raise ValueError('expected json list but found ' % type(value))
					new_list = list()
					for val in value:
						new_list.append(_PROPS.__value_from_type_tuple(value))
					result[key] = new_list
		return result

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

class WTF(DCMsg):
	def __init__(self, errcode, errstr=''):
		DCMsg.__init__(self)
		assert isinstance(errcode, int)
		assert isinstance(errstr, str)
		self.errcode = errcode
		self.errstr = errstr

	def __str__(self):
		return 'WTF(%d): %s' % (self.errcode, self.errstr)

	@property
	def frames(self):
		return [
				DCMsg._encode_int(self.errcode),
				self.errstr.encode(),
			]

	@classmethod
	def from_msg(cls, msg):
		# make sure we have either two or three frames
		assert isinstance(msg, list)

		if len(msg) not in [1, 2]:
			raise ValueError('wrong number of frames')

		code = DCMsg._decode_int(msg[0])
		errstr = ''
		if len(msg) == 2:
			errstr = msg[1].decode()
		return cls(code, errstr)
