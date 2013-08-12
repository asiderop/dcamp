'''
dCAMP message module
'''
import logging

from dcamp.types.messages.common import DCMsg, _PROPS

class CONFIG(DCMsg, _PROPS):
	def __init__(self, key, sequence=0, uuid=None, value=None, properties=None):
		DCMsg.__init__(self)
		_PROPS.__init__(self, properties)
		assert isinstance(key, str)
		assert isinstance(sequence, int)

		self.key = key
		self.sequence = sequence
		self.uuid = '' if uuid is None else uuid
		self.value = '' if value is None else value

	def __str__(self):
		if self.key in ['ICANHAZ', 'KTHXBAI']:
			return '%s %s' % (self.key, self.value)
		elif 'HUGZ' == self.key:
			return self.key
		else:
			result = '#%d: %s = %s' % (self.sequence, self.key, self.value)
			if '' != self.uuid:
				result += ' (%s)' % self.uuid
			for (prop, val) in self.properties.items():
				result += '\n%s : %s' % (prop, val)
			return result

	@property
	def frames(self):
		return [
				self.key.encode(),
				DCMsg._encode_int(self.sequence),
				self.uuid.encode(),
				_PROPS._encode_dict(self.properties),
				self.value.encode(),
			]

	@classmethod
	def from_msg(cls, msg):
		assert isinstance(msg, list)

		# make sure we have five frames
		if 5 != len(msg):
			raise ValueError('wrong number of frames')

		key = msg[0].decode()
		seq = DCMsg._decode_int(msg[1])
		uuid = msg[2].decode()
		props = _PROPS._decode_dict(msg[3])
		val = msg[4].decode()

		return cls(key, seq, uuid, val, properties=props)

def ICANHAZ(subtree=None):
	return CONFIG(key='ICANHAZ', value=subtree)
def KVSYNC(k, v, seq):
	return CONFIG(key=k, sequence=seq, value=v)
def KTHXBAI(subtree=None):
	return CONFIG(key='KTHXBAI', value=subtree)
def KVPUB(k, v, seq, uuid=None):
	return CONFIG(key=k, sequence=seq, uuid=uuid, value=v)
def HUGZ(): # TODO: add sequence numbers to heartbeats?
	return CONFIG(key='HUGZ', sequence=0)
