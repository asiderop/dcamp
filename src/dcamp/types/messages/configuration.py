'''
dCAMP message module
'''
import logging

from dcamp.types.messages.common import DCMsg, _PROPS

class CONFIG(DCMsg, _PROPS):

	ICANHAZ = 0
	KVSYNCPUB = 1
	KTHXBAI = 2
	HUGZ = 3

	ctypes = [
		ICANHAZ,
		KVSYNCPUB,
		KTHXBAI,
		HUGZ,
	]

	def __init__(self, key, value=None, sequence=0, uuid=None, properties=None, peer_id=None):
		DCMsg.__init__(self, peer_id)
		_PROPS.__init__(self, properties)
		assert isinstance(key, str)
		assert isinstance(sequence, int)

		self.key = key
		self.sequence = sequence
		self.uuid = '' if uuid is None else uuid
		self.value = '' if value is None else value

		if 'ICANHAZ' == key:
			self.ctype = CONFIG.ICANHAZ
		elif 'KTHXBAI' == key:
			self.ctype = CONFIG.KTHXBAI
		elif 'HUGZ' == key:
			self.ctype = CONFIG.HUGZ
		else:
			# if not one of the "known" keys, assume sync/pub message
			self.ctype = CONFIG.KVSYNCPUB

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

		return cls(key, val, seq, uuid, properties=props)

def ICANHAZ(subtree=None):
	return CONFIG(key='ICANHAZ', value=subtree)

def KVSYNC(k, v, seq, pid):
	return CONFIG(key=k, value=v, sequence=seq, peer_id=pid)

def KTHXBAI(seq, pid, subtree=None):
	return CONFIG(key='KTHXBAI', value=subtree, sequence=seq, peer_id=pid)

def KVPUB(k, v, seq, uuid=None):
	return CONFIG(key=k, value=v, sequence=seq, uuid=uuid)

def HUGZ(): # TODO: add sequence numbers to heartbeats?
	return CONFIG(key='HUGZ', sequence=0)
