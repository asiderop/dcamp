'''
dCAMP message module
'''
import logging

from dcamp.types.messages.common import DCMsg
from dcamp.types.specs import EndpntSpec

class DATA(DCMsg):
	'''
	Frame 0: data source (leaf or collector node endpoint), as 0MQ string
	Frame 1: metric type or "HUGZ", as 0MQ string [ basic / sum / average / percent / rate]
	Frame 2: metric name/detail, as 0MQ string
	Frame 3: time t1 in ms epoch utc, 8 bytes in network order
	Frame 4: value v1, 8 bytes in network order
	Frame 5: time t2 in ms epoch utc, 8 bytes in network order
	Frame 6: value v2, 8 bytes in network order
	'''

	mtypes = [
		'HUGZ',
		'basic',
		'sum',
		'average',
		'percent',
		'rate',
	]

	def __init__(self, source, mtype, detail=None, time1=None, value1=None, time2=None, value2=None):
		DCMsg.__init__(self, peer_id)

		assert isinstance(source, EndpntSpec)
		assert mtype in DATA.mtypes
		assert isinstance(detail, (str, type(None)))

		assert isinstance(time1, (int, type(None)))
		assert isinstance(value1, (int, type(None)))
		assert isinstance(time2, (int, type(None)))
		assert isinstance(value2, (int, type(None)))

		# TODO: add more verifications of parameters based on given mtype

		self.source = source
		self.mtype = mtype
		self.detail = detail or ''

		self.time1 = time1
		self.value1 = value1
		self.time2 = time2
		self.value2 = value2

	def __str__(self):
		result = '%s -- %s = ' % (self.source, self.detail)
		if self.mtype in ['basic', 'sum']:
			result += '%d' % (self.value1)
		elif self.mtype in ['average', 'percent']:
			val = self.value1 / self.value2
			if 'percent' == self.mtype:
				val *= 100
			result += '%d' (val)
			if 'percent' == self.mtype:
				result += '%%'
			elif 'average' == self.mtype:
				result += ' averaged across %d ms' % (self.time2 - self.time1)
		elif self.mtype in ['rate']:
			result += '%d / ms' % ((self.value2 - self.value1) / (self.time2 - self.time1))

		return result

	@property
	def frames(self):
		return [
				self.source.encode(),
				self.mtype.encode(),
				self.detail.encode(),
				self.time1 is None and b'' or DCMsg._encode_int(self.time1),
				self.value1 is None and b'' or DCMsg._encode_int(self.value1),
				self.time2 is None and b'' or DCMsg._encode_int(self.time2),
				self.value2 is None and b'' or DCMsg._encode_int(self.value2),
			]

	@classmethod
	def from_msg(cls, msg):
		assert isinstance(msg, list)

		# make sure we have six frames
		if 7 != len(msg):
			raise ValueError('wrong number of frames')

		source = EndpntSpec.decode(msg[0])
		mtype = msg[1].decode()
		detail = msg[2].decode()

		time1 = len(msg[3]) == 0 and None or DCMsg._decode_int(msg[3])
		value1 = len(msg[4]) == 0 and None or DCMsg._decode_int(msg[4])
		time2 = len(msg[5]) == 0 and None or DCMsg._decode_int(msg[5])
		value2 = len(msg[6]) == 0 and None or DCMsg._decode_int(msg[6])

		return cls(source, mtype, detail, time1, value1, time2, value2)
