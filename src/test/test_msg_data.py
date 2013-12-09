#!/usr/bin/env python3

from unittest import TestCase
from time import sleep

from dcamp.types.specs import EndpntSpec
from dcamp.types.messages.data import DATA_AVERAGE

class TestDATA(TestCase):
	def setUp(self):
		self.time1 = 1384321742000
		self.time2 = self.time1 + 6000
		self.time_diff = (self.time2 - self.time1) / 1000
		self.d1 = DATA_AVERAGE(
				EndpntSpec('local', 9090),
				{ 'type': 'average' },
				time1 = self.time1,
				value1 = 182,
				time2 = self.time2,
				value2 = 2,
			)

	def test_log_str(self):
		expected = '%d\tlocal:9090\tNone\t91.00 for %d.00 sec' % (self.time1, self.time_diff)
		self.assertEqual(expected, self.d1.log_str())

	def test_str(self):
		expected = 'local:9090 -- None @ %d = 91.00' % (self.time1)
		self.assertEqual(expected, str(self.d1))

if __name__ == '__main__':
	main()
