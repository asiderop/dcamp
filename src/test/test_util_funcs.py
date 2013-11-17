#!/usr/bin/env python3

from unittest import TestCase, main

import dcamp.util.functions as Util

class TestUtilFunctions(TestCase):

	def test_format_bytes(self):
		b = 9876

		# both num and suffix
		self.assertEqual('9.64Kb', Util.format_bytes(b))
		self.assertEqual('9.64 Kilobytes', Util.format_bytes(b, use_short=False))
		self.assertEqual(Util.format_bytes(b), Util.format_bytes(b, use_short=True, num_or_suffix='both'))

		# just num
		self.assertEqual(b/1024, Util.format_bytes(b, num_or_suffix='num'))

		# just suffix
		self.assertEqual('Kb', Util.format_bytes(b, num_or_suffix='suffix'))
		self.assertEqual('Kilobytes', Util.format_bytes(b, use_short=False, num_or_suffix='suffix'))

		self.assertEqual('Kb', Util.format_bytes(1024, num_or_suffix='suffix'))
		self.assertEqual('Kilobyte', Util.format_bytes(1024, use_short=False, num_or_suffix='suffix'))

if __name__ == '__main__':
	main()
