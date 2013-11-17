#!/usr/bin/env python3

from unittest import TestCase, main

from dcamp.types.specs import EndpntSpec, MetricSpec, ThreshSpec

class TestEndpntSpec(TestCase):
	def setUp(self):
		self.e1 = EndpntSpec("localhost", 1000)

	def test_to_from(self):
		self.assertEqual(self.e1, eval(repr(self.e1)))
		self.assertEqual(self.e1, EndpntSpec.from_str(str(self.e1)))
		self.assertEqual(self.e1, EndpntSpec.from_str("localhost:1000"))

	def test_comparison(self):
		e2 = EndpntSpec("localhost", 1000)
		self.assertEqual(self.e1, e2)
		self.assertFalse(self.e1 < e2)

class TestThreshSpec(TestCase):
	def setUp(self):
		self.t1 = ThreshSpec('<', 99)

	def test_to_from(self):
		self.assertEqual(self.t1, eval(repr(self.t1)))
		self.assertEqual(self.t1, ThreshSpec.from_str(str(self.t1)))
		self.assertEqual(self.t1, ThreshSpec.from_str('<99'))

	def test_comparison(self):
		t2 = ThreshSpec('>', 80)
		self.assertNotEqual(self.t1, t2)
		t3 = ThreshSpec('<', 99.0)
		self.assertEqual(self.t1, t3)

if __name__ == '__main__':
	main()
