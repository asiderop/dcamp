#!/usr/bin/env python3

from unittest import TestCase

from dcamp.data.config import EndpntSpec

class TestEndpntSpec(TestCase):
	def setUp(self):
		self.e1 = EndpntSpec("localhost", 1000)

	def test_comparison(self):
		self.assertTrue(self.e1 == eval(repr(self.e1)))

		e2 = EndpntSpec("localhost", 1000)
		self.assertTrue(self.e1 == e2)
		self.assertFalse(self.e1 < e2)

		e3 = EndpntSpec.from_str("localhost:1000")
		self.assertTrue(self.e1 == e3)

	def test_ports(self):
		self.assertTrue(self.e1.port() == self.e1.port(EndpntSpec.TOPO_BASE))
