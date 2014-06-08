#!/usr/bin/env python3

from unittest import TestCase, main

from dcamp.types.specs import EndpntSpec
from dcamp.types.messages.data import DataAverage


class TestData(TestCase):
    def setUp(self):
        self.time = 1384321742000
        self.d1 = DataAverage(
            EndpntSpec('local', 9090),
            {'type': 'average'},
            time=self.time,
            value=182,
            base_value=2,
        )

    def test_log_str(self):
        expected = '%d\tlocal:9090\tNone\t182.00\t2.00' % self.time
        self.assertEqual(expected, self.d1.log_str())

    def test_str(self):
        expected = 'local:9090 -- None @ %d = 182.00 / 2.00' % self.time
        self.assertEqual(expected, str(self.d1))

    def test_marshal(self):
        self.assertEqual(self.d1, Data.from_msg(self.d1.frames, None))

if __name__ == '__main__':
    main()
