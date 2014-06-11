#!/usr/bin/env python3

from unittest import TestCase, main

from dcamp.types.specs import EndpntSpec
from dcamp.types.messages.data import *


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


class TestAggregateData(TestCase):
    def setUp(self):
        self.time1 = 1384321742000
        self.time2 = 1384321842000

        self.d1 = [
            # calc: 100
            DataAverage(
                EndpntSpec('local', 9091),
                {'type': 'average'},
                time=self.time1,
                value=182,
                base_value=2),
            DataAverage(
                EndpntSpec('local', 9091),
                {'type': 'average'},
                time=self.time2,
                value=282,
                base_value=3),
        ]

        self.d2 = [
            # calc: 1
            DataAverage(
                EndpntSpec('local', 9092),
                {'type': 'average'},
                time=self.time1,
                value=490,
                base_value=5),
            DataAverage(
                EndpntSpec('local', 9092),
                {'type': 'average'},
                time=self.time2,
                value=491,
                base_value=6),
        ]

        self.d3 = [
            # calc: 120
            DataAverage(
                EndpntSpec('local', 9093),
                {'type': 'average'},
                time=self.time1,
                value=69,
                base_value=3),
            DataAverage(
                EndpntSpec('local', 9093),
                {'type': 'average'},
                time=self.time2,
                value=669,
                base_value=8),
        ]

        self.sum_aggr = DataAggregate(
            EndpntSpec('local', 9096),
            {
                'type': 'aggregate-sum',
                'aggr-id': 'sum-aggr',

                'is-final': True,
                'samples-type': 'average',
                'node-cnt': 3,
                'aggr-source': EndpntSpec('local', 9096),
            },
            time=self.time1,
            value=221.0,
        )

        self.avg_aggr = DataAggregate(
            EndpntSpec('local', 9096),
            {
                'type': 'aggregate-avg',
                'aggr-id': 'avg-aggr',

                'is-final': True,
                'samples-type': 'average',
                'node-cnt': 3,
                'aggr-source': EndpntSpec('local', 9096),
            },
            time=self.time1,
            value=(221 / 3),
        )

    def add_samples(self, aggr):
        aggr.add_sample(self.d1[0])
        aggr.add_sample(self.d2[0])
        aggr.add_sample(self.d3[0])

        aggr.add_sample(self.d1[1])
        aggr.add_sample(self.d2[1])
        aggr.add_sample(self.d3[1])

    def test_marshal(self):
        self.assertEqual(self.sum_aggr, Data.from_msg(self.sum_aggr.frames, None))

    def test_max(self):
        a = DataAggregate(
            EndpntSpec('local', 9096),
            {
                'type': 'aggregate-max',
                'aggr-id': 'min-aggr',
            },
        )
        self.add_samples(a)
        self.assertEqual(a.aggregate(self.time1 + 500), 120.0)

    def test_min(self):
        a = DataAggregate(
            EndpntSpec('local', 9096),
            {
                'type': 'aggregate-min',
                'aggr-id': 'max-aggr',
            },
        )
        self.add_samples(a)
        self.assertEqual(a.aggregate(1384321782000), 1)

    def test_avg(self):

        a = DataAggregate(
            EndpntSpec('local', 9096),
            {
                'type': 'aggregate-avg',
                'aggr-id': 'avg-aggr',
            },
        )

        self.add_samples(a)
        a.aggregate(time=self.time1)
        self.assertEqual(a.value, self.avg_aggr.value)

    def test_sum(self):

        a = DataAggregate(
            EndpntSpec('local', 9096),
            {
                'type': 'aggregate-sum',
                'aggr-id': 'sum-aggr',
            },
        )

        pre_sum = DataAggregate(
            EndpntSpec('local', 9096),
            {
                'type': 'aggregate-sum',
                'aggr-id': 'sum-aggr',
            },
        )

        self.assertEqual(a, pre_sum)
        self.assertIsNone(a.aggregate(self.time1))
        self.assertEqual(a, pre_sum)

        self.add_samples(a)
        a.aggregate(self.time1)
        self.assertEqual(a, self.sum_aggr)

        a.reset()
        pre_sum['samples-type'] = 'average'
        self.assertEqual(a, pre_sum)

        self.sum_aggr.reset()
        self.assertEqual(a, self.sum_aggr)

if __name__ == '__main__':
    main()
