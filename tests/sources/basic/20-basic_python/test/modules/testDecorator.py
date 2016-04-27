import unittest
import time
from pycompss.api.task import task
from pycompss.api.parameter import *

from decorator import decorator


class testDecorator(unittest.TestCase):

    def setUp(self):
        self.x = 2

    @decorator
    def timeit(func, *a, **k):
        ts = time.time()
        result = func(*a, **k)
        te = time.time()
        return [result, (te - ts)]

    @timeit
    @task(returns=int)
    def function_time_decorated_master(self, x):
        return x**3

    @task(returns=int)
    @timeit
    def function_time_decorated_worker(self, x):
        return x**3

    def test_decorator_master(self):
        """ Test time decorator master"""
        from pycompss.api.api import compss_wait_on
        o = self.function_time_decorated_master(self.x)
        o = compss_wait_on(o)
        res = o[0]
        time = o[1]
        #print '(master time) {}'.format(time)
        self.assertEqual(res, self.x**3)

    def test_decorator_worker(self):
        """ Test time decorator worker"""
        from pycompss.api.api import compss_wait_on
        o = self.function_time_decorated_worker(self.x)
        o = compss_wait_on(o)
        res = o[0]
        time = o[1]
        #print '(worker time) {}'.format(time)
        self.assertEqual(res, self.x**3)

    def test_decorator_worker_list(self):
        """ Test time decorator with list worker"""
        from pycompss.api.api import compss_wait_on
        tasks = [self.function_time_decorated_worker(self.x) for _ in xrange(5)]
        tasks = compss_wait_on(tasks)
        values = [t[0] for t in tasks]
        times = [t[1] for t in tasks]
        #print '(master times) {}'.format(times)
        self.assertSequenceEqual(values, [self.x**3 for _ in range(5)])

    def test_decorator_master_list(self):
        """ Test time decorator with list master"""
        from pycompss.api.api import compss_wait_on
        tasks = [self.function_time_decorated_master(self.x) for _ in xrange(5)]
        values = [t[0] for t in tasks]
        times = [t[1] for t in tasks]
        values = compss_wait_on(values)
        #print '(worker times) {}'.format(times)
        self.assertSequenceEqual(values, [self.x**3 for _ in range(5)])

    def tearDown(self):
        self.x = None
