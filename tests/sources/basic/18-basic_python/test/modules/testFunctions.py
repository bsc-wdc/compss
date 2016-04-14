import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.functions.reduce import mergeReduce, simpleReduce
from pycompss.functions.map import map


@task(returns=int)
def sumTask(x, y):
    return x+y


class testFunctions(unittest.TestCase):

    def setUp(self):
        self.data = [1, 2, 3, 4]
        self.tuples = [(1, [4, 5, 6]), (2, [6, 7, 8]), (1, [1, 2, 3]), (2, [1, 1, 1])]
        self.dicts = dict(self.tuples)
        self.lambdaFunction = lambda x, y: x+y
        self.methodFunction = sumTask

    def test_mergeReduce_seq(self):
        res = mergeReduce(self.lambdaFunction, self.data)
        self.assertEqual(res, sum(self.data))

    def test_simpleReduce_seq(self):
        res = simpleReduce(self.lambdaFunction, self.data)
        self.assertEqual(res, sum(self.data))

    def test_simpleReduce(self):
        from pycompss.api.api import compss_wait_on
        res = simpleReduce(self.methodFunction, self.data)
        res = compss_wait_on(res)
        self.assertEqual(res, sum(self.data))

    def test_mergeReduce(self):
        from pycompss.api.api import compss_wait_on
        res = mergeReduce(self.methodFunction, self.data)
        res = compss_wait_on(res)
        self.assertEqual(res, sum(self.data))

    @unittest.skip("not implemented yet")
    def test_mergeReduceByClass_tuples(self):
        res = mergereduceByClass(self.methodFunction, self.tuples)
        val = [[4, 5, 6, 1, 2, 3], [6, 7, 8, 1, 1, 1]]
        self.assertEqual(res, val)

    @unittest.skip("not implemented yet")
    def test_mergeReduceByClass_dicts(self):
        res = mergereduceByClass(self.methodFunction, self.dicts)
        val = [[4, 5, 6, 1, 2, 3], [6, 7, 8, 1, 1, 1]]
        self.assertEqual(res, val)

    def test_map_seq(self):
        res = map(self.lambdaFunction, self.data, self.data)
        import __builtin__
        cor = __builtin__.map(self.lambdaFunction, self.data, self.data)
        self.assertSequenceEqual(res, cor)

    def test_map(self):
        from pycompss.api.api import compss_wait_on
        res = map(self.methodFunction, self.data, self.data)
        res = compss_wait_on(res)
        import __builtin__
        cor = __builtin__.map(self.methodFunction, self.data, self.data)
        cor = compss_wait_on(cor)
        self.assertSequenceEqual(res, cor)

    def tearDown(self):
        self.data = None
        self.tuples = None
        self.dicts = None
        self.lambdaFunction = None
        self.methodFunction = None
