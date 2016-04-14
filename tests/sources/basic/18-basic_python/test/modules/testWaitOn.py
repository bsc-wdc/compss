import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *


class testWaitOn(unittest.TestCase):

    @task(returns=basestring)
    def function_wait_on_string(self, s):
        return s.upper()

    @task(returns=int)
    def function_iterable_object_wait(self, x):
        return x*x

    @task(returns=int)
    def function_nested_iterable_object_wait(self, x):
        return x*x

    def test_iterable_object_wait(self):
        """ Test iterable object """
        from pycompss.api.api import compss_wait_on
        iobj = [i for i in xrange(10)]

        # todo two test?
        # full modification
        robj = [i*i for i in xrange(10)]
        iobj = [self.function_iterable_object_wait(iobj[i]) for i in xrange(10)]
        iobj = compss_wait_on(iobj)
        self.assertSequenceEqual(iobj, robj, "Full modification: Iterable object a is not equal to b. a={}, b={}".format(iobj, robj))

        # partial modification
        iobj = [i for i in xrange(10)]
        robj = [i for i in xrange(10)]
        for i in xrange(0, 10, 2):
            iobj[i] = self.function_iterable_object_wait(iobj[i])
            robj[i] = i*i
        iobj = compss_wait_on(iobj)
        self.assertSequenceEqual(iobj, robj, "Partial modification: Iterable object a is not equal to b. a={}, b={}".format(iobj, robj))

    def test_nested_iterable_object_wait(self):
        """ Test nested iterable object wait """
        from pycompss.api.api import compss_wait_on
        iobj = [[1, 2, 3], [4, 5, 6]]
        base = [[1, 2, 3], [4, 5, 6]]

        # full modification
        for i in xrange(len(iobj)):
            for j in xrange(len(iobj[i])):
                iobj[i][j] = self.function_nested_iterable_object_wait(iobj[i][j])
                base[i][j] = base[i][j] * base[i][j]
        iobj = map(compss_wait_on, iobj)  # no esta en la ova la espera para listas de listas
        self.assertSequenceEqual(iobj, base, "Full modification: iterable object a is not equal to b. a={} b={}".format(iobj, base))

        iobj = [[1, 2, 3], [4, 5, 6]]
        base = [[1, 2, 3], [4, 5, 6]]
        # partial modification
        for i in xrange(len(iobj[1])):
            iobj[1][i] = self.function_nested_iterable_object_wait(iobj[1][i])
            base[1][i] = base[1][i] * base[1][i]
        iobj = map(compss_wait_on, iobj)  # no esta en la ova la espera para listas de listas
        self.assertSequenceEqual(iobj, base, "Partial modification: iterable object a is not equal to b. a={} b={}".format(iobj, base))

    def test_wait_on_string(self):
        """ Test wait on string"""
        from pycompss.api.api import compss_wait_on
        s = "helloworld"
        o = self.function_wait_on_string(s)
        o = compss_wait_on(o)
        #print (o, s.upper())
        self.assertEqual(o, s.upper(), "strings are not equal: {}, {}".format(s, o))
