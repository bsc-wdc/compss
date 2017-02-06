import unittest
import gc
from pycompss.api.task import task
from pycompss.api.api import compss_wait_on
from pycompss.api.localtask import localtask
from pycompss.api.parameter import *

@task(v=IN, returns=list)
def inc(v):
    return [x+1 for x in v]

@task(v=INOUT)
def append_three_ones(v):
    v += [1, 1, 1]

@localtask
def scale_vector(v, k):
    return [k*x for x in v]

@localtask
def abuse_me(positional, positional_default=3, *args, **kwargs):
    return (positional, positional_default, len(args), len(kwargs))

class testLocalTask(unittest.TestCase):

    def testFunctionalUsage(self):
        aa = [0, 0, 0]
        bb = inc(aa)
        # scale_vector should sync u
        # if it doesn't, then the program will
        # crash
        print bb
        bb = compss_wait_on(bb)
        print bb
        w = scale_vector(bb, 2)
        self.assertEqual([2, 2, 2], w)

    def testInoutUsage(self):
        v = []
        append_three_ones(v)
        # note that v is not a future object, but
        # it must be synced, too
        w = scale_vector(v, 2)
        self.assertEqual([2, 2, 2], w)

    def testSyncIsDoneOnlyOnce(self):
        v = []
        append_three_ones(v)
        w = scale_vector(v, 2)
        # this second call should not sync w
        # in fact, v should be equal to [1, 1, 1]
        self.assertEqual([1, 1, 1], v)
        w = scale_vector(v, 2)
        self.assertEqual([2, 2, 2], w)

    def testArgsKwargsAreRespected(self):
        ret_val = abuse_me(1, 3, 4, 5, 6, kwarg_1="hello", kwarg_2="world")
        # are arguments respected? (i.e: aren't we merging positional arguments)
        # with non-positional and/or keywords?
        self.assertEqual((1, 3, 3, 2), ret_val)


    def testArgsKwargsAreProperlySynced(self):
        v = []
        append_three_ones(v)
        u = [1, 2, 3]
        u = inc(u)
        w = [6, 5, 4]
        w = inc(w)
        ret_val = abuse_me(v, 3, u, vector_w=w)
        # can we synchronize and handle all
        # these values, even if they are
        # different types of arguments?
        self.assertEqual([2, 3, 4], u)
        self.assertEqual([7, 6, 5], w)
        self.assertEqual(([1, 1, 1], 3, 1, 1), ret_val)
