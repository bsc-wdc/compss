import gc
import unittest
import numpy as np
from C import C
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_wait_on
from pycompss.api.local import local

@task(v=IN, returns=list)
def inc(v):
    return [x+1 for x in v]

@task(v=INOUT)
def append_three_ones(v):
    v += [1, 1, 1]

@task(m=IN, returns=list)
def square(m):
    return m**2

@task(v=IN, returns=list)
def normalize(m):
    return m / np.linalg.norm(m)

@task(d=INOUT)
def merge_dict(d):
    sub_dict = d['dict']
    d.update(sub_dict)
    del d['dict']

@local
def remove_trump(d):
    del d['trump']

@local
def solve_equation_system(A, b):
    return np.linalg.solve(A, b)

@local
def scale_vector(v, k):
    return [k*x for x in v]

@local
def abuse_me(positional, positional_default=3, *args, **kwargs):
    return (positional, positional_default, len(args), len(kwargs))

class testLocal(unittest.TestCase):

    def testFunctionalUsage(self):
        aa = [0, 0, 0]
        bb = inc(aa)
        # scale_vector should sync u
        # if it doesn't, then the program will
        # crash
        bb = compss_wait_on(bb)
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
        self.assertEqual([1, 1, 1], v)
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

    def testClassAttributes(self):
        my_C = C()
        my_C.vector = []
        append_three_ones(my_C.vector)
        my_C.vector = scale_vector(my_C.vector, 2)
        # is the class attribute updated?
        self.assertEqual([2, 2, 2], my_C.vector)

    def testMultipleReferences(self):
        original_object = [[]]
        reference_1 = original_object
        reference_2 = original_object
        reference_3 = [original_object]
        my_C = C()
        my_C.vector = reference_2
        reference_4 = my_C.vector
        append_three_ones(original_object[0])
        original_object[0] = scale_vector(original_object[0], 2)
        # are all these references updated?
        self.assertEqual([2, 2, 2], original_object[0])
        self.assertEqual([2, 2, 2], reference_1[0])
        self.assertEqual([2, 2, 2], reference_2[0])
        self.assertEqual([2, 2, 2], reference_3[0][0])
        self.assertEqual([2, 2, 2], my_C.vector[0])
        self.assertEqual([2, 2, 2], reference_4[0])

    '''
        These tests simply try to invoke complex use cases.
    '''
    def testComplexDataStructures1(self):
        original_object = {
            'dog': 'woof',
            13: 37,
            'dict': {
                'sheep': 'animal',
                'cactus': 'plant',
                'trump': 'unknown'
            }
        }
        # lets move the contents of the sub dictionary 'dict' to
        # the main dictionary and then, remove the item with key 'trump'
        merge_dict(original_object)
        remove_trump(original_object)
        self.assertEqual({
            'dog': 'woof',
            13: 37,
            'sheep': 'animal',
            'cactus': 'plant'},
            original_object);
    """
    @unittest.skip("Speed test -> ignoring")
    def testHugeObjects(self):
        N = 5000
        A = square(np.eye(N)*np.random.rand(N))
        b = normalize(np.random.rand(N))
        x = solve_equation_system(A, b)
        # well... does it works if the objects are huge?
        self.assertTrue(np.allclose(np.dot(A, x), b))
    """
