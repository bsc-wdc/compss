#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.parameter import *
import types

# #### DECORATORS #####
from decorator import decorator
import time

# #### SERIALIZER #####
import sys

if sys.version_info >= (3, 0):
    import _pickle as cPickle
else:
    import cPickle


# UNSUPPORTED WITH MYPY!  - DECORATORS MUST HAVE EXPLICIT SPECIFICATION
# @decorator  # MANDATORY TO PRESERVE THE ARGSPEC
# def timeit(func, *a, **k):
#     ts = time.time()
#     result = func(*a, **k)
#     te = time.time()
#     return [result, 'Elapsed time: %2.6f sec' % (te - ts)]

from pycompss.functions.elapsed_time import timeit


# #### FORMULAS #####
# Need to be on a different file

def formula1(n):
    return n * n


def formula2(n):
    x = formula1(n)
    return x * n


# #### CLASS #####
# Need to be on a different file

class MyClass(object):
    static_field = 'value of static field'

    # def __new__(self,v):  # Bug to be checked --- multiple parameters --- really needed? # raises when using bigfloat
    #     v=v+1
    #     return super(MyClass, self).__new__(self)

    def __init__(self, field=None):  # def __init__(self, *args, **kwargs):
        self.field = field
        self.v = 0

    @task()
    def instance_method(self):
        print("TEST")
        print("- Instance method")
        print("- Callee object:", self.field)
        self.field = self.field * 2

    @task(target_direction=IN)
    def instance_method_nonmodifier(self):
        print("TEST")
        print("- Instance method (nonmodifier)")
        print("- Callee object:", self.field)

    @classmethod
    @task()
    def class_method(cls):
        print("TEST")
        print("- Class method of class", cls)
        print("- Static field:", cls.static_field)

    @staticmethod
    @task(returns=int)
    def static_method(a, b, c):
        print("TEST")
        print("- Static method of MyClass")
        return (a + b) * c

    @task(returns=int)
    def return_value_square(self, v):
        print("TEST")
        print("self.field: ", self.field)
        print("- Return value square")
        print("- Input value:", v)
        self.v += v
        print("- Self.v value: ", self.v)
        o = v * v
        print("- Output value:", o)
        return o


# #### FUNCTIONS #####

@task()
def empty_task():
    print("EMPTY_TASK")


@task()
def function_primitives(i, l, f, b, s):
    print("TEST")
    print("- Static Function")
    print("- Primitive params: %d, %ld, %f, %d, %s" % (i, l, f, b, s))


# @task(fin=Parameter(p_type=Type.FILE),
#       finout=Parameter(p_type=Type.FILE, p_direction=Direction.INOUT),
#       fout=Parameter(p_type=Type.FILE, p_direction=Direction.OUT))
@task(fin=FILE, finout=FILE_INOUT, fout=FILE_OUT)
def function_files(fin, finout, fout):
    print("TEST")
    print("- Static Function")

    fin_d = open(fin, 'r')
    finout_d = open(finout, 'r+')
    fout_d = open(fout, 'w')

    print("- In file content:\n", fin_d.read())
    print("- Inout file content:\n", finout_d.read())
    finout_d.write("\n===> INOUT FILE ADDED CONTENT")
    fout_d.write("OUT FILE CONTENT")

    fin_d.close()
    finout_d.close()
    fout_d.close()


def par_func():
    print("- Function")


# @task(o = Parameter(p_direction = Direction.INOUT))
@task(o=INOUT, li=INOUT, dic=INOUT, tup=INOUT, cplx=INOUT)
def function_objects(o, li, dic, tup, cplx, f):
    print("TEST")
    print("- Static Function")
    print("- MyClass object", o.field)
    print("- List", li)
    print("- Dictionary", dic)
    print("- Tuple", tup)
    print("- Complex", cplx)

    valuesDic = [("key3", "value3"), ("key4", "value4")]

    o.field = o.field * 2
    li.append(2)
    dic[valuesDic[len(dic) - 2][0]] = valuesDic[len(dic) - 2][1]

    # This can not be done since it creates a new object with a new reference
    # and we keep the existing before the task execution
    # Moreover, tuples are inmutable
    # tup = list(tup)
    # tup.append('d')
    # tup = tuple(tup)

    # The same happens with the following assignment.
    # The resulting cplx has different id than the used as parameter
    # The operations over cplx return a different object instead of modifying
    # the existing.
    # cplx += cplx


@task(o=OUT)
def function_out_object(o):
    print("TEST")
    print("- Out object")
    print("- MyClass object received: " + str(o.field))
    o.field = 100
    print("- MyClass object final: " + str(o.field))


@task(returns=int)
def function_return_primitive(i):
    print("TEST")
    print("- Static Function")
    print("\t- Parameter:", i)
    return i * 2


@task(returns=MyClass)
def function_return_object(i):
    o = MyClass(i)
    print("TEST")
    print("- Static Function")
    print("\t- Parameter:", o)
    return o


@task(returns=int)
def function_function_parameter(f, v):
    out = f(v)
    print("TEST")
    print("- Function as a parameter: %s" % f)
    return out


@task(returns="{{in_param}}")
def function_return_in_param(in_param):
    print("A== ", in_param)
    return in_param * 2


@task(returns=int)
def function_default_parameter_values(x=100):
    print("TEST")
    print("- Default parameter values (must be 100)")
    print("\t- Value: %s" % x)
    return x


@task(returns=int)
def function_order_parameters(x, y, z=100, w=1000):
    print("TEST")
    print("- Function order parameters.")
    print("\t- Parameter x: %s" % x)
    print("\t- Parameter y: %s" % y)
    print("\t- Parameter z: %s" % z)
    print("\t- Parameter w: %s" % w)
    return x + y + z * w


@task(returns=int)
def function_fu_object(x):
    return x + x


@task(returns=int)
def function_fu_in_task(x):
    return x + x


@task(returns=list)
def function_fu_list_object(l):
    for i in range(len(l)):
        l[i] = l[i] + 1
    return l


@task(returns=list)
def function_fu_list_in_task(l):
    for i in range(len(l)):
        l[i] = l[i] + 1
    return l


@task(returns=int)
def function_iterable_object_wait(x):
    print("TEST")
    print("- Function iterable object wait.")
    print("\t- Parameter x: %s" % x)
    return x * x


# @task(returns = basestring)  # just for python 2
@task(returns=str)
def function_wait_on_string(s):
    print("TEST")
    print("- Function wait_on string.")
    print("\t- Parameter s: %s" % s)
    return s.upper()


@timeit()
@task(returns=int)
def function_time_decorated_master(x):
    print("TEST")
    print("- Function master time (decorated).")
    print("\t- Parameter x: %s" % x)
    return x * x * x


@task(returns=int)
@timeit()
def function_time_decorated_worker(x):
    print("TEST")
    print("- Function worker time (decorated).")
    print("\t- Parameter x: %s" % x)
    return x * x * x


@task(returns=int)
def function_argfunc(f, v):
    print("TEST")
    print("- Function passed as parameter")
    print("Function: ", f)
    value = f(v)
    return value


@task(returns=int)
def function_lambda(f, v):
    print("TEST")
    print("- Function lambda passed as parameter")
    print("Lambda: ", f)
    value = f(v)
    return value


@task(returns=int)
def function_generator(g):
    print("TEST")
    print("- Function generator passed as parameter")
    print(g)
    import sys
    if sys.version_info >= (3, 0):
        out = next(g)
    else:
        out = g.next()
    print("Generator value: ", out)
    return out


@task(returns=types.LambdaType)
def function_lambda_return():
    return lambda x: x ** 2 + 2 * x - 5


@task(returns=types.GeneratorType)
def function_generator_return(g):
    print("TEST")
    print("- Function return a generator")
    import sys
    if sys.version_info >= (3, 0):
        print("Generator value: ", next(g))
    else:
        print("Generator value: ", g.next())
    return g


@task(returns=(float, float))
def multireturn(value):
    print("Value: ", value)
    print("Type:  ", type(value))
    return value, value * 2


@task(returns=float)
def power(value):
    print("Pow value: ", value)
    print("Result: ", value * value)
    return value * value


@task(returns=float)
def merge(v1, v2):
    print("Merge")
    print("v1: ", v1)
    print("v2: ", v2)
    print("Result: ", v1 + v2)
    return v1 + v2


class Foo(object):

    def __init__(self):
        self.mod = cPickle
        self.value = 0

    def set(self, v):
        self.value = v

    def get(self):
        return self.value


@task(returns=object)
def function_moduleObject(value):
    print("Test Module Object")
    v = value.get()
    print("Value before: ", v)
    print("Value set to: ", v * v)
    value.set(v * v)
    return value


@task(returns=list)
def create_block(b_size):
    import numpy as np
    block = np.array(np.random.random((b_size, b_size)), dtype=np.double, copy=False)
    mb = np.matrix(block, dtype=np.double, copy=False)
    return mb


@task(x=INOUT)
def update_block(x, y):
    print("Before the sum:")
    print("x:")
    print(x)
    print("y:")
    print(y)
    print("id: " + str(id(x)))
    x += y
    print("After the sum of x and y:")
    print(x)
    print("id: " + str(id(x)))


@task(returns=str)
def empty_string(s):
    return "XX" + s + "XX"


@task(returns=int)
def char_to_int(mychar):
    return ord(mychar)


@task(returns=1)
def numpy_obj_creator():
    import numbers
    import numpy as np
    value = np.random.rand(10).max()
    assert isinstance(value, numbers.Number) == True
    print(type(value))  # <type 'numpy.float64'>
    return value


@task(returns=1)
def mod_task(o):
    o.append(3)
    return o


@task(returns=1)
def pass_task(o):
    return o


@task(returns=list, x=IN_DELETE)
def increment_object_delete(x):
    return [x[0] + 1]


@task(x=IN_DELETE)
def modify_obj(x):
    return x.get_test() + " adeu"


class mod_class(object):

    def __init__(self):
        self.a = 1
        self.b = [1, 2]

    def increment(self):
        self.a = self.a + 1

    def increase(self):
        self.b.append(3)

    def __repr__(self):
        # We only need to return the representation of the objects
        # whose modification does not vary the object size.
        return "Internal a: " + str(self.a)
        # There is no need to show b because we are increasing the size
        # and we want to evaluate that it checks correctly.
        # IMPORTANT: Any modification within the b list will not be seen unless
        # included in the __repr__
