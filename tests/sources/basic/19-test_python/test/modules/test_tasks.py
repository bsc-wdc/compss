"""
@author: etejedor
@author: fconejer

PyCOMPSs Testbench Tasks
========================
"""

from pycompss.api.task import task
from pycompss.api.parameter import *
import types

##### DECORATORS #####
from decorator import decorator
import time

@decorator  # MANDATORY TO PRESERVE THE ARGSPEC
def timeit(func, *a, **k):
    ts = time.time()
    result = func(*a, **k)
    te = time.time()
    return [result, 'Elapsed time: %2.6f sec' % (te-ts)]


##### FORMULAS #####
# Need to be on a different file

def formula1(n):
    return n*n

def formula2(n):
    x = formula1(n)
    return x*n


##### CLASS #####
# Need to be on a different file

class MyClass(object):

    static_field = 'value of static field'
    
    #def __new__(self,v):  # Bug to be checked --- multiple parameters --- really needed? # raises when using bigfloat
    #    v=v+1
    #    return super(MyClass, self).__new__(self)
    
    def __init__(self, field = None):   #def __init__(self, *args, **kwargs):
        self.field = field
        self.v = 0

    @task()
    def instance_method(self):
        print "TEST"
        print "- Instance method"
        print "- Callee object:", self.field
        self.field = self.field * 2

    @task(isModifier = False)
    def instance_method_nonmodifier(self):
        print "TEST"
        print "- Instance method (nonmodifier)"
        print "- Callee object:", self.field

    @classmethod
    @task()
    def class_method(cls):
        print "TEST"
        print "- Class method of class", cls
        print "- Static field:", cls.static_field

    @task(returns=int)
    def return_value_square(self, v):
        print "TEST"
        print "self.field: ", self.field
        print "- Return value square"   
        print "- Input value:", v
        self.v += v
        print "- Self.v value: ", self.v
        o=v*v
        print "- Output value:", o
        return o

    
##### FUNCTIONS #####

@task()
def function_primitives(i, l, f, b, s):
    print "TEST"
    print "- Static Function"
    print "- Primitive params: %d, %ld, %f, %d, %s" % (i, l, f, b, s)
    
#@task(fin    = Parameter(p_type = Type.FILE),
#      finout = Parameter(p_type = Type.FILE, p_direction = Direction.INOUT),
#      fout   = Parameter(p_type = Type.FILE, p_direction = Direction.OUT))
@task(fin = FILE, finout = FILE_INOUT, fout = FILE_OUT)
def function_files(fin, finout, fout):
    print "TEST"
    print "- Static Function"
    
    fin_d = open(fin, 'r')
    finout_d = open(finout, 'r+')
    fout_d = open(fout, 'w')
    
    print "- In file content:\n", fin_d.read()
    print "- Inout file content:\n", finout_d.read()
    finout_d.write("\n===> INOUT FILE ADDED CONTENT")
    fout_d.write("OUT FILE CONTENT")
    
    fin_d.close()
    finout_d.close()
    fout_d.close()

def par_func():
    print "- Function"

#@task(o = Parameter(p_direction = Direction.INOUT))
@task(o = INOUT)
def function_objects(o, l, dic, tup, cplx, f):
    print "TEST"
    print "- Static Function"
    print "- MyClass object", o.field
    print "- List", l
    print "- Dictionary", dic
    print "- Tuple", tup
    print "- Complex", cplx
    f()
    
    o.field = o.field * 2

@task(returns = int)
def function_return_primitive(i):
    print "TEST"
    print "- Static Function"
    print "\t- Parameter:", i
    return i * 2

@task(returns = MyClass)
def function_return_object(i):
    o = MyClass(i)
    print "TEST"
    print "- Static Function"
    print "\t- Parameter:", o
    return o



@task(returns = int)    
def function_function_parameter(f, v):
    out = f(v)
    print("TEST")
    print("- Function as a parameter: %s" % f)
    return out

@task(returns = int)
def function_default_parameter_values(x=100):
    print("TEST")
    print("- Default parameter values (must be 100)")
    print("\t- Value: %s" % x)
    return x

@task(returns = int)
def function_order_parameters(x,y,z=100,w=1000):
    print("TEST")
    print("- Function order parameters.")
    print("\t- Parameter x: %s" %x)
    print("\t- Parameter y: %s" %y)
    print("\t- Parameter z: %s" %z)
    print("\t- Parameter w: %s" %w)
    return x+y+(z*w)

@task(returns = int)
def function_fu_object(x):
    return x+x

@task(returns = int)
def function_fu_in_task(x):
    return x+x

@task(returns = list)
def function_fu_list_object(l):
    for i in xrange(len(l)):
        l[i] = l[i] + 1
    return l

@task(returns = list)
def function_fu_list_in_task(l):
    for i in xrange(len(l)):
        l[i] = l[i] + 1
    return l

@task(returns = int)
def function_iterable_object_wait(x):
    print("TEST")
    print("- Function iterable object wait.")
    print("\t- Parameter x: %s" %x)
    return x*x

@task(returns = basestring)
def function_wait_on_string(s):
    print("TEST")
    print("- Function wait_on string.")
    print("\t- Parameter s: %s" %s)
    return s.upper()

@timeit
@task(returns = int)
def function_time_decorated_master(x):
    print("TEST")
    print("- Function master time (decorated).")
    print("\t- Parameter x: %s" %x)
    return x*x*x

@task(returns = int)
@timeit
def function_time_decorated_worker(x):
    print("TEST")
    print("- Function worker time (decorated).")
    print("\t- Parameter x: %s" %x)
    return x*x*x

@task(returns = int)
def function_argfunc(f,v):
    print "TEST"
    print "- Function passed as parameter"
    print "Function: ", f
    value = f(v)
    return value

@task(returns = int)
def function_lambda(f, v):
    print "TEST" 
    print "- Function lambda passed as parameter"
    print "Lambda: ", f
    value = f(v)
    return value

@task(returns = int)
def function_generator(g):
    print "TEST"
    print "- Function generator passed as parameter"
    print g
    out = g.next()
    print "Generator value: ", out
    return out

@task(returns = types.LambdaType)
def function_lambda_return():
    return lambda x: x**2 + 2*x - 5

@task(returns = types.GeneratorType)
def function_generator_return(g):
    print "TEST"
    print "- Function return a generator"
    print "Generator value: ", g.next()
    return g

  
@task(returns = (float, float))
def multireturn(value):
    print "Value: ", value
    print "Type:  ", type(value)
    return value, value*2 

@task(returns = float)
def power(value):
    print "Pow value: ", value
    print "Result: ", value*value
    return value*value
    
@task(returns = float)
def merge(v1, v2):
    print "Merge"
    print "v1: ", v1
    print "v2: ", v2
    print "Result: ", v1+v2
    return v1+v2


import cPickle
class Foo(object):
    def __init__(self):
        self.mod=cPickle
        self.value = 0
    def set(self,v):
        self.value = v
    def get(self):
        return self.value

                                            
@task(returns = object)
def function_moduleObject(value):
    print "Test Module Object"
    v = value.get()
    print "Value before: ", v
    print "Value set to: ", v*v
    value.set(v*v)
    return value
                                                                    