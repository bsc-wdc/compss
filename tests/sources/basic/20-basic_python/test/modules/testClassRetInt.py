import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from test.modules.MyClass import MyClassRetInt as MyClass


def par_func():
    return "function"


class testClassRetInt(unittest.TestCase):

    # WARNING!!! WHEN DOING THIS, THE FUTURE OBJECT BUILT WILL BE OF 'OBJECT'
    # TYPE, ANT THE INTERNAL FUNCTIONS WILL NOT BE AVAILABLE.
    @task(returns=1)
    def function_return_object(self, i):
        o = MyClass(i)
        return o

    def test_class_method(self):
        """ Test class method """
        from pycompss.api.api import compss_wait_on
        s = MyClass.class_method()
        s = compss_wait_on(s)
        res = 'value of static field'
        self.assertEqual(s, res)

    def test_instance_method(self):
        """ Test instance method """
        from pycompss.api.api import compss_wait_on
        # todo two test: modifier, no modifier
        val = 1
        o = MyClass(val)
        o.instance_method()
        res = o.instance_method_nonmodifier()
        o.instance_method()
        res = compss_wait_on(res)
        o = compss_wait_on(o)
        self.assertEqual(res, 2)
        self.assertEqual(o.field, val*4)

    @unittest.skip("UNSUPPORTED FUNCTION - Can not use the return statement with an integer and expect the apporpiate future object type.")
    def test_function_return_object(self):
        """ Test function return object"""
        from pycompss.api.api import compss_wait_on
        val = 1
        o = self.function_return_object(val) # When calling this function it will retrieve a "object" future element
        o.instance_method()                  # An consequently, this function will not be available ==> throwing AttributeError.
        o = compss_wait_on(o)
        self.assertEqual(o.field, val*2)
