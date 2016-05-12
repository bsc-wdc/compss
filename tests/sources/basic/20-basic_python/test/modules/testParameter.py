import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from test.modules.auxFunctions import formula2


class testParameter(unittest.TestCase):
    @task(returns=int)
    def function_function_parameter(self, f, v):
        out = f(v)
        return out

    @task(returns=int)
    def function_default_parameter_values(self, x=100):
        return x

    @task(returns=int)
    def function_order_parameters(self, x, y, z=100, w=1000):
        return x+y+(z*w)

    def test_function_as_parameter(self):
        """ Test function as parameter """
        from pycompss.api.api import compss_wait_on
        v = 2
        f = formula2
        o = self.function_function_parameter(f, v)
        o = compss_wait_on(o)
        self.assertEqual(o, v**3, "Function parameter is not send to the task")

    def test_default_parameters(self):
        """ Test default Parameters"""
        from pycompss.api.api import compss_wait_on
        o = self.function_default_parameter_values()
        o = compss_wait_on(o)
        self.assertEqual(o, 100, "Task not recognise default parameter")

    def test_order_parameters(self):
        """ Test order parameters """
        from pycompss.api.api import compss_wait_on
        res = [100003, 3004, 17, 20006, 4007, 808, 809, 810]
        a, b, c, d = (8, 2, 20, 40)
        o = []
        o.append(self.function_order_parameters(1, 2))
        o.append(self.function_order_parameters(2, 2, 3))
        o.append(self.function_order_parameters(3, 2, 3, 4))
        o.append(self.function_order_parameters(4, 2, z=20))
        o.append(self.function_order_parameters(5, 2, w=40))
        o.append(self.function_order_parameters(6, 2, w=40, z=20))
        o.append(self.function_order_parameters(z=20, x=7, w=40, y=2))
        o.append(self.function_order_parameters(w=d, z=c, y=b, x=a))
        o = compss_wait_on(o)
        self.assertSequenceEqual(o, res, "Task not identify different order parameters")
