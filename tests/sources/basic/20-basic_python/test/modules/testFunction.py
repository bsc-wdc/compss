import unittest
from pycompss.api.task import task
from pycompss.api.parameter import *
from test.modules.MyClass import MyClass
from test.modules.auxFunctions import formula1

@task(returns=int)
def function_without_params():
    return 1

class testFunction(unittest.TestCase):

    @task(returns=list)
    def function_primitives(self, i, l, f, b, s):
        types_list = [type(i), type(l), type(f), type(b), type(s)]
        #print "Primitive params: %d, %ld, %f, %d, %s % (i, l, f, b, s)"
        return map(str, types_list)

    @task(fin=FILE, finout=FILE_INOUT, fout=FILE_OUT)
    def function_files(self, fin, finout, fout):
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

    @task(returns=list)
    def function_objects(self, o, l, dic, tup, cplx, f):
        # Bug conocido no se puede devolver type(function): no es serializable
        tipos = map(str, [type(o), type(l), type(dic), type(tup), type(cplx), type(f)])
        return tipos

    @task(returns=int)
    def function_future_object(self, x):
        return x+x

    @task(returns=list)
    def function_fu_list_object(self, l):
        for i in xrange(len(l)):
            l[i] = l[i] + 1
        return l

    @task(returns=int)
    def function_return_primitive(self, i):
        return i * 2

    @task(returns=MyClass)
    def function_return_object(self, i):
        o = MyClass(i)
        return o

    def test_function_primitives(self):
        """ Test function_primitives"""
        from pycompss.api.api import compss_wait_on
        types_list = self.function_primitives(1, 1L, 1.0, True, 'a string')
        types_list = compss_wait_on(types_list)
        res = map(str, [type(1), type(1L), type(1.0), type(True), type('a string')])
        self.assertSequenceEqual(types_list, res)

    def test_function_files(self):
        """ test function files """
        from pycompss.api.api import compss_open
        # todo hacer tests separados
        fin, finout, fout = 'infile', 'inoutfile', 'outfile'
        fin_d = open(fin, 'w')
        finout_d = open(finout, 'w')
        fin_d.write('IN FILE CONTENT')
        finout_d.write('INOUT FILE INITIAL CONTENT')
        fin_d.close()
        finout_d.close()
        self.function_files(fin, finout, fout)
        # add check content
        fin_d = open(fin, 'r')
        self.assertEqual(fin_d.read(), 'IN FILE CONTENT')

        finout_d = compss_open(finout, 'r')
        self.assertEqual(finout_d.read(), 'INOUT FILE INITIAL CONTENT\n===> INOUT FILE ADDED CONTENT')

        fout_d = compss_open(fout, 'r')
        self.assertEqual(fout_d.read(), "OUT FILE CONTENT")
        # todo remove files

    def test_function_objects(self):
        """ Test function objects """
        from pycompss.api.api import compss_wait_on
        val = 1
        o = MyClass(val)  # fail?
        l = [1, 2, 3, 4]
        dic = {'key1': 'value1', 'key2': 'value2'}
        tup = ('a', 'b', 'c')
        cplx = complex('1+2j')
        f = formula1  # no va bien

        v = self.function_objects(o, l, dic, tup, cplx, f)
        v = compss_wait_on(v)
        res = map(str, [type(o), type(l), type(dic), type(tup), type(cplx), type(f)])
        self.assertSequenceEqual(v, res)

    def test_function_return_primitive(self):
        """ Test function return primitive"""
        from pycompss.api.api import compss_wait_on
        val = 1
        i = self.function_return_primitive(val)
        self.function_return_primitive(i)  # why?
        i = compss_wait_on(i)
        self.assertEqual(i, val*2)

    def test_function_return_object(self):
        """ test function return object """
        from pycompss.api.api import compss_wait_on
        val = 1
        o = self.function_return_object(val)
        o.instance_method()
        o = compss_wait_on(o)
        self.assertEqual(o.field, val*2)

    def test_function_future_parameter(self):
        """ Test function future parameter"""
        from pycompss.api.api import compss_wait_on
        x = 5
        fu = self.function_future_object(x)
        o = self.function_future_object(fu)
        o = compss_wait_on(o)
        self.assertEqual(o, 20)

    def test_function_future_list(self):
        """ Test function future object list"""
        from pycompss.api.api import compss_wait_on
        l = [1 for _ in range(5)]
        fu = self.function_fu_list_object(l)
        o = self.function_fu_list_object(fu)
        o = compss_wait_on(o)
        res = [3 for _ in range(5)]
        self.assertSequenceEqual(o, res)

    def test_function_without_parameters(self):
        """ Test function without Params """
        from pycompss.api.api import compss_wait_on
        o = function_without_params()
        o = compss_wait_on(o)
        self.assertEqual(o, 1)
