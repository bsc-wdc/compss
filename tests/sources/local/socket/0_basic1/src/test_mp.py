#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
==================
    This file represents PyCOMPSs Testbench.
    It implements all functionalities in order to evaluate the PyCOMPSs features.
"""

# Imports
import sys

from pycompss.api.api import compss_open, compss_wait_on
from modules.test_tasks import empty_task
from modules.test_tasks import function_primitives, function_files, function_objects, function_out_object, function_return_primitive
from modules.test_tasks import function_return_object, MyClass, par_func
from modules.test_tasks import function_function_parameter, formula2
from modules.test_tasks import function_default_parameter_values, function_order_parameters
from modules.test_tasks import function_fu_object, function_fu_in_task, function_return_in_param
from modules.test_tasks import function_fu_list_object, function_fu_list_in_task
from modules.test_tasks import function_iterable_object_wait, function_wait_on_string
from modules.test_tasks import function_time_decorated_master, function_time_decorated_worker
from modules.test_tasks import function_argfunc, function_lambda, function_generator
from modules.test_tasks import function_lambda_return, function_generator_return
from modules.test_tasks import multireturn, power, merge
from modules.test_tasks import function_moduleObject, Foo
from modules.test_tasks import create_block, update_block
from modules.test_tasks import empty_string, char_to_int
from modules.test_tasks import numpy_obj_creator
from modules.test_tasks import increment_object_delete, modify_obj
from modules.my_class import MyClass2
from pycompss.runtime.management.object_tracker import OT
from modules.test_tasks import mod_task, pass_task, mod_class


def test_empty_function():
    print("test_empty_function")
    for i in range(8):
        empty_task()


def test_function_primitives():
    print("test_function_primitives")
    lo = 1  # In python 2 it is long = 1L, but in python 3 ints are longs.
    function_primitives(1, lo, 1.0, True, 'a string')


def test_function_files():
    print("test_function_files")
    fin = 'infile'
    finout = 'inoutfile'
    fout = 'outfile'

    fin_d = open(fin, 'w')
    finout_d = open(finout, 'w')
    fin_d.write('IN FILE CONTENT')
    finout_d.write('INOUT FILE INITIAL CONTENT')
    fin_d.close()
    finout_d.close()

    function_files(fin, finout, fout)


def test_function_objects():
    print("test_function_objects")
    val = 2
    o = MyClass(val)
    li = [1]
    dic = {'key1': 'value1', 'key2': 'value2'}
    tup = ('a', 'b', 4)
    cplx = complex('1+2j')

    function_objects(o, li, dic, tup, cplx, par_func)
    function_objects(o, li, dic, tup, cplx, par_func)

    errors = False

    o = compss_wait_on(o)
    if o.field != 8:
        print("- INOUT (objects): ERROR")
        errors = True

    li = compss_wait_on(li)
    if li != [1, 2, 2]:
        print("- INOUT (list): ERROR")
        errors = True

    dic = compss_wait_on(dic)
    if dic != {'key1': 'value1', 'key2': 'value2', 'key3': 'value3', 'key4': 'value4'}:
        print("- INOUT (dictionary): ERROR")
        errors = True

    # Tuples are inmutable... why INOUT?
    # tup = compss_wait_on(tup)
    # if tup != ('a', 'b', 'c', 'd', 'd'):
    #     print("- INOUT (tuple): ERROR")
    #     errors = True

    # Operations with tuples always generate a new object.
    # They behave like simple types... why INOUT?
    # cplx = compss_wait_on(cplx)
    # if cplx != complex('4+8j'):
    #     print("- INOUT (complex): ERROR")
    #     errors = True

    if not errors:
        print("- INOUT: OK")


def test_function_out_object():
    print("test_function_out_object")
    o = MyClass(2)

    function_out_object(o)

    o = compss_wait_on(o)
    if o.field != 100:
        print("- OUT (object): ERROR")
    else:
        print("- OUT (object): OK")


def test_mp_file_access():
    print("test_file_mp_access")

    print("test_function_files")
    fin = 'infile'
    finout = 'inoutfile'
    fout = 'outfile'

    fin_d = open(fin, 'w')
    finout_d = open(finout, 'w')
    fin_d.write('IN FILE CONTENT')
    finout_d.write('INOUT FILE INITIAL CONTENT')
    fin_d.close()
    finout_d.close()

    function_files(fin, finout, fout)

    fout_d = compss_open(fout, 'r+')
    if fout_d.read() == 'OUT FILE CONTENT':
        print("- File access from MP: OK")
    else:
        print("- File access from MP: ERROR")
    fout_d.write("\n===> OUT FILE ADDED CONTENT")
    fout_d.close()

    function_files(fout, finout, fin)


def test_mp_object_access():
    print("test_object_mp_access")
    val = 1
    o = MyClass(val)
    li = [1, 2, 3, 4]
    dic = {'key1': 'value1', 'key2': 'value2'}
    tup = ('a', 'b', 'c')
    cplx = complex('1+2j')

    function_objects(o, li, dic, tup, cplx, par_func)
    o = compss_wait_on(o)
    if o.field == val * 2:
        print("- Object access from MP 1: OK")
    else:
        print("- Object access from MP 1: ERROR")

    o.field = val * 4
    function_objects(o, li, dic, tup, cplx, par_func)


def test_instance_method():
    print("test_instance_method")
    val = 1
    o = MyClass(val)

    o.instance_method()
    o.instance_method_nonmodifier()
    o.instance_method()

    o = compss_wait_on(o)
    if o.field == val * 4:
        print("- Object access from MP 2: OK")
    else:
        print("- Object access from MP 2: ERROR")


def test_class_method():
    print("test_class_method")
    MyClass.class_method()


def test_static_method():
    print("test_static_method")
    o = MyClass(1)
    result = o.static_method(2, 4, 8)
    result = compss_wait_on(result)
    if result == (2+4) * 8:
        print("- Static method: OK")
    else:
        print("- Static method: ERROR")


def test_function_return_primitive():
    print("test_function_return_primitive")
    val = 1
    i = function_return_primitive(val)
    function_return_primitive(i)

    i = compss_wait_on(i)
    if i == val * 2:
        print("- Primitive access from MP: OK")
    else:
        print("- Primitive access from MP: ERROR")


def test_function_return_object():
    print("test_function_return_object")
    val = 1
    o = function_return_object(val)
    o.instance_method()

    o = compss_wait_on(o)
    if o.field == val * 2:
        print("- Object access from MP 3: OK")
    else:
        print("- Object access from MP 3: ERROR")


def test_return_in_param():
    A = [1, 2]
    orig_id = id(A)

    A = function_return_in_param(A)
    task_id_0 = id(A)

    A = function_return_in_param(A)
    task_id_1 = id(A)

    A = function_return_in_param(A)
    task_id_2 = id(A)

    new_a = compss_wait_on(A)

    assert orig_id == task_id_0 == task_id_1 == task_id_2
    assert len(new_a) == 16


def test_function_as_parameter():
    print("test_function_as_parameter")
    f = formula2
    v = 2
    o = function_function_parameter(f, v)
    o = compss_wait_on(o)
    if o == v * v * v:
        print("- Function as a parameter: OK")
    else:
        print("- Function as a parameter: ERROR")


def test_default_parameters():
    print("test_default_parameters")
    o = function_default_parameter_values()
    o = compss_wait_on(o)
    if o == 100:
        print("- Default parameter value: OK")
    else:
        print("- Default parameter value: ERROR")


def test_order_parameters():
    print("test_order_parameters")
    o1 = function_order_parameters(1, 2)
    o2 = function_order_parameters(2, 2, 3)
    o3 = function_order_parameters(3, 2, 3, 4)
    o4 = function_order_parameters(4, 2, z=20)
    o5 = function_order_parameters(5, 2, w=40)
    o6 = function_order_parameters(6, 2, w=40, z=20)
    o7 = function_order_parameters(z=20, x=7, w=40, y=2)
    a = 8
    b = 2
    c = 20
    d = 40
    o8 = function_order_parameters(w=d, z=c, y=b, x=a)

    o1 = compss_wait_on(o1)
    o2 = compss_wait_on(o2)
    o3 = compss_wait_on(o3)
    o4 = compss_wait_on(o4)
    o5 = compss_wait_on(o5)
    o6 = compss_wait_on(o6)
    o7 = compss_wait_on(o7)
    o8 = compss_wait_on(o8)

    if o1 == 100003:
        print("- Test parameter position (sorted): OK")
    else:
        print("- Test parameter position (sorted): ERROR")

    print("- Test parameter position (sorted): OK" if o1 == 100003 else "- Test parameter position (sorted): ERROR")

    if o2 == 3004:
        print("- Test parameter position + 1 default (sorted): OK")
    else:
        print("- Test parameter position + 1 default (sorted): ERROR")

    if o3 == 17:
        print("- Test parameter position + 2 default (sorted): OK")
    else:
        print("- Test parameter position + 2 default (sorted): ERROR")

    if o4 == 20006:
        print("- Test parameter position + 1 explicit value (sorted): OK")
    else:
        print("- Test parameter position + 1 explicit value (sorted): ERROR")

    if o5 == 4007:
        print("- Test parameter position + 1 explicit value (unsorted): OK")
    else:
        print("- Test parameter position + 1 explicit value(unsorted): ERROR")

    if o6 == 808:
        print("- Test parameter position + 2 explicit value (unsorted): OK")
    else:
        print("- Test parameter position  + 2 explicit value (unsorted): ERROR")

    if o7 == 809:
        print("- Test parameter position completely unsorted: OK")
    else:
        print("- Test parameter position completely unsorted: ERROR")

    if o8 == 810:
        print("- Test parameter position completely unsorted (variables): OK")
    else:
        print("- Test parameter position completely unsorted (variables): ERROR")

    if o1 == 100003 and o2 == 3004 and o3 == 17 and o4 == 20006 and o5 == 4007 and o6 == 808 and o7 == 809 and o8 == 810:
        print("- All test_order_parameteres successful!!!: OK")
    else:
        print("- At least one test_order_parameters has FAILED: ERROR")


def test_fu_parameter_in_task():
    print("test_fu_parameter_in_task")
    x = 5
    fu = function_fu_object(x)
    o = function_fu_in_task(fu)
    o = compss_wait_on(o)
    if o == 20:
        print("- Test future object as parameter in task: OK")
    else:
        print("- Test future object as parameter in task: ERROR")


def test_fu_list_in_task():
    print("test_fu_list_in_task")
    li = []
    for i in range(0, 5):
        li.append(1)
    fu = function_fu_list_object(li)
    o = function_fu_list_in_task(fu)
    o = compss_wait_on(o)
    result = True
    for i in range(0, 5):
        if o[i] != 3:
            result = False
    if result:
        print("- Test future object list as parameter in task: OK")
    else:
        print("- Test future object list as parameter in task: ERROR")


def test_iterable_object_wait():
    print("test_iterable_object_wait")
    # List
    iobj = []
    for i in range(0, 10):
        iobj.append(i)

    # full modification
    for i in range(len(iobj)):
        iobj[i] = function_iterable_object_wait(iobj[i])

    iobj = compss_wait_on(iobj)

    result = True
    for i in range(len(iobj)):
        if iobj[i] != i * i:
            result = False

    if result:
        print("- Wait on an iterable object list (completely modified): OK")
    else:
        print("- Wait on an iterable object list (completely modified): ERROR")

    iobj = []
    for i in range(0, 10):
        iobj.append(i)

    # partial modification
    for i in range(len(iobj) - 5):
        iobj[i] = function_iterable_object_wait(iobj[i])

    iobj = compss_wait_on(iobj)

    result = True
    for i in range(len(iobj) - 5):
        if iobj[i] != i * i:
            result = False
    for i in range(5, len(iobj)):
        if iobj[i] != i:
            result = False

    if result:
        print("- Wait on an iterable object list (partially modified): OK")
    else:
        print("- Wait on an iterable object list (partially modified): ERROR")

    # Dict
    iobj = {}
    for i in range(0, 10):
        iobj[i] = i

    # full dict modification
    for i in range(len(iobj)):
        iobj[i] = function_iterable_object_wait(iobj[i])

    iobj = compss_wait_on(iobj)

    result = True
    for i in range(len(iobj)):
        if iobj[i] != i * i:
            result = False

    if result:
        print("- Wait on an iterable object dict (completely modified): OK")
    else:
        print("- Wait on an iterable object dict (completely modified): ERROR")

    iobj = {}
    for i in range(0, 10):
        iobj[i] = i
    # partial dict modification
    for i in range(len(iobj) - 5):
        iobj[i] = function_iterable_object_wait(iobj[i])

    iobj = compss_wait_on(iobj)

    result = True
    for i in range(len(iobj) - 5):
        if iobj[i] != i * i:
            result = False
    for i in range(5, len(iobj)):
        if iobj[i] != i:
            result = False

    if result:
        print("- Wait on an iterable object dict (partially modified): OK")
    else:
        print("- Wait on an iterable object dict (partially modified): ERROR")


def test_wait_on_string():
    print("test_wait_on_string")
    s = "helloworld"
    o = function_wait_on_string(s)
    o = compss_wait_on(o)
    if o == s.upper():
        print("- Wait on a basestring object: OK")
    else:
        print("- Wait on a basestring object: ERROR")


def test_time_decorator():
    print("test_time_decorator")
    x = 2
    o = function_time_decorated_master(x)
    out = compss_wait_on(o[0])
    if out == x * x * x:
        print("- Test timeit decorator (master time): OK")
        print("\t * %s" % o[1])
    else:
        print("- Test timeit decorator (master time): ERROR")

    o = function_time_decorated_worker(x)
    o = compss_wait_on(o)
    if o[0] == x * x * x:
        print("- Test timeit decorator (worker time): OK")
        print("\t * %s" % o[1])
    else:
        print("- Test timeit decorator (worker time): ERROR")

    x = 2
    o = []
    for i in range(5):
        o.append(function_time_decorated_master(x))
    values = [row[0] for row in o]
    times = [row[1] for row in o]
    # if the decorator is outside, the wait_on must be done only for the values
    o = compss_wait_on(values)
    r = True
    for i in range(5):
        if o[i] != x * x * x:
            r = False
    if r:
        print("- Test timeit decorator with list (master time) : OK")
        for i in range(len(times)):
            print("\t * %s" % times[i])
    else:
        print("- Test timeit decorator with list (master time): ERROR")

    x = 2
    o = []
    for i in range(5):
        o.append(function_time_decorated_worker(x))
    # if the decorator is inside, the wait_on must be done previously
    o = compss_wait_on(o)
    values = [row[0] for row in o]
    times = [row[1] for row in o]
    r = True
    for i in range(5):
        if values[i] != x * x * x:
            r = False
    if r:
        print("- Test timeit decorator with list (worker time): OK")
        for i in range(len(times)):
            print("\t * %s" % times[i])
    else:
        print("- Test timeit decorator with list (worker time): ERROR")


# My function
def fun(x):
    return x * x


def test_argfunc():
    print("test_argfunc")
    f = fun
    v = function_argfunc(f, 2)
    v = compss_wait_on(v)
    if v == 2 * 2:
        print("- Test function as argument: OK")
    else:
        print("- Test funcition as argument: ERROR")


def test_lambda():
    print("test_lambda")
    f = lambda x: x ** 2 + 2 * x - 5
    v = function_lambda(f, 10)
    v = compss_wait_on(v)
    if v == 115:
        print("- Test lambda as argument: OK")
    else:
        print("- Test lambda as argument: ERROR")


# My generator
def gen(n):
    num = 0
    while num < n:
        yield num
        num += 1


def test_generator():
    print("test_generator")
    g = gen(10)
    import sys
    if sys.version_info >= (3, 0):
        next(g)
        next(g)
        pre = next(g)
    else:
        g.next()
        g.next()
        pre = g.next()
    print("Status before task: ", pre)
    v = function_generator(g)
    post = compss_wait_on(v)
    print("Status within task (next generator value): ", post)
    if post == (pre + 1):
        print("- Test generator as argument: OK")
    else:
        print("- Test generator as argument: ERROR")


def test_lambda_return():
    print("test_lambda_return")
    li = function_lambda_return()
    li = compss_wait_on(li)
    out = li(10)
    if out == 115:
        print("- Test return lambda: OK")
    else:
        print("- Test return lambda: ERROR")


def test_generator_return():
    print("test_generator_return")
    g = gen(10)
    import sys
    if sys.version_info >= (3, 0):
        pre = next(g)
    else:
        pre = g.next()
    gr = function_generator_return(g)
    gr = compss_wait_on(gr)
    if sys.version_info >= (3, 0):
        out = next(gr)
    else:
        out = gr.next()
    if out == (pre + 2):
        print("- Test return generator: OK")
    else:
        print("- Test return generator: ERROR")


def test_all_class_tasks():
    print("test_instance_method")
    val = 1
    o = MyClass(val)
    o.instance_method(88)  # 1
    o.instance_method_nonmodifier()  # 2
    o.instance_method(88)  # 3
    o = compss_wait_on(o)
    if o.field == val * 4:
        print("- Object access from MP 4: OK")
    else:
        print("- Object access from MP 4: ERROR")

    print("test_class_method")
    MyClass.class_method()  # 4

    print("test_instance_method_with_parameter_and_return")

    o = MyClass('HolaMundo')
    b = o.return_value_square(99)  # 5
    b1 = compss_wait_on(b)
    o1 = compss_wait_on(o)
    # print('result1: ', b1
    # print('accum  : ', o1.v
    if b1 == 9801 and o1.v == 99:
        print("- Object access from MP (Round 1): OK")
    else:
        print("- Object access from MP (Round 1): ERROR")

    b = o.return_value_square(199)  # 5
    b2 = compss_wait_on(b)
    # print('result2: ', b2)
    if b2 == 39601:
        print("- Object access from MP (Round 2): OK")
    else:
        print("- Object access from MP (Round 2): ERROR")

    b = o.return_value_square(299)  # 5
    b3 = compss_wait_on(b)
    o3 = compss_wait_on(o)
    # print('result3: ', b3)
    # print('accum  : ', o3.v)
    if b3 == 89401 and o3.v == 597:
        print("- Object access from MP (Round 3): OK")
    else:
        print("- Object access from MP (Round 3): ERROR")


def test_multireturn():
    print("test_multireturn")
    v, w = multireturn(10.0)
    x = power(v)
    y = power(w)
    res = merge(x, y)
    res = compss_wait_on(res)
    a = compss_wait_on(x)
    b = compss_wait_on(y)
    if res == 500 and a == 100 and b == 400:
        print("- Test multiple objects return: OK")
    else:
        print("- Test multiple objects return: ERROR")


def test_multireturn_multicall():
    print("test_multireturn_multicall")
    v, w = multireturn(10.0)
    v, w = multireturn(10.0)
    x = power(v)
    y = power(w)
    res = merge(x, y)
    res = compss_wait_on(res)
    a = compss_wait_on(x)
    b = compss_wait_on(y)
    if res == 500 and a == 100 and b == 400:
        print("- Test multiple objects return (multicall): OK")
    else:
        print("- Test multiple objects return (multicall): ERROR")


def test_moduleObject():
    print("test_moduleObject")
    foo = Foo()
    foo.set(10)
    result = function_moduleObject(foo)
    result = compss_wait_on(result)
    if result.get() == 10 * 10:
        print("- Test module object parameter: OK")
    else:
        print("- Test module object parameter: ERROR")


def test_inouts():
    print("CREATE BLOCKS")
    a = create_block(4)
    b = create_block(4)

    print("WAIT FOR A BLOCK")
    a = compss_wait_on(a)
    initial = a.copy()
    print(initial)

    print("UPDATE INOUT")
    update_block(a, b)

    print("WAIT FOR A BLOCK")
    a = compss_wait_on(a)
    print(a)

    if (initial == a).all():
        print("- Test inouts: ERROR")
    else:
        print("- Test inouts: OK")


def test_empty_string():
    s = ''
    result = empty_string(s)
    result = compss_wait_on(result)
    if result == 'XXXX':
        print("- Test empty string: OK")
    else:
        print(result)
        print("- Test empty string: ERROR")


def test_character():
    mychar = 'c'
    result = char_to_int(mychar)
    result = compss_wait_on(result)
    if result == ord(mychar):
        print("- Test character: OK")
    else:
        print("- Test character: ERROR")
        assert result == ord(mychar)


def test_numpy_type_assurance():
    # This test will fail if numpy save is used with other numpy types
    # rather than ndarray and matrix
    import numbers
    value = numpy_obj_creator()
    value = compss_wait_on(value)
    if isinstance(value, numbers.Number):
        print("- Test numpy type assurance: OK")
    else:
        print("- Test numpy type assurance: ERROR")
        assert isinstance(value, numbers.Number)


def testInDeleteObject():
    obj_1 = [0]
    obj_2 = increment_object_delete(obj_1)
    obj_2 = compss_wait_on(obj_2)
    obj_1_id = OT.get_object_id(obj_1)
    if not (obj_1_id in OT.pending_to_synchronize) and OT.get_object_id(obj_1) == "":
        print("- Test 1 IN_DELETE object: OK")
    else:
        print("- Test 1 IN_DELETE object: ERROR")


def testInDeleteObject2():
    obj_1 = [0]
    count = 0
    for i in range(10):
        obj_1[0] = i - 1
        obj_2 = increment_object_delete(obj_1)
        obj_2 = compss_wait_on(obj_2)
        obj_1_id = OT.get_object_id(obj_1)
        if not (obj_1_id in OT.pending_to_synchronize) and OT.get_object_id(obj_1) == "" and i == obj_2[0]:
            count += 1
    if count == 10:
        print("- Test 2 IN_DELETE object: OK")
    else:
        print("- Test 2 IN_DELETE object: ERROR")


def testInDeleteObject3():
    obj_1 = [0]
    obj_list = []
    for i in range(10):
        obj_1[0] = i - 1
        obj_2 = increment_object_delete(obj_1)
        obj_list.append(obj_2)
    obj_list = compss_wait_on(obj_list)
    if obj_list == [[i] for i in range(10)]:
        print("- Test 3 IN_DELETE object: OK")
    else:
        print("- Test 3 IN_DELETE object: ERROR")


def testInDeleteObject4():
    obj_1 = MyClass2()
    obj_2 = modify_obj(obj_1)
    obj_2 = compss_wait_on(obj_2)
    obj_1_id = OT.get_object_id(obj_1)
    if not (obj_1_id in OT.pending_to_synchronize) and OT.get_object_id(obj_1) == "":
        print("- Test 4 IN_DELETE object: OK")
    else:
        print("- Test 4 IN_DELETE object: ERROR")


def main_program():
    test_empty_function()
    test_function_primitives()
    test_function_files()
    test_function_objects()
    test_mp_file_access()
    test_mp_object_access()
    test_instance_method()

    test_class_method()
    if sys.version_info >= (3, 0):
        # Static methods are only supported with python 3
        test_static_method()

    test_function_return_primitive()
    test_function_return_object()
    test_return_in_param()
    test_function_as_parameter()
    test_default_parameters()
    test_order_parameters()
    test_fu_parameter_in_task()
    test_fu_list_in_task()

    test_iterable_object_wait()
    test_wait_on_string()

    test_time_decorator()

    # test_argfunc()   # can not be done if the function is defined within the same file as __main__ -> pickle limitation
    test_lambda()
    test_generator()

    test_lambda_return()
    test_generator_return()

    # test_all_class_tasks()

    test_multireturn()

    test_multireturn_multicall()

    test_moduleObject()

    test_inouts()

    test_empty_string()
    test_character()

    test_numpy_type_assurance()

    testInDeleteObject()
    testInDeleteObject2()
    testInDeleteObject3()
    testInDeleteObject4()


if __name__ == "__main__":
    main_program()
