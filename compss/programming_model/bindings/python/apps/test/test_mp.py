


'''
@author: etejedor
'''

from pycompss.api.api import compss_open, compss_wait_on

from test.test_tasks import function_primitives, function_files, function_objects, function_return_primitive, function_return_object, MyClass, par_func


def main_program():
    
    test_function_primitives()
    test_function_files()
    test_function_objects()
      
    test_mp_file_access()
    test_mp_object_access()
      
    test_instance_method()
    test_class_method()
      
    test_function_return_primitive()
    test_function_return_object()
    


def test_function_primitives():
    print "test_function_primitives"
    function_primitives(1, 1L, 1.0, True, 'a string')
    
    
def test_function_files():
    print "test_function_files"
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
    print "test_function_objects"
    val = 1
    o = MyClass(val)
    l = [1, 2, 3, 4]
    dic = {'key1':'value1', 'key2':'value2'}
    tup = ('a', 'b', 'c')
    cplx = complex('1+2j')
    
    function_objects(o, l, dic, tup, cplx, par_func)
    function_objects(o, l, dic, tup, cplx, par_func)
    
    
def test_mp_file_access():
    print "test_file_mp_access"
    
    print "test_function_files"
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
        print "- File access from MP: OK"
    else:
        print "- File access from MP: ERROR"
    fout_d.write("\n===> OUT FILE ADDED CONTENT")
    fout_d.close()
    
    function_files(fout, finout, fin)


def test_mp_object_access():
    print "test_object_mp_access"
    val = 1
    o = MyClass(val)
    l = [1, 2, 3, 4]
    dic = {'key1':'value1', 'key2':'value2'}
    tup = ('a', 'b', 'c')
    cplx = complex('1+2j')
    
    function_objects(o, l, dic, tup, cplx, par_func)
    o = compss_wait_on(o)
    if (o.field == val * 2):
        print "- Object access from MP: OK"
    else:
        print "- Object access from MP: ERROR"
    
    o.field = val * 4
    function_objects(o, l, dic, tup, cplx, par_func)


def test_instance_method():
    print "test_instance_method"
    val = 1
    o = MyClass(val)
    
    o.instance_method()
    o.instance_method_nonmodifier()
    o.instance_method()
    
    o = compss_wait_on(o)
    if (o.field == val * 4):
        print "- Object access from MP: OK"
    else:
        print "- Object access from MP: ERROR"
    
    
def test_class_method():
    print "test_class_method"
    MyClass.class_method() 


def test_function_return_primitive():
    print "test_function_return_primitive"
    val = 1
    i = function_return_primitive(val)
    function_return_primitive(i)
    
    i = compss_wait_on(i)
    if i == val * 2:
        print "- Primitive access from MP: OK"
    else:
        print "- Primitive access from MP: ERROR"
    
    
def test_function_return_object():
    print "test_function_return_object"
    val = 1
    o = function_return_object(val)
    o.instance_method()
     
    o = compss_wait_on(o)
    if o.field == val * 2:
        print "- Object access from MP: OK"
    else:
        print "- Object access from MP: ERROR"
    
    
if __name__ == "__main__":
    main_program()
    
    
