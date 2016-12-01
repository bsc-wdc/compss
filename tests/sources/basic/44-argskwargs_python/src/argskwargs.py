from pycompss.api.task import task
from pycompss.api.parameter import *

@task(returns=int)
def argTask(*arg):
    print "ARG: ", arg
    return sum(arg)
  
@task(returns=int)
def varargTask(v, w, *arg):
    print "V: ", v
    print "W: ", w
    print "ARG: ", arg
    return (v * w) + sum(arg)

@task(returns=int)
def kwargTask(**karg):
    print "KARG: ", karg
    return len(karg)
  
@task(returns=int)
def varkwargTask(v, w , **karg):
    print "V: ", v
    print "W: ", w
    print "KARG: ", karg
    return (v * w) + len(karg)
  
@task(returns=int)
def argkwargTask(*arg, **karg):
    print "ARG: ", arg
    print "KARG: ", karg
    return sum(arg) + len(karg)

@task(returns=int)
def varargkwargTask(v, w , *arg, **karg):
    print "V: ", v
    print "W: ", w
    print "ARG: ", arg
    print "KARG: ", karg
    return (v * w) + sum(arg) + len(karg)

def main():
    from pycompss.api.api import compss_wait_on

    # Test all independently
    v11 = argTask(1,2)
    v12 = argTask(1,2,3,4)
    
    v21 = varargTask(10, 20, 1, 2, 3, 4)
    v22 = varargTask(4, 50, 5, 4, 3, 2, 1)
    
    v31 = kwargTask(hello='world')
    v32 = kwargTask(this='is', a='test')
    
    v41 = varkwargTask(1, 2, hello='world')
    v42 = varkwargTask(2, 3, this='is', a='test')
    
    v51 = argkwargTask(1, 2, hello='world')
    v52 = argkwargTask(1, 2, 3, 4, this='is', a='test')
    
    v61 = varargkwargTask(1, 2, 3, 4, hello='world')
    v62 = varargkwargTask(1, 2, 3, 4, 5, 6, this='is', a='test')

    v1 = compss_wait_on(v11)
    v2 = compss_wait_on(v12)
    v3 = compss_wait_on(v21)
    v4 = compss_wait_on(v22)
    v5 = compss_wait_on(v31)
    v6 = compss_wait_on(v32)
    v7 = compss_wait_on(v41)
    v8 = compss_wait_on(v42)
    v9 = compss_wait_on(v51)
    v10 = compss_wait_on(v52)
    v11 = compss_wait_on(v61)
    v12 = compss_wait_on(v62)
    
    if v1 == 3:
        print "- Test *arg 1: OK"
    else:
        print "- Test *arg 1: ERROR"
        
    if v2 == 10:
        print "- Test *arg 2: OK"
    else:
        print "- Test *arg 2: ERROR"
        
    if v3 == 210:
        print "- Test vars and *arg 1: OK"
    else:
        print "- Test vars and *arg 1: ERROR"
        
    if v4 == 215:
        print "- Test vars and *arg 2: OK"
    else:
        print "- Test vars and *arg 2: ERROR"
        
    if v5 == 1:
        print "- Test **kwarg 1: OK"
    else:
        print "- Test **kwarg 1: ERROR"
        
    if v6 == 2:
        print "- Test **kwarg 2: OK"
    else:
        print "- Test **kwarg 2: ERROR"
        
    if v7 == 3:
        print "- Test vars and **kwarg 1: OK"
    else:
        print "- Test vars and **kwarg 1: ERROR"
        
    if v8 == 8:
        print "- Test vars and **kwarg 2: OK"
    else:
        print "- Test vars and **kwarg 2: ERROR"
        
    if v9 == 4:
        print "- Test *arg and **kwarg 1: OK"
    else:
        print "- Test *arg and **kwarg 1: ERROR"
        
    if v10 == 12:
        print "- Test *arg and **kwarg 2: OK"
    else:
        print "- Test *arg and **kwarg 2: ERROR"
        
    if v11 == 10:
        print "- Test vars and *arg and **kwarg 1: OK"
    else:
        print "- Test vars and *arg and **kwarg 1: ERROR"
        
    if v12 == 22:
        print "- Test vars and *arg and **kwarg 2: OK"
    else:
        print "- Test vars and *arg and **kwarg 2: ERROR"

if __name__ == '__main__':
    main()
    
