#!/usr/bin/python
# -*- coding: utf-8 -*-
'''TEST APP'''
import time
from pycompss.api.task import task
from pycompss.api.parameter import *

@task(returns=int,priority=True)
def function_A(x, y, z):
    result = x + y + z
    return result


@task(returns=list)
def function_B(v):
    import platform
    return list(platform.uname())


@task(returns=str)
def function_C(x):
    import subprocess
    p = subprocess.Popen("date", stdout=subprocess.PIPE, shell=True)
    (output, err) = p.communicate()
    return "Today is", output


def main():
    import sys
    import os
    from pycompss.api.api import compss_wait_on
    a = 1
    b = 2
    c = 3

    print "Start"
    start = time.time()

    result = function_A(a, b, c)    
    result = compss_wait_on(result)
    l = []
    m = []
    for i in range(c):
        l.append(function_B(a))
        m.append(function_C(a))
    l = compss_wait_on(l)
    m = compss_wait_on(m)

    end = time.time()-start
    print "Result:"
    print result
    for i in range(c):
        print "l:"
        print l[i]
        print "m:"
        print m[i]
    print "Ellapsed Time:"
    print end
    
    
    print "------------------------"
    print "------------------------"
    
    return result


if __name__ == "__main__":
    main()