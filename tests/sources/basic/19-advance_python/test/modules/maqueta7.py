#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
--------
TEST APP
--------

Aplicacion que utiliza la funcionalidad de launch_pycompss_applicacion llamando a aplicaciones con parametros y con anidamiento
(todo dentro del mismo fichero!)
EJECUCION LOCAL

__main__ --> launch(app)
                 |--------> task(function_A) -----> launch(app2)
                                                        |----------> task(function_B)

Ejecucion: python maqueta7.py
"""

import time
import sys
import os
from pycompss.api.task import task
from pycompss.api.parameter import *


#app_path = os.path.abspath(module.maqueta7.__file__).replace('.pyc', '.py')  # "/home/user/test_maqueta/src/maqueta7.py"


@task(returns=list)
def function_B(v):
    import platform
    return list(platform.uname())


def app2(x, y, z):
    from pycompss.api.api import compss_wait_on
    print "-----------"
    print "sys.argv_"
    print sys.argv
    print "x_"
    print x
    print "y_"
    print y
    print "z_"
    print z
    print "-----------"
    result = function_B(1)
    result = compss_wait_on(result)
    return result


@task(returns=int, priority=True)
def function_A(x, y, z):
    result = x + y + z
    # print result
    from pycompss.runtime.launch import launch_pycompss_application
    import module.maqueta7
    aux = os.path.abspath(module.maqueta7.__file__).replace('.pyc', '.py')  # "/home/user/test_maqueta/src/maqueta7.py"
    args = ['3', '2', '1']
    kwargs = {}
    x = launch_pycompss_application(aux, 'app2', args, kwargs)
    # print x
    return [result, x]


def app(a, b, c):
    from pycompss.api.api import compss_wait_on
    print "In app"
    print "sys.argv:"
    print sys.argv
    print "a:"
    print a
    print "b:"
    print b
    print "c:"
    print c
    x = function_A(2, 2, 2)
    # print x
    x = compss_wait_on(x)
    # print x
    return x
