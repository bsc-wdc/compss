#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
--------
TEST APP
--------

Aplicacion que utiliza la funcionalidad de launch_pycompss_applicacion llamando
a aplicaciones con parametros y con anidamiento
(distintos ficheros!)
EJECUCION LOCAL

__main__ --> launch(app)
                 |--------> task(function_A) -----> launch(auxiliar.app2)
                                                        |----------> task(function_B)

Ejecucion: python maqueta5.py
"""

import time
import sys
from pycompss.api.task import task
from pycompss.api.parameter import *


@task(returns=int, priority=True)
def function_A(x, y, z):
    result = x + y + z
    # print result
    from pycompss.runtime.launch import launch_pycompss_application
    import modules.auxiliar
    aux = os.path.abspath(modules.auxiliar.__file__).replace('.pyc', '.py')  # "/home/user/test_maqueta/src/auxiliar.py"
    args = ['3', '2', '1']
    kwargs = {}
    x = launch_pycompss_application(aux, 'app2', args, kwargs)
    # print x
    return [result, x]


def app(a, b, c):
    from pycompss.api.api import compss_wait_on
    print "In app"
    print sys.argv
    x = function_A(a, b, c)
    # print x
    x = compss_wait_on(x)
    # print x
    return x
