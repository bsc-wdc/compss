#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
--------
TEST APP
--------

Aplicacion que utiliza la funcionalidad de launch_pycompss_applicacion llamando
a aplicaciones con parametros (mismo fichero! sin anidamiento)
EJECUCION LOCAL

__main__ --> launch(app)
                 |--------> task(function_A)

Ejecucion: python maqueta3.py
"""

from pycompss.api.task import task
from pycompss.api.parameter import *


@task(returns=int, priority=True)
def function_A(x, y, z):
    result = x + y + z
    print result
    return result


def app(a, b, c):
    from pycompss.api.api import compss_wait_on
    print "In app"
    x = function_A(a, b, c)
    print x
    x = compss_wait_on(x)
    print x
    return x
