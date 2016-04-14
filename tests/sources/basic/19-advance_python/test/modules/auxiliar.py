#!/usr/bin/python
# -*- coding: utf-8 -*-
'''AUXILIAR APP'''
from pycompss.api.task import task
from pycompss.api.parameter import *


@task(returns=list)
def function_B(v):
    import platform
    return list(platform.uname())


def app2(*args):
    from pycompss.api.api import compss_wait_on
    result = function_B(1)
    result = compss_wait_on(result)
    return result
