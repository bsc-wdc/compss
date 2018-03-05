#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Utils - Location
===================
    This file contains the methods to detect the origin of the call stack.
    Useful to detect if we are in the master or in the worker.
"""

import sys
import inspect


def i_am_at_master():
    """
    Determine if the execution is being performed in the master node or
    in a worker node.

    # if 'pycompss/runtime/launch.py' in inspect.stack()[-1][1]: --> I am at master
    # if (inspect.stack()[-2][3] == 'compss_worker' or
    #     inspect.stack()[-2][3] == 'compss_persistent_worker'): --> I am at worker
    :return: <Boolean> - True if we are in the master node. False if we are in a worker node.
    """
    return not inspect.stack()[-2][3] in ['compss_worker', 'compss_persistent_worker']


def i_am_within_scope():
    """
    Determine if the execution is being performed within the PyCOMPSs scope.
    :return:  <Boolean> - True if under scope. False on the contrary.
    """
    return sys.path[0].endswith('Bindings/python/pycompss/runtime') or \
           sys.path[0].endswith('Bindings/python/pycompss/worker') or \
           sys.path[0] == ''
