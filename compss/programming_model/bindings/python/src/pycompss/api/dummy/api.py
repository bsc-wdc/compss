"""
@author: fconejer

PyCOMPSs Dummy API
==================
    This file defines the public PyCOMPSs API functions without functionality.
    It implements a dummy compss_open and compss_wait_on functions.
"""


def compss_wait_on(obj):
    """
    Dummy compss_wait_on.
    :param obj: The object to wait on.
    :return: The same object defined as parameter
    """
    return obj


def compss_open(obj):
    """
    Dummy compss_open
    :param obj: The object to open
    :return: The same object defined as parameter
    """
    return obj
