"""
@author: fconejer

PyCOMPSs Functions: Elapsed time decorator
==========================================
    This file defines the time it decorator to be used over the task decorator.
"""

from decorator import decorator
import time

@decorator  # Mandatory in order to preserver the argspec
def timeit(func, *a, **k):
    """
    Elapsed time decorator.
    :param func: Function to be measured (can be a decorated function, usually with @task decorator).
    :param a: args
    :param k: kwargs
    :return: a list with [the function result, The elapsed time]
    """
    ts = time.time()
    result = func(*a, **k)
    te = time.time()
    return [result, (te-ts)]