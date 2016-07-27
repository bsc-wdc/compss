"""
@author: scorella

PyCOMPSs Mathematical Library: Algebra: Mean
============================================
    This file contains the arithmetic mean algorithm.
"""


from pycompss.api.task import task
from pycompss.functions.reduce import mergeReduce


def _list_lenght(l):
    """
    Recursive function to get the size of any list
    """
    if l:
        if not isinstance(l[0], list):
            return 1 + _list_lenght(l[1:])
        else:
            return _list_lenght(l[0]) + _list_lenght(l[1:])
    return 0


@task(returns=float)
def _mean(X, n):
    return sum(X)/float(n)


def mean(X, wait=False):
    """
    Arithmetic mean
    :param X: chunked data
    :param wait: if we want to wait for result. Default False
    :return: mean of X.
    """
    n = _list_lenght(X)
    result = mergeReduce(reduce_add, [_mean(x, n) for x in X])
    if wait:
        from pycompss.api.api import compss_wait_on
        result = compss_wait_on(result)
    return result
