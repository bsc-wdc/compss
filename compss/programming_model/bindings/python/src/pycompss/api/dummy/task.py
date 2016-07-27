"""
@author: fconejer

PyCOMPSs Dummy API - task
=========================
    This file contains the dummy class task used as decorator.

    # How to use it:
    try:
        from pycompss.api.parameter import *
        from pycompss.api.task import task
        from pycompss.api.constraint import constraint
    except ImportError:
        from pycompss.api.dummy.parameter import *
        from pycompss.api.dummy.task import task
        from pycompss.api.dummy.constraint import constraint

    @constraint(ProcessorCoreCount=8)
    @task(returns=list, a=FILE_IN)
    def foo(a, b):
        return (a, b)

    def main():
        res = foo(1, 2)
        try:
            from pycompss.api.api import compss_wait_on
        except ImportError:
            from pycompss.api.dummy.api import compss_wait_on

        res = compss_wait_on(res)
        print res

    if __name__ == "__main__":
        main()
"""


class task(object):
    """
    Dummy task class (decorator style)
    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        def wrapped_f(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapped_f
