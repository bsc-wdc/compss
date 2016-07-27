"""
@author: fconejer

PyCOMPSs Dummy API - Constraint
===============================
    This file contains the dummy class constraint used as decorator.
"""


class constraint(object):
    """
    Dummy constraint class (decorator style)
    """

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def __call__(self, f):
        def wrapped_f(*args, **kwargs):
            return f(*args, **kwargs)
        return wrapped_f
