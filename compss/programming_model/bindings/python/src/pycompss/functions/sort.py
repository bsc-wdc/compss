"""
@author: scorella

PyCOMPSs Functions: Data generators
===================================
    This file defines the common data producing functions.
"""


def sort(iterable, comp=None, key=None, reverse=False):
    """
    Apply function of two arguments cumulatively to the items of data, from left to right,
    so as to reduce the iterable to a single value.
    :param iterable: data.
    :param comp: specifies a custom comparison function of two arguments.
    :param key: specifies a function of one argument that is used to extract a comparison key from each list element.
    :param reverse: if set to True, then the list elements are sorted as if each comparison were reversed.
    :return: a new sorted list from the items in iterable.
    """
    try:
        return sorted(iterable, comp, key, reverse)
    except Exception, e:
        raise e
