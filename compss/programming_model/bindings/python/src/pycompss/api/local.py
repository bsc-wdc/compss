#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs API - LocalTask
========================
    This file contains the @local decorator, which is intended to be a
    decorator for non-task functions that may receive future objects
    as parameters (i.e: their inputs are pycompss task outputs).
    It also handles INOUTs
"""

from pycompss.api.api import compss_wait_on
from pycompss.util.replace import replace
from pycompss.util.location import i_am_within_scope
import gc


def local(input_function):
    if not i_am_within_scope():

        # Return dummy local decorator
        def wrapped_function(*args, **kwargs):
            return input_function(*args, **kwargs)
        return wrapped_function

    else:

        from pycompss.runtime.binding import get_object_id, pending_to_synchronize

        def must_sync(obj):
            return get_object_id(obj) in pending_to_synchronize

        def sync_if_needed(obj):
            if must_sync(obj):
                new_val = compss_wait_on(obj)
                replace(obj, new_val)

        def wrapped_function(*args, **kwargs):
            gc.collect()
            _args = []
            _kwargs = {}
            for arg in args:
                sync_if_needed(arg)
                _args.append(arg)
            for (key, value) in kwargs.items():
                sync_if_needed(value)
                _kwargs[key] = value

            return input_function(*_args, **_kwargs)

        return wrapped_function
