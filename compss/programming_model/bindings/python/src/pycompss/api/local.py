#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# -*- coding: utf-8 -*-

"""
PyCOMPSs API - LocalTask
========================
    This file contains the @local decorator, which is intended to be a
    decorator for non-task functions that may receive future objects
    as parameters (i.e: their inputs are pycompss task outputs).
    It also handles INOUTs
"""

import gc

from pycompss.api.api import compss_wait_on
from pycompss.util.objects.replace import replace
from pycompss.runtime.management.object_tracker import \
    OT_is_obj_pending_to_synchronize
import pycompss.util.context as context


def local(input_function):
    """ Local decorator.

    :param input_function: Input function.
    :return: Wrapped function.
    """
    if not context.in_pycompss():
        # Return dummy local decorator

        def wrapped_function(*args, **kwargs):
            return input_function(*args, **kwargs)

        return wrapped_function

    else:

        def sync_if_needed(obj):
            # type: (object) -> None
            if OT_is_obj_pending_to_synchronize(obj):
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


# ########################################################################### #
# #################### LOCAL DECORATOR ALTERNATIVE NAME ##################### #
# ########################################################################### #

Local = local
LOCAL = local
