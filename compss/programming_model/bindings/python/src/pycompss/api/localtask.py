#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
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
"""
@author: srodrig1

PyCOMPSs API - LocalTask
===================
    This file contains the localtask decorator, which is intended to be a
    decorator for non-task functions that may receive future objects
    as parameters (i.e: their inputs are pycompss task outputs).
"""
from pycompss.api.api import compss_wait_on
from pycompss.runtime.binding import Future
from pycompss.util.replace import replace
import ctypes
import sys
import gc

def localtask(input_function):
    from pycompss.runtime.binding import task_objects

    def must_sync(obj):
        return isinstance(obj, Future) or id(obj) in task_objects

    def sync_if_needed(obj):
        if must_sync(obj):
            old_id = id(obj)
            new_val = compss_wait_on(obj)
            replace(obj, new_val)
            del task_objects[old_id]

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
