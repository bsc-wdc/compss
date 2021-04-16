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
from .internals import _init, _check_init, _submit_point, ValueWrapper, _check_accessed, _delete_accessed
from exaqute.common import ExaquteException


def init():
    _init()


def get_value_from_remote(obj):
    _check_init()
    _submit_point()
    t = type(obj)
    if t is list:
        return [get_value_from_remote(o) for o in obj]
    if t is ValueWrapper:
        if not obj.keep:
            raise ExaquteException("get_value_from_remote called on not keeped object, object created at {}".format(obj.traceback))
        return obj.unwrap_value()
    else:
        if isinstance(obj,(int, bool, float, str)):
            return obj
        else:
            if not _check_accessed(obj):
                print("WARN: get_value_from_remote called on non-task value, got {!r}".format(obj))
            else:
                _delete_accessed(obj)
            return obj

def barrier():
    _check_init()
    _submit_point()

def delete_object(*objs):
    for obj in objs: 
        _delete_object(obj)

def _delete_object(obj):
    _check_init()
    t = type(obj)
    if t is list:
        for o in obj:
            _delete_object(o)
    elif t is ValueWrapper:
        if not obj.keep:
            raise ExaquteException("Deleting non-keeped object, object created at {}".format(obj.traceback))
        if obj.deleted:
            raise ExaquteException("Deleting already deleted object, object created at {}".format(obj.traceback))
        obj.deleted = True
    else:
        if not isinstance(obj, (bool, int, float, str)):
            if not _check_accessed(obj):
                raise ExaquteException("delete_object called on non-task value, got {!r}".format(obj) + " type " + str(t))
            else:
                 _delete_accessed(obj)
