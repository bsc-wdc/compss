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

PyCOMPSs Utils: Object properties.

Offers some functions that check properties about objects.
For example, check if an object belongs to a module and so on.

"""
import imp
import types
import ctypes
import inspect
import collections

def is_module_available(module_name):
    """
    Checks if a module is available in the current Python installation.
    @param module_name: Name of the module
    @return: Boolean -> True if the module is available, False otherwise
    """
    try:
        imp.find_module(module_name)
        return True
    except:
        return False

def is_basic_iterable(obj):
    """
    Checks if an object is a basic iterable.
    By basic iterable we want to mean objects that are iterable and from
    a basic type.
    @param obj: Object to be analysed
    @return: Boolean -> True if obj is a basic iterable (see list below), False otherwise
    """
    return isinstance(obj, (list, tuple, bytearray, buffer, xrange, set, frozenset, dict))


def object_belongs_to_module(obj, module_name):
    """
    Checks if a given object belongs to a given module
    (or some sub-module).
    @param obj: Object to be analysed
    @param module_name: Name of the module we want to check
    @return: Boolean -> True if obj belongs to the given module, False otherwise
    """
    return any(module_name == x for x in type(obj).__module__.split('.'))


def get_object_hierarchy(obj):
    """
    Generates a set of identifiers of objects that are obj or are under the
    object hierarchy determined by obj.

    W A R N I N G: This function can give incomplete object hierarchies if there is
    some object which is iterable and it is not included in our list. Since in python
    __iter__ can modify objects and there is no way to know when this will happen without
    explictly copying or iterating the object, then only a few safe types will be iterated.
    Anyway, this will cover almost all the common Python codes one can find, even in
    scientific environments.

    @param obj: Object to be analysed
    @yield: An integer with the id of an object from the object hierarchy of obj
    """
    object_stack = [obj]
    vis = set()
    while object_stack:
        current_object = object_stack.pop()
        current_object_id = id(current_object)
        if not current_object_id in vis:
            vis.add(current_object_id)
            yield current_object
            if hasattr(current_object, '__dict__'):
                map(object_stack.append, current_object.__dict__.values())
            elif is_basic_iterable(current_object):
                map(object_stack.append, iter(current_object))

def has_subobjects_of_module(obj, module_name):
    """
    Detects if an object belongs to a module or, at least, has
    some sub-object that belongs to it. We only iterate through
    sub objects that are class instances. This is due to the fact
    that, in Python, there are some objects that, when iterated,
    change in some way. For example, file objects move their
    pointer when iterated.
    @param obj: Object to be analysed
    @param module_name: Name of the module we want to check
    @return: Boolean -> True if obj or some subobject belongs to the given module, False otherwise
    """
    if not is_module_available(module_name):
        return False
    for sub_object in get_object_hierarchy(obj):
        if object_belongs_to_module(sub_object, module_name):
            return True
    return False

def has_numpy_objects(obj):
    """
    Checks if the given object is a numpy object or
    some of its subojects are.
    @param obj: An object
    @return: Boolean -> True if obj is a numpy objects (or some of 
    its subobjects). False otherwise
    """
    return has_subobjects_of_module(obj, 'numpy')
