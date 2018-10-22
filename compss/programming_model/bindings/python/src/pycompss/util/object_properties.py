#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Utils: Object properties.

Offers some functions that check properties about objects.
For example, check if an object belongs to a module and so on.

"""

import imp

def get_module_name(path, file_name):
    '''Get the module name considering its path and filename.

    Example: runcompss -d src/kmeans.py
             path = "test/kmeans.py"
             file_name = "kmeans" (without py extension)
             return mod_name = "test.kmeans"

    :param path: relative path until the file.py from where the runcompss has been executed
    :param file_name: python file to be executed name (without the py extension)
    :return: the module name
    '''
    import os
    dirs = path.split(os.path.sep)
    mod_name = file_name
    i = len(dirs) - 1
    while i > 0:
        new_l = len(path) - (len(dirs[i]) + 1)
        path = path[0:new_l]
        if '__init__.py' in os.listdir(path):
            # directory is a package
            i -= 1
            mod_name = dirs[i] + '.' + mod_name
        else:
            break
    return mod_name


def get_top_decorator(code, decorator_keys):
    '''Retrieves the decorator which is on top of the current task decorators stack.

    :param code: Tuple which contains the task code to analyse and the number of lines of the code.
    :param decorator_keys: Typle which contains the available decorator keys
    :return: the decorator name in the form "pycompss.api.__name__"
    '''
    # Code has two fields:
    # code[0] = the entire function code.
    # code[1] = the number of lines of the function code.
    func_code = code[0]
    decorators = [l.strip() for l in func_code if l.strip().startswith('@')]
    # Could be improved if it stops when the first line without @ is found,
    # but we have to be care if a decorator is commented (# before @)
    # The strip is due to the spaces that appear before functions definitions,
    # such as class methods.
    for dk in decorator_keys:
        for d in decorators:
            if d.startswith('@' + dk):
                return 'pycompss.api.' + dk.lower()  # each decorator __name__
    # If no decorator is found, then the current decorator is the one to register
    return __name__

def get_wrapped_sourcelines(f):
    '''Gets a list of source lines and starting line number for the given function.
    :param f: Input function
    :return: Source lines
    '''
    def _get_wrapped_sourcelines(f):
        """
        Gets a list of source lines and starting line number for the given function.

        :param f: Input function
        :return: Source lines
        """
        if hasattr(f, "__wrapped__"):
            # has __wrapped__, going deep
            return _get_wrapped_sourcelines(f.__wrapped__)
        else:
            # Returning getsourcelines
            return inspect.getsourcelines(f)
    import inspect
    if hasattr(f, '__wrapped__'):
        # has __wrapped__, apply the same function to the wrapped content
        return _get_wrapped_sourcelines(f.__wrapped__)
    else:
        # Returning getsourcelines
        return inspect.getsourcelines(f)



def is_module_available(module_name):
    '''Checks if a module is available in the current Python installation.

    :param module_name: Name of the module
    :return: Boolean -> True if the module is available, False otherwise
    '''
    try:
        imp.find_module(module_name)
        return True
    except ImportError:
        return False

def is_basic_iterable(obj):
    '''Checks if an object is a basic iterable.
    By basic iterable we want to mean objects that are iterable and from a basic type.

    :param obj: Object to be analysed
    :return: Boolean -> True if obj is a basic iterable (see list below), False otherwise
    '''
    return isinstance(obj, (list, tuple, bytearray, set, frozenset, dict))

def object_belongs_to_module(obj, module_name):
    '''Checks if a given object belongs to a given module (or some sub-module).

    :param obj: Object to be analysed
    :param module_name: Name of the module we want to check
    :return: Boolean -> True if obj belongs to the given module, False otherwise
    '''
    return any(module_name == x for x in type(obj).__module__.split('.'))

def has_numpy_objects(obj):
    '''Checks if the given object is a numpy object or some of its subojects are.

    :param obj: An object
    :return: Boolean -> True if obj is a numpy objects (or some of
    its subobjects). False otherwise
    '''
    return has_subobjects_of_module(obj, 'numpy')
