#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Util - Objects - Properties.

Offers some functions that check properties about objects.
For example, check if an object belongs to a module and so on.
"""

import builtins
import inspect
import os
import sys
from collections import OrderedDict

from pycompss.util.typing_helper import typing


def get_module_name(path: str, file_name: str) -> str:
    """Get the module name considering its path and filename.

    Example: runcompss -d src/kmeans.py
             path = "test/kmeans.py"
             file_name = "kmeans" (without py extension)
             return mod_name = "test.kmeans"

    :param path: relative path until the file.py from where the runcompss has
                 been executed.
    :param file_name: python file to be executed name
                      (without the py extension).
    :return: the module name
    """
    dirs = path.split(os.path.sep)
    mod_name = file_name
    i = len(dirs) - 1
    while i > 0:
        new_l = len(path) - (len(dirs[i]) + 1)
        path = path[0:new_l]
        if "__init__.py" in os.listdir(path):
            # directory is a package
            i -= 1
            mod_name = dirs[i] + "." + mod_name
        else:
            break
    return mod_name


def get_wrapped_source(function: typing.Callable) -> str:
    """Get the text of the source code for the given function.

    :param function: Input function.
    :return: Source.
    """
    if hasattr(function, "__wrapped__"):
        # Has __wrapped__: going deep
        wrapped = function.__wrapped__  # type: ignore
        return get_wrapped_source(wrapped)
    # Returning getsource
    try:
        source = inspect.getsource(function)
    except TypeError:
        # This is a numba jit declared task
        py_func = function.py_func  # type: ignore
        source = inspect.getsource(py_func)
    return source


def is_module_available(module_name: str) -> bool:
    """Check if a module is available in the current Python installation.

    :param module_name: Name of the module.
    :return: True if the module is available. False otherwise.
    """
    try:
        try:
            import importlib

            _importlib = importlib  # type: typing.Any
            module = _importlib.util.find_spec(module_name)  # noqa
        except AttributeError:
            # This can only happen in conda
            import imp  # noqa # Deprecated in python 3

            module = imp.find_module(module_name)  # noqa
        if module:
            return True
        return False
    except ImportError:
        return False


def is_basic_iterable(obj: typing.Any) -> bool:
    """Check if an object is a basic iterable.

    By basic iterable we want to mean objects that are iterable and from a
    basic type.

    :param obj: Object to be analysed.
    :return: True if obj is a basic iterable (see list below). False otherwise.
    """
    return isinstance(obj, (list, tuple, bytearray, set, frozenset))


def is_dict(obj: typing.Any) -> bool:
    """Check if an object is a dictionary.

    :param obj: Object to be analysed.
    :return: True if obj is of dict type. False otherwise.
    """
    return isinstance(obj, (dict, OrderedDict))


def object_belongs_to_module(obj: typing.Any, module_name: str) -> bool:
    """Check if a given object belongs to a given module (or some sub-module).

    :param obj: Object to be analysed.
    :param module_name: Name of the module we want to check.
    :return: True if obj belongs to the given module. False otherwise.
    """
    return any(module_name == x for x in type(obj).__module__.split("."))


def create_object_by_con_type(con_type: str) -> typing.Any:
    """Create an "empty" object knowing its class name.

    :param con_type: object type info in <path_to_module>:<class_name> format.
    :return: "empty" object of a type.
    """
    path, class_name = con_type.split(":")
    if hasattr(builtins, class_name):
        _obj = getattr(builtins, class_name)
        return _obj()

    directory, module_name = os.path.split(path)
    module_name = os.path.splitext(module_name)[0]

    klass = globals().get(class_name, None)
    if klass:
        return klass()

    if module_name not in sys.modules:
        sys.path.append(directory)
        module = __import__(module_name)
        sys.modules[module_name] = module
    else:
        module = sys.modules[module_name]

    klass = getattr(module, class_name)
    ret = klass()
    return ret


#########################################################
#               DEPRECATED FUNCTIONS                    #
#########################################################
# These functions are not currently used within         #
# PyCOMPSs, but they are kept just in case needed.      #
#########################################################
#
# def get_defining_class(method):
#     # type: (...) -> ...
#     """ Get the class from the given a method.
#
#     :param method: Method to check its defining class.
#     :return: Class which method belongs. None if not found.
#     """
#     if inspect.ismethod(method):
#         for cls in inspect.getmro(method.__self__.__class__):
#             if cls.__dict__.get(method.__name__) is method:
#                 return cls
#     if inspect.isfunction(method):
#         return getattr(inspect.getmodule(method),
#                        method.__qualname__.split(".<locals>",
#                                                  1)[0].rsplit(".", 1)[0])
#     # Return not required since None would have been implicitly
#     # returned anyway
#     return None
#
#
# def get_top_decorator(code, decorator_keys):
#     # type: (list, list) -> str
#     """ Retrieves the decorator which is on top of the current task
#     decorators stack.
#
#     :param code: Tuple which contains the task code to analyse and the number
#                  of lines of the code.
#     :param decorator_keys: Tuple which contains the available decorator keys
#     :return: the decorator name in the form "pycompss.api.__name__"
#     """
#     # Code has two fields:
#     # code[0] = the entire function code.
#     # code[1] = the number of lines of the function code.
#     func_code = code[0]
#     decorators = [code_line.strip() for code_line in
#                   func_code if code_line.strip().startswith("@")]
#     # Could be improved if it stops when the first line without @ is found,
#     # but we have to be care if a decorator is commented (# before @)
#     # The strip is due to the spaces that appear before functions
#     # definitions, such as class methods.
#     for dk in decorator_keys:
#         for d in decorators:
#             if d.startswith("@" + dk):
#                 # each decorator __name__
#                 return "pycompss.api." + dk.lower()
#     # If no decorator is found, the current decorator is the one to register
#     return __name__
#
#
# def get_wrapped_source_lines(f):
#     # type: (...) -> tuple
#     """ Gets a list of source lines and starting line number for the given
#     function.
#
#     :param f: Input function.
#     :return: Source lines.
#     """
#     if hasattr(f, "__wrapped__"):
#         # has __wrapped__, apply the same function to the wrapped content
#         return _get_wrapped_source_lines(f.__wrapped__)
#     else:
#         # Returning get_source_lines
#         try:
#             source_lines = inspect.getsourcelines(f)
#         except TypeError:
#             # This is a numba jit declared task
#             source_lines = inspect.getsourcelines(f.py_func)
#         return source_lines
#
#
# def _get_wrapped_source_lines(f):
#     # type: (...) -> tuple
#     """ Recursive function which gets a list of source lines and starting
#     line number for the given function.
#
#     :param f: Input function.
#     :return: Source lines.
#     """
#     if hasattr(f, "__wrapped__"):
#         # has __wrapped__, going deep
#         return _get_wrapped_source_lines(f.__wrapped__)
#     else:
#         # Returning get_source_lines
#         try:
#             source_lines = inspect.getsourcelines(f)
#         except TypeError:
#             # This is a numba jit declared task
#             source_lines = inspect.getsourcelines(f.py_func)
#         return source_lines
