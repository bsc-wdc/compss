#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs DECORATOR COMMONS
==========================
    This file contains the main decorator class.
"""

import os
import inspect
from contextlib import contextmanager

import pycompss.util.context as context
from pycompss.api.commons.error_msgs import wrong_value
from pycompss.api.commons.error_msgs import cast_env_to_int_error
from pycompss.api.commons.error_msgs import cast_string_to_int_error

if __debug__:
    import logging
    logger = logging.getLogger(__name__)


class PyCOMPSsDecorator(object):
    """
    This class implements all common code of the PyCOMPSs decorators.
    """

    def __init__(self, decorator_name, *args, **kwargs):  # noqa
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = context.in_pycompss()
        self.registered = False
        # This enables the decorator to get info from the caller
        # (e.g. self.source_frame_info.filename or
        #       self.source_frame_info.lineno)
        # self.source_frame_info = inspect.getframeinfo(inspect.stack()[1][0])

        if self.scope:
            if __debug__:
                logger.debug("Init " + decorator_name + " decorator...")

    def __resolve_working_dir__(self):
        """
        Resolve the working directory considering deprecated naming.
        Updates self.kwargs:
            - Removes workingDir if exists.
            - Updates working_dir with the working directory.

        :return: None
        """
        if 'working_dir' in self.kwargs:
            # Accepted argument
            pass
        elif 'workingDir' in self.kwargs:
            self.kwargs['working_dir'] = self.kwargs.pop('workingDir')
        else:
            self.kwargs['working_dir'] = '[unassigned]'  # Empty or '[unassigned]'

    def __resolve_fail_by_exit_value__(self):
        """
        Resolve the fail by exit value.
        Updates self.kwargs:
            - Updates fail_by_exit_value if necessary.

        :return: None
        """
        if 'fail_by_exit_value' in self.kwargs:
            fail_by_ev = self.kwargs['fail_by_exit_value']
            if isinstance(fail_by_ev, bool):
                # Accepted argument
                pass
            elif isinstance(fail_by_ev, str):
                # Accepted argument
                pass
            else:
                raise Exception("Incorrect format for fail_by_exit_value property. " +  # noqa: E501
                                "It should be boolean or an environment variable")      # noqa: E501
        else:
            self.kwargs['fail_by_exit_value'] = 'false'


###################
# COMMON CONTEXTS #
###################

@contextmanager
def keep_arguments(args, kwargs, prepend_strings=True):
    """
    Context which saves and restores the function arguments.
    It also enables or disables the PREPEND_STRINGS property from @task.

    :return: None
    """
    # Keep function arguments
    if len(args) > 0:
        # The 'self' for a method function is passed as args[0]
        slf = args[0]

        # Replace and store the attributes
        saved = {}
        for k, v in kwargs.items():
            if hasattr(slf, k):
                saved[k] = getattr(slf, k)
                setattr(slf, k, v)
    # Set PREPEND_STRINGS
    import pycompss.api.task as t
    if not prepend_strings:
        t.PREPEND_STRINGS = False
    yield
    # Restore PREPEND_STRINGS to default: True
    t.PREPEND_STRINGS = True
    # Restore function arguments
    if len(args) > 0:
        # Put things back
        for k, v in saved.items():
            setattr(slf, k, v)


##############################
# COMMON PARAMETER FUNCTIONS #
##############################

def process_computing_nodes(decorator_name, kwargs):
    """
    Process computing nodes from decorator.
    Modifies the kwargs dictionary since it can be used in lower level
    decorators (until execution when invoked).
    Used in some decorators:
        - compss
        - decaf
        - multinode
        - ompss

    :return: the number of computing nodes
    """
    if 'computing_nodes' not in kwargs and 'computingNodes' not in kwargs:
        kwargs['computing_nodes'] = 1
    else:
        if 'computingNodes' in kwargs:
            kwargs['computing_nodes'] = kwargs.pop('computingNodes')
        computing_nodes = kwargs['computing_nodes']
        if isinstance(computing_nodes, int):
            # Nothing to do
            pass
        elif isinstance(computing_nodes, str):
            # Check if it is an environment variable to be loaded
            if computing_nodes.strip().startswith('$'):
                # Computing nodes is an ENV variable, load it
                env_var = computing_nodes.strip()[1:]  # Remove $
                if env_var.startswith('{'):
                    env_var = env_var[1:-1]  # remove brackets
                try:
                    kwargs['computing_nodes'] = int(os.environ[env_var])
                except ValueError:
                    raise Exception(
                        cast_env_to_int_error('Computing Nodes'))
            else:
                # ComputingNodes is in string form, cast it
                try:
                    kwargs['computing_nodes'] = int(computing_nodes)
                except ValueError:
                    raise Exception(
                        cast_string_to_int_error('Computing Nodes'))
        else:
            raise Exception(wrong_value("Computing Nodes", decorator_name))

    if __debug__:
        logger.debug("This " + decorator_name + " task will have " +
                     str(kwargs['computing_nodes']) +
                     " computing nodes.")


def get_module(function):
    """
    Retrieve the module from the given function.

    :param function: Function to analyse.
    :return: The function's module
    """
    module = inspect.getmodule(function)
    module_name = module.__name__  # not func.__module__

    if module_name == '__main__' or module_name == 'pycompss.runtime.launch':
        # The module where the function is defined was run as
        # __main__, so we need to find out the real module name.

        # path = module.__file__
        # dirs = module.__file__.split(os.sep)
        # file_name = os.path.splitext(os.path.basename(module.__file__))[0]

        # Get the real module name from our launch.py variable
        path = getattr(module, "APP_PATH")

        dirs = path.split(os.path.sep)
        file_name = os.path.splitext(os.path.basename(path))[0]
        mod_name = file_name

        i = len(dirs) - 1
        while i > 0:
            new_l = len(path) - (len(dirs[i]) + 1)
            path = path[0:new_l]
            if "__init__.py" in os.listdir(path):
                # directory is a package
                i -= 1
                mod_name = dirs[i] + '.' + mod_name
            else:
                break
        module_name = mod_name

    return module_name
