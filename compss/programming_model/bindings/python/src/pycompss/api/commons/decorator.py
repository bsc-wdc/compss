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
PyCOMPSs DECORATOR COMMONS
==========================
    This file contains the main decorator class.
"""

import os
import sys
import subprocess
from contextlib import contextmanager

import pycompss.util.context as context
from pycompss.util.exceptions import MissingImplementedException
from pycompss.util.exceptions import PyCOMPSsException

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

# Global name to be used within kwargs for the core element.
CORE_ELEMENT_KEY = 'compss_core_element'


class PyCOMPSsDecorator(object):
    """
    This class implements all common code of the PyCOMPSs decorators.
    """

    __slots__ = ['decorator_name', 'args', 'kwargs',
                 'scope', 'core_element', 'core_element_configured']

    def __init__(self, decorator_name, *args, **kwargs):  # noqa
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = context.in_pycompss()
        self.core_element = None
        self.core_element_configured = False
        # This enables the decorator to get info from the caller
        # (e.g. self.source_frame_info.filename or
        #       self.source_frame_info.lineno)
        # import inspect
        # self.source_frame_info = inspect.getframeinfo(inspect.stack()[1][0])

        if __debug__ and self.scope:
            # Log only in the master
            logger.debug("Init " + decorator_name + " decorator...")

    def __configure_core_element__(self, kwargs, user_function):
        # type: (dict, ...) -> None
        """
        Include the registering info related to the decorator which inherits

        :param kwargs: Current keyword arguments to be updated with the core
                       element information.
        :param user_function: Decorated function.
        :return: None
        """
        raise MissingImplementedException("__configure_core_element__")

    #########################################
    # VERY USUAL FUNCTIONS THAT MODIFY SELF #
    #########################################

    def __resolve_working_dir__(self):
        # type: () -> None
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
            self.kwargs['working_dir'] = '[unassigned]'

    def __resolve_fail_by_exit_value__(self):
        # type: () -> None
        """
        Resolve the fail by exit value.
        Updates self.kwargs:
            - Updates fail_by_exit_value if necessary.

        :return: None
        """
        if 'fail_by_exit_value' in self.kwargs:
            fail_by_ev = self.kwargs['fail_by_exit_value']
            if isinstance(fail_by_ev, bool):
                self.kwargs['fail_by_exit_value'] = str(fail_by_ev)
            elif isinstance(fail_by_ev, str):
                # Accepted argument
                pass
            elif isinstance(fail_by_ev, int):
                self.kwargs['fail_by_exit_value'] = str(fail_by_ev)
            else:
                raise PyCOMPSsException("Incorrect format for fail_by_exit_value property. "    # noqa: E501
                                        "It should be boolean or an environment variable")      # noqa: E501
        else:
            self.kwargs['fail_by_exit_value'] = 'false'

    def __process_computing_nodes__(self, decorator_name):
        # type: (str) -> None
        """
        Processes the computing_nodes from the decorator.
        We only ensure that the correct self.kwargs entry exists since its value
        will be parsed and resolved by the master.process_computing_nodes.
        Used in decorators:
            - mpi
            - multinode
            - compss
            - decaf

        :return: None
        """
        if 'computing_nodes' not in self.kwargs:
            if 'computingNodes' not in self.kwargs:
                # No annotation present, adding default value
                self.kwargs['computing_nodes'] = 1
            else:
                # Legacy annotation present, switching
                self.kwargs['computing_nodes'] = self.kwargs.pop('computingNodes')  # noqa: E501
        else:
            # Valid annotation found, nothing to do
            pass

        if __debug__:
            logger.debug("This " + decorator_name + " task will have " +
                         str(self.kwargs['computing_nodes']) +
                         " computing nodes.")


###################
# COMMON CONTEXTS #
###################

@contextmanager
def keep_arguments(args, kwargs, prepend_strings=True):
    # type: (tuple, dict, bool) -> None
    """
    Context which saves and restores the function arguments.
    It also enables or disables the PREPEND_STRINGS property from @task.

    :param args: Arguments.
    :param kwargs: Key word arguments.
    :param prepend_strings: Prepend strings in the task.
    :return: None
    """
    # Keep function arguments
    saved = None
    slf = None
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


#################
# OTHER COMMONS #
#################

def run_command(cmd, args, kwargs):
    # type: (list, tuple, dict) -> int
    """ Executes the command considering necessary the args and kwargs.

    :param cmd: Command to run.
    :param args: Decorator arguments.
    :param kwargs: Decorator key arguments.
    :return: Execution return code.
    """
    if args:
        args_elements = []
        for a in args:
            if a:
                args_elements.append(a)
        cmd += args_elements
    my_env = os.environ.copy()
    env_path = my_env["PATH"]
    if "working_dir" in kwargs:
        my_env["PATH"] = kwargs["working_dir"] + env_path
    elif "workingDir" in kwargs:
        my_env["PATH"] = kwargs["workingDir"] + env_path
    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            env=my_env)
    out, err = proc.communicate()
    if sys.version_info[0] < 3:
        out_message = out.strip()
        err_message = err.strip()
    else:
        out_message = out.decode().strip()
        err_message = err.decode().strip()
    if out_message:
        print(out_message)
    if err_message:
        sys.stderr.write(err_message + '\n')
    return proc.returncode
