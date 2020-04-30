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
PyCOMPSs API - MPI
==================
    This file contains the class mpi, needed for the mpi
    definition through the decorator.
"""

import inspect
import logging
import os
from functools import wraps
import pycompss.util.context as context
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.error_msgs import cast_env_to_int_error
from pycompss.util.arguments import check_arguments

if __debug__:
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {'runner'}
SUPPORTED_ARGUMENTS = {'binary',
                       'processes',
                       'working_dir',
                       'binary',
                       'runner',
                       'flags',
                       'scale_by_cu',
                       'fail_by_exit_value'}
DEPRECATED_ARGUMENTS = {'computing_nodes',
                        'computingNodes',
                        'workingDir'}


class MPI(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """

    def __init__(self, *args, **kwargs):
        """
        Store arguments passed to the decorator
        # self = itself.
        # args = not used.
        # kwargs = dictionary with the given mpi parameters

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        self.args = args
        self.kwargs = kwargs
        self.registered = False
        self.scope = context.in_pycompss()
        self.task_type = "mpi"
        if self.scope:
            if __debug__:
                logger.debug("Init @mpi decorator...")

            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            "@mpi")
            # Replace the legacy annotation
            if 'computingNodes' in self.kwargs:
                self.kwargs['processes'] = self.kwargs.pop('computingNodes')
            if 'computing_nodes' in self.kwargs:
                  self.kwargs['processes'] = self.kwargs.pop('computing_nodes')

            # Set default value if it has not been defined
            if 'processes' not in self.kwargs:
                self.kwargs['processes'] = 1

            # The processes parameter will have to go down until the execution is invoked.
            # WARN: processes can be an int, a env string, a str with dynamic variable name.
            processes = self.kwargs['processes']
            if __debug__:
                logger.debug("This MPI task will have " + str(processes) + " processes.")
        else:
            pass

    def __call__(self, func):
        """
        Parse and set the mpi parameters within the task core element.

        :param func: Function to decorate
        :return: Decorated function.
        """

        @wraps(func)
        def mpi_f(*args, **kwargs):
            if not self.scope:
                # from pycompss.api.dummy.mpi import mpi as dummy_mpi
                # d_m = dummy_mpi(self.args, self.kwargs)
                # return d_m.__call__(func)
                raise Exception(not_in_pycompss("mpi"))

            if context.in_master():
                # master code
                mod = inspect.getmodule(func)
                self.module = mod.__name__  # not func.__module__

                if self.module == '__main__' or self.module == 'pycompss.runtime.launch':
                    # The module where the function is defined was run as
                    # __main__, so we need to find out the real module name.

                    # path = mod.__file__
                    # dirs = mod.__file__.split(os.sep)
                    # file_name = os.path.splitext(
                    #                 os.path.basename(mod.__file__))[0]

                    # Get the real module name from our launch.py variable
                    path = getattr(mod, "APP_PATH")

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
                    self.module = mod_name

                # Include the registering info related to @mpi

                # Retrieve the base core_element established at @task decorator
                from pycompss.api.task import current_core_element as cce
                if not self.registered:
                    self.registered = True

                    # Update the core element information with the @mpi information
                    if "binary" in self.kwargs:
                        binary = self.kwargs['binary']
                        cce.set_impl_type("MPI")
                    else:
                        binary = "[unassigned]"
                        cce.set_impl_type("PYTHON_MPI")
                        self.task_type = "PYTHON_MPI"

                    if 'working_dir' in self.kwargs:
                        working_dir = self.kwargs['working_dir']
                    else:
                        working_dir = '[unassigned]'  # Empty or '[unassigned]'

                    runner = self.kwargs['runner']
                    if 'flags' in self.kwargs:
                        flags = self.kwargs['flags']
                    else:
                        flags = '[unassigned]'  # Empty or '[unassigned]'
                    if 'scale_by_cu' in self.kwargs:
                        scale_by_cu = self.kwargs['scale_by_cu']
                        if isinstance(scale_by_cu, bool):
                            if scale_by_cu :
                                scale_by_cu_str = 'true'
                            else:
                                scale_by_cu_str = 'false'
                        elif isinstance(scale_by_cu, str):
                            scale_by_cu_str = scale_by_cu
                        else:
                            raise Exception("Incorrect format for scale_by_cu property. " +
                                            " It should be boolean or an environment variable")
                    else :
                        scale_by_cu_str = 'false'
                        
                    if 'fail_by_exit_value' in self.kwargs:
                        fail_by_ev = self.kwargs['fail_by_exit_value']
                        if isinstance(fail_by_ev, bool):
                            if fail_by_ev:
                                fail_by_ev_str = 'true'
                            else:
                                fail_by_ev_str = 'false'
                        elif isinstance(fail_by_ev, str):
                            fail_by_ev_str = fail_by_ev
                        else:
                            raise Exception("Incorrect format for fail_by_exit_value property. " +
                                            " It should be boolean or an environment variable")
                    else :
                        fail_by_ev_str = 'false'
                    
                    if binary == "[unassigned]":
                        impl_signature = "MPI."
                    else:
                        impl_signature = 'MPI.' + str(self.kwargs['processes']) + "." + binary

                    # Add information to CoreElement
                    cce.set_impl_signature(impl_signature)
                    impl_args = [binary, working_dir, runner, flags, scale_by_cu_str, fail_by_ev_str]
                    cce.set_impl_type_args(impl_args)
            else:
                # worker code
                pass

            # This is executed only when called.
            if __debug__:
                logger.debug("Executing mpi_f wrapper.")

            # Set the computing_nodes variable in kwargs for its usage
            # in @task decorator
            kwargs['computing_nodes'] = self.kwargs['processes']

            if len(args) > 0:
                # The 'self' for a method function is passed as args[0]
                slf = args[0]

                # Replace and store the attributes
                saved = {}
                for k, v in self.kwargs.items():
                    if hasattr(slf, k):
                        saved[k] = getattr(slf, k)
                        setattr(slf, k, v)

            # Call the method
            import pycompss.api.task as t
            if self.task_type == "PYTHON_MPI":
                t.prepend_strings = True
            else:
                t.prepend_strings = False
            ret = func(*args, **kwargs)
            t.prepend_strings = True

            if len(args) > 0:
                # Put things back
                for k, v in saved.items():
                    setattr(slf, k, v)

            return ret

        mpi_f.__doc__ = func.__doc__
        return mpi_f


# ########################################################################### #
# ##################### MPI DECORATOR ALTERNATIVE NAME ###################### #
# ########################################################################### #

mpi = MPI
