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

from functools import wraps
import pycompss.util.context as context
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import get_module
from pycompss.api.commons.decorator import keep_arguments
from pycompss.util.arguments import check_arguments

if __debug__:
    import logging

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


class MPI(PyCOMPSsDecorator):
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
        self.task_type = "mpi"
        decorator_name = '@' + self.__class__.__name__.lower()
        super(self.__class__, self).__init__(decorator_name, *args, **kwargs)
        if self.scope:
            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            decorator_name)

            # Replace the legacy annotation
            if 'computingNodes' in self.kwargs:
                self.kwargs['processes'] = self.kwargs.pop('computingNodes')
            if 'computing_nodes' in self.kwargs:
                self.kwargs['processes'] = self.kwargs.pop('computing_nodes')

            # Set default value if it has not been defined
            if 'processes' not in self.kwargs:
                self.kwargs['processes'] = 1

            # The processes parameter will have to go down until the execution is invoked.
            # WARNING: processes can be an int, a env string, a str with dynamic variable name.
            if __debug__:
                logger.debug("This MPI task will have " +
                             str(self.kwargs['processes']) + " processes.")
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
                raise Exception(not_in_pycompss("mpi"))

            if __debug__:
                logger.debug("Executing mpi_f wrapper.")

            if context.in_master():
                # master code
                self.module = get_module(func)

                if not self.registered:
                    # Register

                    # Resolve @mpi specific parameters
                    if "binary" in self.kwargs:
                        binary = self.kwargs['binary']
                        impl_type = "MPI"
                    else:
                        binary = "[unassigned]"
                        impl_type = "PYTHON_MPI"
                        self.task_type = impl_type

                    runner = self.kwargs['runner']

                    if 'flags' in self.kwargs:
                        flags = self.kwargs['flags']
                    else:
                        flags = '[unassigned]'  # Empty or '[unassigned]'

                    if 'scale_by_cu' in self.kwargs:
                        scale_by_cu = self.kwargs['scale_by_cu']
                        if isinstance(scale_by_cu, bool):
                            if scale_by_cu:
                                scale_by_cu_str = 'true'
                            else:
                                scale_by_cu_str = 'false'
                        elif isinstance(scale_by_cu, str):
                            scale_by_cu_str = scale_by_cu
                        else:
                            raise Exception("Incorrect format for scale_by_cu property. " +  # noqa: E501
                                            "It should be boolean or an environment variable")  # noqa: E501
                    else:
                        scale_by_cu_str = 'false'

                    # Resolve the working directory
                    self.__resolve_working_dir__()
                    # Resolve the fail by exit value
                    self.__resolve_fail_by_exit_value__()

                    if binary == "[unassigned]":
                        impl_signature = impl_type + '.'
                    else:
                        impl_signature = '.'.join((impl_type,
                                                   str(self.kwargs['processes']),  # noqa: E501
                                                   binary))
                    impl_args = [binary,
                                 self.kwargs['working_dir'],
                                 runner,
                                 flags,
                                 scale_by_cu_str,
                                 self.kwargs['fail_by_exit_value']]

                    # Retrieve the base core_element established at @task
                    # decorator and update the core element information with
                    # the @mpi information
                    from pycompss.api.task import CURRENT_CORE_ELEMENT as cce
                    cce.set_impl_type(impl_type)
                    cce.set_impl_signature(impl_signature)
                    cce.set_impl_type_args(impl_args)

                    # Set as registered
                    self.registered = True
            else:
                # worker code
                pass

            # Set the computing_nodes variable in kwargs for its usage
            # in @task decorator
            kwargs['computing_nodes'] = self.kwargs['processes']

            if self.task_type == "PYTHON_MPI":
                prepend_strings = True
            else:
                prepend_strings = False

            with keep_arguments(args, kwargs, prepend_strings=prepend_strings):
                # Call the method
                ret = func(*args, **kwargs)

            return ret

        mpi_f.__doc__ = func.__doc__
        return mpi_f


# ########################################################################### #
# ##################### MPI DECORATOR ALTERNATIVE NAME ###################### #
# ########################################################################### #

mpi = MPI
