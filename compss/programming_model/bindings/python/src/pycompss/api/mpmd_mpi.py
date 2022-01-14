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
PyCOMPSs API - MPMD MPI
==================
    This file contains the class mpmd mpi, needed for the multiple program mpi
    definition through the decorator.
"""

from functools import wraps

from pycompss.api.commons.constants import RUNNER

import pycompss.util.context as context
from pycompss.api.commons.constants import *
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import resolve_working_dir
from pycompss.api.commons.decorator import resolve_fail_by_exit_value
from pycompss.runtime.task.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException


if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {RUNNER}
SUPPORTED_ARGUMENTS = {RUNNER,
                       PROGRAMS,
                       WORKING_DIR,
                       PROCESSES_PER_NODE,
                       FAIL_BY_EXIT_VALUE}
DEPRECATED_ARGUMENTS = set()


class MPMDMPI(PyCOMPSsDecorator):
    """
    """

    __slots__ = ['task_type', 'decorator_name', 'processes']

    def __init__(self, *args, **kwargs):
        """ Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given mpi parameters.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        self.task_type = "mpmd_mpi"
        self.decorator_name = "".join(('@', MPMDMPI.__name__.lower()))
        self.processes = 0
        super(MPMDMPI, self).__init__(self.decorator_name, *args, **kwargs)
        if self.scope:
            if __debug__:
                logger.debug("Init @mpmd_mpi decorator...")

            # Add <param_name>_layout params to SUPPORTED_ARGUMENTS
            for key in self.kwargs.keys():
                if "_layout" in key:
                    SUPPORTED_ARGUMENTS.add(key)

            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            self.decorator_name)

    def __call__(self, user_function):
        # type: (typing.Callable) -> typing.Callable
        """ Parse and set the mpmd mpi parameters within the task core element.

        :param user_function: User function to be decorated.
        :return: Decorated dummy user function, which will invoke MPMD MPI task.
        """

        @wraps(user_function)
        def mpmd_mpi_f(*args, **kwargs):
            return self.__decorator_body__(user_function, args, kwargs)

        mpmd_mpi_f.__doc__ = user_function.__doc__
        return mpmd_mpi_f

    def __decorator_body__(self, user_function, args, kwargs):
        if not self.scope:
            raise NotImplementedError

        if __debug__:
            logger.debug("Executing mpmd_mpi_f wrapper.")

        if (context.in_master() or context.is_nesting_enabled()) \
                and not self.core_element_configured:
            # master code - or worker with nesting enabled
            self.__configure_core_element__(kwargs, user_function)

        kwargs[PROCESSES_PER_NODE] = self.kwargs.get(PROCESSES_PER_NODE, 1)
        kwargs[COMPUTING_NODES] = self.processes

        with keep_arguments(args, kwargs, prepend_strings=False):
            # Call the method
            ret = user_function(*args, **kwargs)

        return ret

    def _get_programs_params(self):
        # type: () -> list
        """ Resolve the collection layout, such as blocks, strides, etc.

        :return: list(programs_length, binary, params, processes)
        :raises PyCOMPSsException: If programs are not dict objects.
        """
        programs = self.kwargs[PROGRAMS]
        programs_params = [str(len(programs))]

        for program in programs:
            if not isinstance(program, dict):
                raise PyCOMPSsException("Incorrect 'program' param in MPMD MPI")

            binary = program.get(BINARY, None)
            if not binary:
                raise PyCOMPSsException("No binary file provided for MPMD MPI")

            params = program.get(PARAMS, "[unassigned]")
            procs = str(program.get(PROCESSES, 1))
            programs_params.extend([binary, params, procs])

            # increase total # of processes for this mpmd task
            self.processes += program.get(PROCESSES, 1)

        return programs_params

    def __configure_core_element__(self, kwargs, user_function):
        # type: (dict, ...) -> None
        """ Include the registering info related to @mpmd_mpi.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :param user_function: Decorated function.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @mpmd_mpi core element.")

        # Resolve @mpmd_mpi specific parameters
        impl_type = "MPMDMPI"
        runner = self.kwargs[RUNNER]

        # Resolve the working directory
        resolve_working_dir(self.kwargs)
        # Resolve the fail by exit value
        resolve_fail_by_exit_value(self.kwargs)

        ppn = str(self.kwargs.get(PROCESSES_PER_NODE, 1))
        impl_signature = '.'.join((impl_type, str(ppn)))

        prog_params = self._get_programs_params()

        impl_args = [runner,
                     self.kwargs[WORKING_DIR],
                     ppn,
                     self.kwargs[FAIL_BY_EXIT_VALUE]]
        impl_args.extend(prog_params)

        if CORE_ELEMENT_KEY in kwargs:
            kwargs[CORE_ELEMENT_KEY].set_impl_type(impl_type)
            kwargs[CORE_ELEMENT_KEY].set_impl_signature(impl_signature)
            kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)
        else:
            core_element = CE()
            core_element.set_impl_type(impl_type)
            core_element.set_impl_signature(impl_signature)
            core_element.set_impl_type_args(impl_args)
            kwargs[CORE_ELEMENT_KEY] = core_element

        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# ##################### MPI DECORATOR ALTERNATIVE NAME ###################### #
# ########################################################################### #

mpmd_mpi = MPMDMPI
