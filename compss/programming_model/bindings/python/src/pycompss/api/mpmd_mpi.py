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
PyCOMPSs API - MPMD MPI decorator.

This file contains the mpmd mpi class, needed for the multiple program mpi
task definition through the decorator.
"""

from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import INTERNAL_LABELS
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import resolve_fail_by_exit_value
from pycompss.api.commons.decorator import resolve_working_dir
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {LABELS.runner}
SUPPORTED_ARGUMENTS = {
    LABELS.runner,
    LABELS.programs,
    LABELS.working_dir,
    LABELS.processes_per_node,
    LABELS.fail_by_exit_value,
}
DEPRECATED_ARGUMENTS = set()  # type: typing.Set[str]


class MPMDMPI:  # pylint: disable=R0903,R0902
    # disable=too-few-public-methods, too-many-instance-attributes
    """MPMDMPI decorator class.

    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpmd_mpi task creation.
    """

    __slots__ = [
        "decorator_name",
        "args",
        "kwargs",
        "scope",
        "core_element",
        "core_element_configured",
        "task_type",
        "processes",
    ]

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given mpi parameters.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = "".join(("@", MPMDMPI.__name__.lower()))
        # super(MPMDMPI, self).__init__(decorator_name, *args, **kwargs)
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        self.core_element = None  # type: typing.Optional[CE]
        self.core_element_configured = False
        # MPMD_MPI specific:
        self.task_type = "mpmd_mpi"
        self.processes = 0
        if self.scope:
            if __debug__:
                logger.debug("Init @mpmd_mpi decorator...")

            # Add <param_name>_layout params to SUPPORTED_ARGUMENTS
            for key in self.kwargs:
                if "_layout" in key:
                    SUPPORTED_ARGUMENTS.add(key)

            # Check the arguments
            check_arguments(
                MANDATORY_ARGUMENTS,
                DEPRECATED_ARGUMENTS,
                SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                list(kwargs.keys()),
                self.decorator_name,
            )

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the mpmd mpi parameters within the task core element.

        :param user_function: User function to be decorated.
        :return: Decorated dummy user function, which will invoke MPMD MPI
                 task.
        """

        @wraps(user_function)
        def mpmd_mpi_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            return self.__decorator_body__(user_function, args, kwargs)

        mpmd_mpi_f.__doc__ = user_function.__doc__
        return mpmd_mpi_f

    def __decorator_body__(
        self, user_function: typing.Callable, args: tuple, kwargs: dict
    ) -> typing.Any:
        """Body of the mpmd_mpi decorator.

        :param user_function: Decorated function.
        :param args: Function arguments.
        :param kwargs: Function keyword arguments.
        :returns: Result of executing the user_function with the given args
                  and kwargs.
        """
        if not self.scope:
            raise NotImplementedError

        if __debug__:
            logger.debug("Executing mpmd_mpi_f wrapper.")

        if (
            CONTEXT.in_master() or CONTEXT.is_nesting_enabled()
        ) and not self.core_element_configured:
            # master code - or worker with nesting enabled
            self.__configure_core_element__(kwargs)

        kwargs[LABELS.processes_per_node] = self.kwargs.get(
            LABELS.processes_per_node, 1
        )
        kwargs[LABELS.computing_nodes] = self.processes

        with keep_arguments(args, kwargs, prepend_strings=False):
            # Call the method
            ret = user_function(*args, **kwargs)

        return ret

    def __get_programs_params__(self) -> list:
        """Resolve the collection layout, such as blocks, strides, etc.

        :return: list(programs_length, binary, args, processes)
        :raises PyCOMPSsException: If programs are not dict objects.
        """
        programs = self.kwargs[LABELS.programs]
        programs_args = [str(len(programs))]

        for program in programs:
            if not isinstance(program, dict):
                raise PyCOMPSsException(
                    "Incorrect 'program' param in MPMD MPI"
                )

            binary = program.get(LABELS.binary, None)
            if not binary:
                raise PyCOMPSsException("No binary file provided for MPMD MPI")

            params = program.get(LABELS.args, INTERNAL_LABELS.unassigned)
            procs = str(program.get(LABELS.processes, 1))
            programs_args.extend([binary, params, procs])

            # increase total # of processes for this mpmd task
            self.processes += program.get(LABELS.processes, 1)

        return programs_args

    def __configure_core_element__(self, kwargs: dict) -> None:
        """Include the registering info related to @mpmd_mpi.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @mpmd_mpi core element.")

        # Resolve @mpmd_mpi specific parameters
        impl_type = "MPMDMPI"
        runner = self.kwargs[LABELS.runner]

        # Resolve the working directory
        resolve_working_dir(self.kwargs)
        # Resolve the fail by exit value
        resolve_fail_by_exit_value(self.kwargs)

        ppn = str(self.kwargs.get(LABELS.processes_per_node, 1))
        impl_signature = ".".join((impl_type, str(ppn)))

        prog_params = self.__get_programs_params__()

        impl_args = [
            runner,
            self.kwargs[LABELS.working_dir],
            ppn,
            self.kwargs[LABELS.fail_by_exit_value],
        ]
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

mpmd_mpi = MPMDMPI  # pylint: disable=invalid-name
