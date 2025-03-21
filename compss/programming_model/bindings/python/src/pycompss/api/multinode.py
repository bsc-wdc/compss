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
PyCOMPSs API - Multinode decorator.

This file contains the Multinode class, needed for the MultiNode task
definition through the decorator.
"""

import os
from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.constants import LEGACY_LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import process_computing_nodes
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.implementation_types import IMPLEMENTATION_TYPES
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = set()  # type: typing.Set[str]
SUPPORTED_ARGUMENTS = {LABELS.computing_nodes, LABELS.processes_per_node}
DEPRECATED_ARGUMENTS = {LEGACY_LABELS.computing_nodes}
SLURM_SKIP_VARS = [
    "SLURM_JOBID",
    "SLURM_JOB_ID",
    "SLURM_USER",
    "SLURM_QOS",
    "SLURM_PARTITION",
    "SLURM_OVERLAP",
]


class MultiNode:  # pylint: disable=too-few-public-methods
    """MultiNode decorator class.

    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on MultiNode task creation.
    """

    __slots__ = [
        "decorator_name",
        "args",
        "kwargs",
        "scope",
        "core_element",
        "core_element_configured",
    ]

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given constraints.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = "".join(("@", MultiNode.__name__.lower()))
        # super(MultiNode, self).__init__(decorator_name, *args, **kwargs)
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        self.core_element = None  # type: typing.Optional[CE]
        self.core_element_configured = False
        if self.scope:
            # Check the arguments
            check_arguments(
                MANDATORY_ARGUMENTS,
                DEPRECATED_ARGUMENTS,
                SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                list(kwargs.keys()),
                decorator_name,
            )

            # Get the computing nodes
            process_computing_nodes(decorator_name, self.kwargs)
            if LABELS.processes_per_node not in kwargs:
                kwargs[LABELS.processes_per_node] = 1

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the multinode parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def multinode_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            if not self.scope:
                raise NotInPyCOMPSsException(not_in_pycompss("MultiNode"))

            if __debug__:
                logger.debug("Executing multinode_f wrapper.")

            if (
                CONTEXT.in_master() or CONTEXT.is_nesting_enabled()
            ) and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs)

            if CONTEXT.in_worker():
                old_slurm_env = set_slurm_environment()

            # Set the computing_nodes variable in kwargs for its usage
            # in @task decorator
            kwargs[LABELS.computing_nodes] = self.kwargs[
                LABELS.computing_nodes
            ]
            if LABELS.processes_per_node in self.kwargs:
                kwargs[LABELS.processes_per_node] = self.kwargs[
                    LABELS.processes_per_node
                ]
            with keep_arguments(args, kwargs, prepend_strings=True):
                # Call the method
                ret = user_function(*args, **kwargs)

            if CONTEXT.in_worker():
                reset_slurm_environment(old_slurm_env)

            return ret

        multinode_f.__doc__ = user_function.__doc__
        return multinode_f

    def __configure_core_element__(self, kwargs: dict) -> None:
        """Include the registering info related to @multinode.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @multinode core element.")

        # Resolve @multinode specific parameters
        impl_type = IMPLEMENTATION_TYPES.multi_node
        if LABELS.processes_per_node in self.kwargs:
            ppn = str(self.kwargs[LABELS.processes_per_node])
        else:
            ppn = "1"
        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @constraint)
            kwargs[CORE_ELEMENT_KEY].set_impl_type(impl_type)
        else:
            # @binary is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            core_element = CE()
            core_element.set_impl_type(impl_type)
            kwargs[CORE_ELEMENT_KEY] = core_element
        if __debug__:
            logger.debug("Adding ppn in core element impl type args.")
        kwargs[CORE_ELEMENT_KEY].set_impl_type_args([ppn])

        # Set as configured
        self.core_element_configured = True


def set_slurm_environment() -> dict:
    """Set SLURM environment.

    :return: old Slurm environment
    """
    num_nodes = int(os.environ["COMPSS_NUM_NODES"])
    num_threads = int(os.environ["COMPSS_NUM_THREADS"])
    total_processes = num_nodes * num_threads
    hostnames = os.environ["COMPSS_HOSTNAMES"]
    nodes = set(hostnames.split(","))
    old_slurm_env = remove_slurm_environment()
    # set slurm environment with COMPSs variables
    os.environ["SLURM_NTASKS"] = str(total_processes)
    os.environ["SLURM_NNODES"] = str(num_nodes)
    os.environ["SLURM_JOB_NUM_NODES"] = str(num_nodes)
    os.environ["SLURM_NODELIST"] = ",".join(nodes)
    os.environ["SLURM_JOB_NODELIST"] = ",".join(nodes)
    os.environ["SLURM_TASKS_PER_NODE"] = "".join(
        (str(num_threads), "(x", str(num_nodes), ")")
    )
    os.environ["SLURM_CPUS_PER_NODE"] = "".join(
        (str(num_threads), "(x", str(num_nodes), ")")
    )
    return old_slurm_env


def remove_slurm_environment() -> dict:
    """Remove the Slurm variables from the environment.

    :return: removed Slurm variables.
    """
    old_slurm_env = {}
    for key, value in os.environ.items():
        if key.startswith("SLURM"):
            if key not in SLURM_SKIP_VARS:
                old_slurm_env[key] = value
                os.environ.pop(key)
    # TODO: ISSUE DECTECTED - WAS NOT RETURNING old_slurm_env: ASK JORGE
    return old_slurm_env


def reset_slurm_environment(
    old_slurm_env: typing.Optional[dict] = None,
) -> None:
    """Reestablish SLURM environment.

    :return: None
    """
    if old_slurm_env:
        for key, value in old_slurm_env.items():
            os.environ[key] = value


# ########################################################################### #
# ################## MultiNode DECORATOR ALTERNATIVE NAME ################### #
# ########################################################################### #

multinode = MultiNode  # pylint: disable=invalid-name
