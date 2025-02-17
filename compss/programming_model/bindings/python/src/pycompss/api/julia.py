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
PyCOMPSs API - Julia decorator.

This file contains the Julia class, needed for the Julia task definition
through the decorator.
"""

from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.constants import LEGACY_LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import process_computing_nodes
from pycompss.api.commons.decorator import resolve_fail_by_exit_value
from pycompss.api.commons.decorator import resolve_working_dir
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.implementation_types import IMPLEMENTATION_TYPES
from pycompss.api.multinode import set_slurm_environment
from pycompss.api.multinode import reset_slurm_environment
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {LABELS.julia_script}
SUPPORTED_ARGUMENTS = {
    LABELS.julia_executor,
    LABELS.julia_args,
    LABELS.julia_script,
    LABELS.fail_by_exit_value,
    LABELS.working_dir,
    LABELS.computing_nodes,
}
DEPRECATED_ARGUMENTS = {
    LEGACY_LABELS.working_dir,
    LEGACY_LABELS.computing_nodes,
}


class Julia:  # pylint: disable=too-few-public-methods
    """Julia decorator class.

    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on julia task creation.
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

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        """
        decorator_name = "".join(("@", Julia.__name__.lower()))
        # super(Julia, self).__init__(decorator_name, *args, **kwargs)
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

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the julia parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def julia_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            if not self.scope:
                raise NotInPyCOMPSsException(not_in_pycompss("julia"))

            if __debug__:
                logger.debug("Executing julia_f wrapper.")

            if (
                CONTEXT.in_master() or CONTEXT.is_nesting_enabled()
            ) and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs)

            multinode_decorator_defined = False
            old_slurm_env = {}
            if LABELS.computing_nodes in kwargs:
                multinode_decorator_defined = True
                # We need to do what @multinode does:
                if CONTEXT.in_worker():
                    old_slurm_env = set_slurm_environment()

            # Set the computing_nodes variable in kwargs for its usage
            # in @task decorator
            kwargs[LABELS.computing_nodes] = self.kwargs[
                LABELS.computing_nodes
            ]

            with keep_arguments(args, kwargs, prepend_strings=False):
                # Call the method
                ret = user_function(*args, **kwargs)

            if multinode_decorator_defined and CONTEXT.in_worker():
                reset_slurm_environment(old_slurm_env)

            return ret

        julia_f.__doc__ = user_function.__doc__
        return julia_f

    def __configure_core_element__(self, kwargs: dict) -> None:
        """Include the registering info related to @julia.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @julia core element.")

        # Resolve @julia specific parameters
        julia_executor = "julia"
        if LABELS.julia_executor in self.kwargs:
            julia_executor = self.kwargs[LABELS.julia_executor]

        julia_args = ""
        if LABELS.julia_args in self.kwargs:
            julia_args = self.kwargs[LABELS.julia_args]

        julia_script = self.kwargs[LABELS.julia_script]

        # Resolve computing nodes
        if LABELS.computing_nodes in kwargs:
            # It has been defined in @multinode
            computing_nodes = kwargs[LABELS.computing_nodes]
        elif LABELS.computing_nodes in self.kwargs:
            # It has been defined in @julia
            computing_nodes = self.kwargs[LABELS.computing_nodes]
        else:
            # Not defined anywhere
            computing_nodes = "1"
        self.kwargs[LABELS.computing_nodes] = computing_nodes

        # Resolve the working directory
        resolve_working_dir(self.kwargs)
        # Resolve the fail by exit value
        resolve_fail_by_exit_value(self.kwargs)

        impl_type = IMPLEMENTATION_TYPES.julia
        impl_signature = ".".join((impl_type, julia_script))
        impl_args = [
            julia_executor,
            julia_args,
            julia_script,
            self.kwargs[LABELS.working_dir],
            self.kwargs[LABELS.fail_by_exit_value],
            self.kwargs[LABELS.computing_nodes],
        ]

        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @constraint)
            kwargs[CORE_ELEMENT_KEY].set_impl_type(impl_type)
            kwargs[CORE_ELEMENT_KEY].set_impl_signature(impl_signature)
            kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)
        else:
            # @binary is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            core_element = CE()
            core_element.set_impl_type(impl_type)
            core_element.set_impl_signature(impl_signature)
            core_element.set_impl_type_args(impl_args)
            kwargs[CORE_ELEMENT_KEY] = core_element

        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# #################### DECAF DECORATOR ALTERNATIVE NAME ##################### #
# ########################################################################### #

julia = Julia  # pylint: disable=invalid-name
