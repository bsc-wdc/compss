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
PyCOMPSs API - Reduction decorator.

This file contains the Reduction class, needed for the reduction of data
elements task definition.
"""

import os
from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.error_msgs import cast_env_to_int_error
from pycompss.api.commons.error_msgs import cast_string_to_int_error
from pycompss.api.dummy.reduction import reduction as dummy_reduction
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing

# Used only for typing - disabled formatting due to multiline.
# fmt: off
from pycompss.runtime.\
    task.definitions.core_element import CE  # pylint: disable=unused-import
# fmt: on

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = set()  # type: typing.Set[str]
SUPPORTED_ARGUMENTS = {LABELS.chunk_size, LABELS.is_reduce}
DEPRECATED_ARGUMENTS = set()  # type: typing.Set[str]


class Reduction:  # pylint: disable=too-few-public-methods
    """Reduction decorator class.

    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on Reduction task creation.
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
        kwargs = dictionary with the given Reduce parameters

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = "".join(("@", Reduction.__name__.lower()))
        # super(Reduction, self).__init__(decorator_name, *args, **kwargs)
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
            self.__process_reduction_params__()

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the reduce parameters within the task core element.

        :param user_function: Function to decorate
        :return: Decorated function.
        """

        @wraps(user_function)
        def reduce_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            if not self.scope:
                d_r = dummy_reduction(self.args, self.kwargs)
                return d_r.__call__(user_function)(*args, **kwargs)

            if __debug__:
                logger.debug("Executing reduce_f wrapper.")

            # Set the chunk size and is_reduce variables in kwargs for their
            # usage in @task decorator
            kwargs[LABELS.chunk_size] = self.kwargs[LABELS.chunk_size]
            kwargs[LABELS.is_reduce] = self.kwargs[LABELS.is_reduce]

            with keep_arguments(args, kwargs, prepend_strings=False):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        reduce_f.__doc__ = user_function.__doc__
        return reduce_f

    def __process_reduction_params__(self) -> None:
        """Process the chunk size and is_reduce from the decorator.

        :return: None
        """
        # Resolve @reduce specific parameters
        if LABELS.chunk_size not in self.kwargs:
            chunk_size = 0
        else:
            chunk_size_kw = self.kwargs[LABELS.chunk_size]
            if isinstance(chunk_size_kw, int):
                chunk_size = chunk_size_kw
            elif isinstance(chunk_size_kw, str):
                # Convert string to int
                chunk_size = self.__parse_chunk_size__(chunk_size_kw)
            else:
                raise PyCOMPSsException(
                    "ERROR: Wrong chunk_size value at @reduction decorator."
                )

        if LABELS.is_reduce not in self.kwargs:
            is_reduce = True
        else:
            is_reduce = self.kwargs[LABELS.is_reduce]

        if __debug__:
            logger.debug(
                "The task is_reduce flag is set to: %s", str(is_reduce)
            )
            logger.debug(
                "This Reduction task will have %s sized chunks",
                str(chunk_size),
            )

        # Set the chunk_size variable in kwargs for its usage in @task
        self.kwargs[LABELS.chunk_size] = chunk_size
        self.kwargs[LABELS.is_reduce] = is_reduce

    @staticmethod
    def __parse_chunk_size__(chunk_size: str) -> int:
        """Parse chunk size as string and returns its value as integer.

        :param chunk_size: Chunk size as string.
        :return: Chunk size as integer.
        :raises PyCOMPSsException: Can not cast string to int error.
        """
        # Check if it is an environment variable to be loaded
        if chunk_size.strip().startswith("$"):
            # Chunk size is an ENV variable, load it
            env_var = chunk_size.strip()[1:]  # Remove $
            if env_var.startswith("{"):
                env_var = env_var[1:-1]  # remove brackets
            try:
                parsed_chunk_size = int(os.environ[env_var])
            except ValueError as chunk_size_env_var_error:
                raise PyCOMPSsException(
                    cast_env_to_int_error(LABELS.chunk_size)
                ) from chunk_size_env_var_error
        else:
            # ChunkSize is in string form, cast it
            try:
                parsed_chunk_size = int(chunk_size)
            except ValueError as chunk_size_error:
                raise PyCOMPSsException(
                    cast_string_to_int_error(LABELS.chunk_size)
                ) from chunk_size_error
        return parsed_chunk_size


# ########################################################################### #
# ################# REDUCTION DECORATOR ALTERNATIVE NAME #################### #
# ########################################################################### #

reduction = Reduction  # pylint: disable=invalid-name
