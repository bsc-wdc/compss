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
PyCOMPSs API - Container decorator.

This file contains the Container class, needed for the container task
definition through the decorator.
"""

from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import INTERNAL_LABELS
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.constants import LEGACY_LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.api.commons.implementation_types import IMPLEMENTATION_TYPES
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {LABELS.engine, LABELS.image}
SUPPORTED_ARGUMENTS = {LABELS.engine, LABELS.image, LABELS.options}
DEPRECATED_ARGUMENTS = {
    LABELS.fail_by_exit_value,
    LABELS.working_dir,
    LEGACY_LABELS.working_dir,
    LABELS.binary,
}


class Container:  # pylint: disable=too-few-public-methods
    """Container decorator class.

    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given constraints.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = "@" + Container.__name__.lower()
        # super(Container, self).__init__(decorator_name, *args, **kwargs)
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        self.core_element = None  # type: typing.Optional[CE]
        self.core_element_configured = False
        if self.scope:
            if __debug__:
                logger.debug("Init @container decorator...")
            # Check the arguments
            check_arguments(
                MANDATORY_ARGUMENTS,
                DEPRECATED_ARGUMENTS,
                SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                list(kwargs.keys()),
                decorator_name,
            )

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """Parse and set the container parameters within the task core element.

        :param user_function: Function to decorate
        :return: Decorated function.
        """

        @wraps(user_function)
        def container_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            if not self.scope:
                raise NotInPyCOMPSsException(not_in_pycompss("container"))

            if __debug__:
                logger.debug("Executing container_f wrapper.")

            if (
                CONTEXT.in_master() or CONTEXT.is_nesting_enabled()
            ) and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs, user_function)

            with keep_arguments(args, kwargs, prepend_strings=False):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        container_f.__doc__ = user_function.__doc__
        return container_f

    def __configure_core_element__(
        self, kwargs: dict, user_function: typing.Callable
    ) -> None:
        """Include the registering info related to @container.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :param user_function: Decorated function.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @container core element.")

        # Resolve @container (mandatory) specific parameters
        _engine = self.kwargs[LABELS.engine]
        _image = self.kwargs[LABELS.image]
        _func = str(user_function.__name__)

        # Type and signature
        impl_type = IMPLEMENTATION_TYPES.container
        impl_signature = ".".join([impl_type, _func])

        impl_args = [
            _engine,  # engine
            _image,  # image
            self.kwargs.get(LABELS.options, INTERNAL_LABELS.unassigned),
            INTERNAL_LABELS.unassigned,  # internal_type
            INTERNAL_LABELS.unassigned,  # internal_binary
            INTERNAL_LABELS.unassigned,  # internal_func
            INTERNAL_LABELS.unassigned,  # working_dir
            INTERNAL_LABELS.unassigned,
        ]  # fail_by_ev

        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @constraint)
            kwargs[CORE_ELEMENT_KEY].set_impl_type(impl_type)
            kwargs[CORE_ELEMENT_KEY].set_impl_signature(impl_signature)
            kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)
            kwargs[CORE_ELEMENT_KEY].set_impl_container(impl_args[:3])
        else:
            # @container is in the top of the decorators stack.
            # Instantiate a new core element object, update it and include
            # it into kwarg
            core_element = CE()
            core_element.set_impl_type(impl_type)
            core_element.set_impl_signature(impl_signature)
            core_element.set_impl_type_args(impl_args)
            core_element.set_impl_container(impl_args[:3])
            kwargs[CORE_ELEMENT_KEY] = core_element

        # Set as configured
        self.core_element_configured = True


# ########################################################################### #
# ################# CONTAINER DECORATOR ALTERNATIVE NAME #################### #
# ########################################################################### #

container = Container  # pylint: disable=invalid-name
