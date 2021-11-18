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
PyCOMPSs API - CONTAINER
=====================
    This file contains the class constraint, needed for the container task
    definition through the decorator.
"""

from pycompss.util.typing_helper import typing
from functools import wraps

import pycompss.util.context as context
from pycompss.api.commons.constants import BINARY
from pycompss.api.commons.constants import WORKING_DIR
from pycompss.api.commons.constants import FAIL_BY_EXIT_VALUE
from pycompss.api.commons.constants import LEGACY_WORKING_DIR
from pycompss.api.commons.constants import ENGINE
from pycompss.api.commons.constants import IMAGE
from pycompss.api.commons.constants import UNASSIGNED
from pycompss.api.commons.implementation_types import IMPL_CONTAINER
from pycompss.api.commons.error_msgs import not_in_pycompss
from pycompss.util.exceptions import NotInPyCOMPSsException
from pycompss.util.arguments import check_arguments
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.runtime.task.core_element import CE

if __debug__:
    import logging
    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {ENGINE,
                       IMAGE}
SUPPORTED_ARGUMENTS = {ENGINE,
                       IMAGE}
DEPRECATED_ARGUMENTS = {FAIL_BY_EXIT_VALUE,
                        WORKING_DIR,
                        LEGACY_WORKING_DIR,
                        BINARY}


class Container(object):
    """
    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """

    def __init__(self, *args, **kwargs):
        # type: (*typing.Any, **typing.Any) -> None
        """
        Store arguments passed to the decorator
        # self = itself.
        # args = not used.
        # kwargs = dictionary with the given constraints.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = "@" + Container.__name__.lower()
        # super(Container, self).__init__(decorator_name, *args, **kwargs)
        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = context.in_pycompss()
        self.core_element = None  # type: typing.Any
        self.core_element_configured = False
        if self.scope:
            if __debug__:
                logger.debug("Init @container decorator...")
            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            decorator_name)

    def __call__(self, user_function):
        # type: (typing.Any) -> typing.Any
        """
        Parse and set the container parameters within the task core element.

        :param user_function: Function to decorate
        :return: Decorated function.
        """

        @wraps(user_function)
        def container_f(*args, **kwargs):
            # type: (*typing.Any, **typing.Any) -> typing.Any
            if not self.scope:
                raise NotInPyCOMPSsException(not_in_pycompss("container"))

            if __debug__:
                logger.debug("Executing container_f wrapper.")

            if (context.in_master() or context.is_nesting_enabled()) \
                    and not self.core_element_configured:
                # master code - or worker with nesting enabled
                self.__configure_core_element__(kwargs, user_function)

            with keep_arguments(args, kwargs, prepend_strings=False):
                # Call the method
                ret = user_function(*args, **kwargs)

            return ret

        container_f.__doc__ = user_function.__doc__
        return container_f

    def __configure_core_element__(self, kwargs, user_function):
        # type: (dict, typing.Any) -> None
        """ Include the registering info related to @container.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :param user_function: Decorated function.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @container core element.")

        # Resolve @container (mandatory) specific parameters
        _engine = self.kwargs[ENGINE]
        _image = self.kwargs[IMAGE]

        _func = str(user_function.__name__)

        # Type and signature
        impl_type = IMPL_CONTAINER
        impl_signature = ".".join([impl_type, _func])

        impl_args = [_engine,     # engine
                     _image,      # image
                     UNASSIGNED,  # internal_type
                     UNASSIGNED,  # internal_binary
                     UNASSIGNED,  # internal_func
                     UNASSIGNED,  # working_dir
                     UNASSIGNED]  # fail_by_ev

        if CORE_ELEMENT_KEY in kwargs:
            # Core element has already been created in a higher level decorator
            # (e.g. @constraint)
            kwargs[CORE_ELEMENT_KEY].set_impl_type(impl_type)
            kwargs[CORE_ELEMENT_KEY].set_impl_signature(impl_signature)
            kwargs[CORE_ELEMENT_KEY].set_impl_type_args(impl_args)
        else:
            # @container is in the top of the decorators stack.
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
# ################# CONTAINER DECORATOR ALTERNATIVE NAME #################### #
# ########################################################################### #

container = Container
