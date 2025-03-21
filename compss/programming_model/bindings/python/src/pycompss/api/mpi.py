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
PyCOMPSs API - Mpi decorator.

This file contains the MPI class, needed for the mpi task definition through
the decorator.
"""

from functools import wraps

from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import INTERNAL_LABELS
from pycompss.api.commons.constants import LABELS
from pycompss.api.commons.constants import LEGACY_LABELS
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import process_computing_nodes
from pycompss.api.commons.decorator import resolve_fail_by_exit_value
from pycompss.api.commons.decorator import resolve_working_dir
from pycompss.api.commons.decorator import run_command
from pycompss.api.commons.implementation_types import IMPLEMENTATION_TYPES
from pycompss.runtime.task.definitions.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {LABELS.runner}
SUPPORTED_ARGUMENTS = {
    LABELS.binary,
    LABELS.processes,
    LABELS.working_dir,
    LABELS.runner,
    LABELS.flags,
    LABELS.processes_per_node,
    LABELS.scale_by_cu,
    LABELS.args,
    LABELS.fail_by_exit_value,
}
DEPRECATED_ARGUMENTS = {
    LEGACY_LABELS.computing_units,
    LEGACY_LABELS.computing_nodes,
    LEGACY_LABELS.working_dir,
}


class Mpi:  # pylint: disable=too-few-public-methods
    """Mpi decorator class.

    This decorator also preserves the argspec, but includes the __init__ and
    __call__ methods, useful on mpi task creation.
    """

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given mpi parameters.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        self.task_type = "mpi"
        self.decorator_name = "".join(("@", Mpi.__name__.lower()))
        # super(MPI, self).__init__(decorator_name, *args, **kwargs)
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        self.core_element = None  # type: typing.Optional[CE]
        self.core_element_configured = False
        if self.scope:
            if __debug__:
                logger.debug("Init @mpi decorator...")

            # TODO: Maybe add here the collection layout to avoid iterate
            #       twice per elements
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
        """Parse and set the mpi parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def mpi_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            return self.__decorator_body__(user_function, args, kwargs)

        mpi_f.__doc__ = user_function.__doc__
        return mpi_f

    def __decorator_body__(
        self, user_function: typing.Callable, args: tuple, kwargs: dict
    ) -> typing.Any:
        """Body of the mpi decorator.

        :param user_function: Decorated function.
        :param args: Function arguments.
        :param kwargs: Function keyword arguments.
        :returns: Result of executing the user_function with the given args
                  and kwargs.
        """
        if not self.scope:
            # Execute the mpi as with PyCOMPSs so that sequential
            # execution performs as parallel.
            # To disable: raise Exception(not_in_pycompss("mpi"))
            # TODO: Intercept @task parameters to get stream redirection
            if "binary" in self.kwargs:
                return self.__run_mpi__(args, kwargs)
            print(
                "WARN: Python MPI as dummy is not fully supported. "
                "Executing decorated function."
            )
            return user_function(*args, **kwargs)

        if __debug__:
            logger.debug("Executing mpi_f wrapper.")

        if (
            CONTEXT.in_master() or CONTEXT.is_nesting_enabled()
        ) and not self.core_element_configured:
            # master code - or worker with nesting enabled
            self.__configure_core_element__(kwargs)

        # The processes' parameter will have to go down until the execution
        # is invoked. To this end, set the computing_nodes variable in kwargs
        # for its usage in @task decorator
        # WARNING: processes can be an int, a env string, a str with
        #          dynamic variable name.
        if "processes" in self.kwargs:
            kwargs["computing_nodes"] = self.kwargs["processes"]
        else:
            # If processes not defined, check computing_units or set default
            process_computing_nodes(self.decorator_name, self.kwargs)
            kwargs["computing_nodes"] = self.kwargs["computing_nodes"]
        if "processes_per_node" in self.kwargs:
            kwargs["processes_per_node"] = self.kwargs["processes_per_node"]
        else:
            kwargs["processes_per_node"] = 1
        if __debug__:
            logger.debug(
                "This MPI task will have %s processes "
                "and %s processes per node.",
                str(kwargs["computing_nodes"]),
                str(kwargs["processes_per_node"]),
            )

        prepend_strings = self.task_type == IMPLEMENTATION_TYPES.python_mpi

        with keep_arguments(args, kwargs, prepend_strings=prepend_strings):
            # Call the method
            ret = user_function(*args, **kwargs)

        return ret

    def __run_mpi__(self, args: tuple, kwargs: dict) -> int:
        """Run the mpi binary defined in the decorator when used as dummy.

        :param args: Arguments received from call.
        :param kwargs: Keyword arguments received from call.
        :return: Execution return code.
        """
        cmd = [self.kwargs[LABELS.runner]]
        if LABELS.processes in self.kwargs:
            cmd += ["-np", self.kwargs[LABELS.processes]]
        elif LABELS.computing_nodes in self.kwargs:
            cmd += ["-np", self.kwargs[LABELS.computing_nodes]]
        elif LEGACY_LABELS.computing_nodes in self.kwargs:
            cmd += ["-np", self.kwargs[LEGACY_LABELS.computing_nodes]]

        if LABELS.flags in self.kwargs:
            cmd += self.kwargs[LABELS.flags].split()
        cmd += [self.kwargs[LABELS.binary]]

        return run_command(cmd, args, kwargs)

    def __resolve_collection_layout_params__(self) -> list:
        """Resolve the collection layout, such as blocks, strides, etc.

        :return: list(param_name, block_count, block_length, stride).
        :raises PyCOMPSsException: If the collection layout does not contain
                                   block_count.
        """
        num_layouts = 0
        layout_params = []
        for key, value in self.kwargs.items():
            if "_layout" in key:
                num_layouts += 1
                param_name = key.split("_layout")[0]
                collection_layout = value

                block_count = self.__get_block_count__(collection_layout)
                block_length = self.__get_block_length__(collection_layout)
                stride = self.__get_stride__(collection_layout)

                if (block_length != -1 and block_count == -1) or (
                    stride != -1 and block_count == -1
                ):
                    msg = "Error: collection_layout must contain block_count!"
                    raise PyCOMPSsException(msg)
                layout_params.extend(
                    [
                        param_name,
                        str(block_count),
                        str(block_length),
                        str(stride),
                    ]
                )
        layout_params.insert(0, str(num_layouts))
        return layout_params

    @staticmethod
    def __get_block_count__(collection_layout: dict) -> int:
        """Get the block count from the given collection layout.

        :param collection_layout: Collection layout.
        :return: Block count value.
        """
        if "block_count" in collection_layout:
            return collection_layout["block_count"]
        return -1

    @staticmethod
    def __get_block_length__(collection_layout: dict) -> int:
        """Get the block length from the given collection layout.

        :param collection_layout: Collection layout.
        :return: Block length value.
        """
        if "block_length" in collection_layout:
            return collection_layout["block_length"]
        return -1

    @staticmethod
    def __get_stride__(collection_layout: dict) -> int:
        """Get the stride from the given collection layout.

        :param collection_layout: Collection layout.
        :return: Stride value.
        """
        if "stride" in collection_layout:
            return collection_layout["stride"]
        return -1

    def __configure_core_element__(  # pylint: disable=too-many-branches
        self, kwargs: dict
    ) -> None:
        """Include the registering info related to @mpi.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @mpi core element.")

        # Resolve @mpi specific parameters
        if LABELS.binary in self.kwargs:
            binary = self.kwargs[LABELS.binary]
            impl_type = IMPLEMENTATION_TYPES.mpi
        else:
            binary = INTERNAL_LABELS.unassigned
            impl_type = IMPLEMENTATION_TYPES.python_mpi
            self.task_type = impl_type

        runner = self.kwargs[LABELS.runner]

        if LABELS.flags in self.kwargs:
            flags = self.kwargs[LABELS.flags]
        else:
            flags = (
                INTERNAL_LABELS.unassigned
            )  # Empty or INTERNAL_LABELS.unassigned

        # Check if scale by cu is defined
        scale_by_cu_str = self.__resolve_scale_by_cu__()

        # Resolve the working directory
        resolve_working_dir(self.kwargs)
        # Resolve the fail by exit value
        resolve_fail_by_exit_value(self.kwargs)
        # Resolve parameter collection layout
        collection_layout_params = self.__resolve_collection_layout_params__()

        if LABELS.processes in self.kwargs:
            proc = self.kwargs[LABELS.processes]
        elif LABELS.computing_nodes in self.kwargs:
            proc = self.kwargs[LABELS.computing_nodes]
        elif LEGACY_LABELS.computing_nodes in self.kwargs:
            proc = self.kwargs[LEGACY_LABELS.computing_nodes]
        else:
            proc = "1"

        if LABELS.processes_per_node in self.kwargs:
            ppn = str(self.kwargs[LABELS.processes_per_node])
        else:
            ppn = "1"

        if binary == INTERNAL_LABELS.unassigned:
            impl_signature = impl_type + "."
        else:
            impl_signature = ".".join((impl_type, str(proc), binary))

        impl_args = [
            binary,
            self.kwargs[LABELS.working_dir],
            runner,
            ppn,
            flags,
            scale_by_cu_str,
            self.kwargs.get(LABELS.args, INTERNAL_LABELS.unassigned),
            self.kwargs[LABELS.fail_by_exit_value],
        ]

        if impl_type == IMPLEMENTATION_TYPES.python_mpi:
            impl_args = impl_args + collection_layout_params

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

    def __resolve_scale_by_cu__(self) -> str:
        """Check if scale_by_cu is defined and process it.

        :return: Scale by cu value as string.
        :raises PyCOMPSsException: If scale_by_cu is not bool or string.
        """
        if LABELS.scale_by_cu in self.kwargs:
            scale_by_cu = self.kwargs[LABELS.scale_by_cu]
            if isinstance(scale_by_cu, bool):
                if scale_by_cu:
                    scale_by_cu_str = "true"
                else:
                    scale_by_cu_str = "false"
            elif str(scale_by_cu).lower() in ["true", "false"]:
                scale_by_cu_str = str(scale_by_cu).lower()
            else:
                raise PyCOMPSsException(
                    "Incorrect format for scale_by_cu property. "
                    "It should be boolean or 'true' or 'false'"
                )
        else:
            scale_by_cu_str = "false"
        return scale_by_cu_str


# ########################################################################### #
# ##################### MPI DECORATOR ALTERNATIVE NAME ###################### #
# ########################################################################### #

mpi = Mpi  # pylint: disable=invalid-name
