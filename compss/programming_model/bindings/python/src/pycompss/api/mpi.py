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
PyCOMPSs API - MPI
==================
    This file contains the class mpi, needed for the mpi
    definition through the decorator.
"""

from functools import wraps
import pycompss.util.context as context
from pycompss.api.commons.decorator import PyCOMPSsDecorator
from pycompss.api.commons.decorator import keep_arguments
from pycompss.api.commons.decorator import CORE_ELEMENT_KEY
from pycompss.api.commons.decorator import run_command
from pycompss.runtime.task.core_element import CE
from pycompss.util.arguments import check_arguments
from pycompss.util.exceptions import PyCOMPSsException


if __debug__:
    import logging

    logger = logging.getLogger(__name__)

MANDATORY_ARGUMENTS = {'runner'}
SUPPORTED_ARGUMENTS = {'binary',
                       'processes',
                       'working_dir',
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

    __slots__ = ['task_type']

    def __init__(self, *args, **kwargs):
        """ Store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given mpi parameters.

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        self.task_type = "mpi"
        decorator_name = "".join(('@', MPI.__name__.lower()))
        super(MPI, self).__init__(decorator_name, *args, **kwargs)
        if self.scope:
            if __debug__:
                logger.debug("Init @mpi decorator...")

            # noqa TODO: Maybe add here the collection layout to avoid iterate twice per elements
            # Add <param_name>_layout params to SUPPORTED_ARGUMENTS
            for key in self.kwargs.keys():
                if "_layout" in key:
                    SUPPORTED_ARGUMENTS.add(key)

            # Check the arguments
            check_arguments(MANDATORY_ARGUMENTS,
                            DEPRECATED_ARGUMENTS,
                            SUPPORTED_ARGUMENTS | DEPRECATED_ARGUMENTS,
                            list(kwargs.keys()),
                            decorator_name)

            # Get the computing nodes
            self.__process_computing_nodes__(decorator_name)

            # Set default value if it has not been defined
            if 'processes' not in self.kwargs:
                self.kwargs['processes'] = 1

            # The processes parameter will have to go down until the execution
            # is invoked.
            # WARNING: processes can be an int, a env string, a str with
            #          dynamic variable name.
            if __debug__:
                logger.debug("This MPI task will have " +
                             str(self.kwargs['processes']) + " processes.")

    def __call__(self, user_function):
        """ Parse and set the mpi parameters within the task core element.

        :param user_function: Function to decorate.
        :return: Decorated function.
        """

        @wraps(user_function)
        def mpi_f(*args, **kwargs):
            return self.__decorator_body__(user_function, args, kwargs)

        mpi_f.__doc__ = user_function.__doc__
        return mpi_f

    def __decorator_body__(self, user_function, args, kwargs):
        if not self.scope:
            # Execute the mpi as with PyCOMPSs so that sequential
            # execution performs as parallel.
            # To disable: raise Exception(not_in_pycompss("mpi"))
            # TODO: Intercept @task parameters to get stream redirection
            return self.__run_mpi__(args, kwargs)

        if __debug__:
            logger.debug("Executing mpi_f wrapper.")

        if (context.in_master() or context.is_nesting_enabled()) \
                and not self.core_element_configured:
            # master code - or worker with nesting enabled
            self.__configure_core_element__(kwargs, user_function)

        # Set the computing_nodes variable in kwargs for its usage
        # in @task decorator
        kwargs['computing_nodes'] = self.kwargs['processes']

        if self.task_type == "PYTHON_MPI":
            prepend_strings = True
        else:
            prepend_strings = False

        with keep_arguments(args, kwargs, prepend_strings=prepend_strings):
            # Call the method
            ret = user_function(*args, **kwargs)

        return ret

    def __run_mpi__(self, *args, **kwargs):
        # type: (..., dict) -> int
        """ Runs the mpi binary defined in the decorator when used as dummy.

        :param args: Arguments received from call.
        :param kwargs: Keyword arguments received from call.
        :return: Execution return code.
        """
        cmd = [self.kwargs['runner']]
        if 'processes' in self.kwargs:
            cmd += ['-np', self.kwargs['processes']]
        elif 'computing_nodes' in self.kwargs:
            cmd += ['-np', self.kwargs['computing_nodes']]
        elif 'computingNodes' in self.kwargs:
            cmd += ['-np', self.kwargs['computingNodes']]

        if 'flags' in self.kwargs:
            cmd += self.kwargs['flags'].split()
        cmd += [self.kwargs['binary']]

        return run_command(cmd, args, kwargs)

    def __resolve_collection_layout_params__(self):
        # type: () -> list
        """ Resolve the collection layout, such as blocks, strides, etc.

        :return: list(param_name, block_count, block_length, stride)
        :raises PyCOMPSsException: If the collection layout does not contain block_count.
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

                if (block_length != -1 and block_count == -1) or \
                        (stride != -1 and block_count == -1):
                    msg = "Error: collection_layout must contain block_count!"
                    raise PyCOMPSsException(msg)
                layout_params.extend([param_name, str(block_count), str(block_length), str(stride)])
        layout_params.insert(0, str(num_layouts))
        return layout_params

    @staticmethod
    def __get_block_count__(collection_layout):
        # type: (dict) -> int
        """ Get the block count from the given collection layout.

        :param collection_layout: Collection layout.
        :return: Block count value.
        """
        if "block_count" in collection_layout:
            return collection_layout["block_count"]
        else:
            return -1

    @staticmethod
    def __get_block_length__(collection_layout):
        # type: (dict) -> int
        """ Get the block length from the given collection layout.

        :param collection_layout: Collection layout.
        :return: Block length value.
        """
        if "block_length" in collection_layout:
            return collection_layout["block_length"]
        else:
            return -1

    @staticmethod
    def __get_stride__(collection_layout):
        # type: (dict) -> int
        """ Get the stride from the given collection layout.

        :param collection_layout: Collection layout.
        :return: Stride value.
        """
        if "stride" in collection_layout:
            return collection_layout["stride"]
        else:
            return -1

    def __configure_core_element__(self, kwargs, user_function):
        # type: (dict, ...) -> None
        """ Include the registering info related to @mpi.

        IMPORTANT! Updates self.kwargs[CORE_ELEMENT_KEY].

        :param kwargs: Keyword arguments received from call.
        :param user_function: Decorated function.
        :return: None
        """
        if __debug__:
            logger.debug("Configuring @mpi core element.")

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

        # Check if scale by cu is defined
        scale_by_cu_str = self.__resolve_scale_by_cu__()

        # Resolve the working directory
        self.__resolve_working_dir__()
        # Resolve the fail by exit value
        self.__resolve_fail_by_exit_value__()
        # Resolve parameter collection layout
        collection_layout_params = self.__resolve_collection_layout_params__()

        if binary == "[unassigned]":
            impl_signature = impl_type + '.'
        else:
            impl_signature = '.'.join((impl_type,
                                       str(self.kwargs['processes']),
                                       binary))
        impl_args = [binary,
                     self.kwargs['working_dir'],
                     runner,
                     flags,
                     scale_by_cu_str,
                     self.kwargs['fail_by_exit_value']]

        if impl_type == "PYTHON_MPI":
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

    def __resolve_scale_by_cu__(self):
        # type: () -> str
        """ Checks if scale_by_cu is defined and process it.

        :return: Scale by cu value as string.
        :raises PyCOMPSsException: If scale_by_cu is not bool or string.
        """
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
                raise PyCOMPSsException("Incorrect format for scale_by_cu property. "
                                        "It should be boolean or an environment variable")  # noqa: E501
        else:
            scale_by_cu_str = 'false'
        return scale_by_cu_str


# ########################################################################### #
# ##################### MPI DECORATOR ALTERNATIVE NAME ###################### #
# ########################################################################### #

mpi = MPI
