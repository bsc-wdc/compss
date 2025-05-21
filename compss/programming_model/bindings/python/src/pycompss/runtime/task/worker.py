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
PyCOMPSs runtime - Task - Worker.

This file contains the task core functions when acting as worker.
"""

import gc
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from shutil import copyfile

from pycompss.api import parameter
from pycompss.util.context import CONTEXT
from pycompss.api.commons.constants import LABELS
from pycompss.api.exceptions import COMPSsException
from pycompss.runtime.commons import CONSTANTS
from pycompss.runtime.management.classes import Future
from pycompss.runtime.management.object_tracker import OT
from pycompss.runtime.task.arguments import get_name_from_kwarg
from pycompss.runtime.task.arguments import get_name_from_vararg
from pycompss.runtime.task.arguments import is_kwarg
from pycompss.runtime.task.arguments import is_return
from pycompss.runtime.task.arguments import is_vararg
from pycompss.runtime.task.commons import get_default_direction
from pycompss.runtime.task.definitions.arguments import TaskArguments
from pycompss.runtime.task.definitions.function import FunctionDefinition
from pycompss.runtime.task.parameter import Parameter
from pycompss.runtime.task.parameter import get_compss_type
from pycompss.runtime.task.parameter import get_new_parameter
from pycompss.runtime.task.parameter import get_direction_from_key
from pycompss.runtime.task.shared_args import SHARED_ARGUMENTS
from pycompss.runtime.task.wrappers.psco_stream import PscoStreamWrapper
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.logger.helpers import swap_logger_name
from pycompss.util.objects.properties import create_object_by_con_type
from pycompss.util.objects.util import group_iterable
from pycompss.util.serialization.serializer import deserialize_from_file
from pycompss.util.serialization.serializer import serialize_to_file
from pycompss.util.serialization.serializer import serialize_to_file_mpienv
from pycompss.util.std.redirects import not_std_redirector
from pycompss.util.std.redirects import std_redirector
from pycompss.util.storages.persistent import is_psco
from pycompss.util.tracing.helpers import EventInsideWorker
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing
from pycompss.worker.commons.worker import build_task_parameter

# The cache is only available currently for piper_worker.py and python >= 3.8
# If supported in the future by another worker, add a common interface
# with these two functions and import the appropriate.
from pycompss.worker.piper.cache.tracker import CACHE_TRACKER
from pycompss.worker.piper.cache.classes import TaskWorkerCache

NP = None  # type: typing.Any
try:
    import numpy

    NP = numpy
except ImportError:
    NP = None

if __debug__:
    import logging

    LOGGER = logging.getLogger(__name__)


class TaskWorker:
    """Task class representation for the Worker.

    Process the task decorator and prepare call the user function.
    """

    __slots__ = [
        "user_function",
        "decorator_arguments",
        "param_args",
        "param_varargs",
        "on_failure",
        "defaults",
        "cache",
    ]

    def __init__(
        self,
        decorator_arguments: TaskArguments,
        decorated_function: FunctionDefinition,
    ) -> None:
        """Task at worker constructor.

        :param decorator_arguments: Decorator arguments.
        :param decorated_function: Decorated function.
        """
        # Initialize TaskCommons
        self.decorator_arguments = decorator_arguments
        self.user_function = decorated_function.function
        self.param_args = []  # type: typing.List[typing.Any]
        self.param_varargs = None  # type: typing.Any
        self.on_failure = ""
        self.defaults = {}  # type: dict
        self.cache = TaskWorkerCache()

    def call(
        self, *args: typing.Any, **kwargs: typing.Any
    ) -> typing.Tuple[list, list, str, tuple]:
        """Run the task as worker.

        This function deals with task calls in the worker's side
        Note that the call to the user function is made by the worker,
        not by the user code.

        :param args: Arguments.
        :param kwargs: Keyword arguments.
        :return: A function that calls the user function with the given
                 parameters and does the proper serializations and updates
                 the affected objects.
        """
        global LOGGER
        # Save the args in a global place
        # (needed from synchronize when using nesting)
        SHARED_ARGUMENTS.set_worker_args(args)
        # Grab LOGGER from kwargs
        # (shadows outer LOGGER since it is set by the worker)
        LOGGER = kwargs["compss_logger"]  # noqa
        with swap_logger_name(LOGGER, __name__):
            if __debug__:
                LOGGER.debug("Starting @task decorator worker call")

            # Redirect stdout/stderr if necessary to show the prints/exceptions
            # in the job out/err files
            redirect_std = True
            if kwargs["compss_log_files"]:
                # Redirect all stdout and stderr during the user code execution
                # to job out and err files.
                job_out, job_err = kwargs["compss_log_files"]
            else:
                job_out, job_err = None, None
                redirect_std = False
            if __debug__:
                LOGGER.debug("Redirecting stdout to: %s", str(job_out))
                LOGGER.debug("Redirecting stderr to: %s", str(job_err))
            with (
                std_redirector(job_out, job_err)
                if redirect_std
                else not_std_redirector()
            ):
                # Update the on_failure attribute
                # (could be defined by @on_failure)
                if LABELS.on_failure in kwargs:
                    self.on_failure = kwargs.pop(LABELS.on_failure, "RETRY")
                    self.decorator_arguments.on_failure = self.on_failure
                else:
                    self.on_failure = self.decorator_arguments.on_failure
                self.defaults = kwargs.pop(LABELS.defaults, {})
                self.decorator_arguments.defaults = self.defaults

                # Pop cache if available
                cache_on = False
                cache = kwargs.pop("compss_cache", None)
                if cache:
                    (
                        self.cache.in_queue,
                        self.cache.out_queue,
                        self.cache.ids,
                        self.cache.profiler,
                    ) = cache
                    cache_on = True

                if __debug__:
                    LOGGER.debug("Revealing objects")
                # All parameters are in the same args list. At the moment we
                # only know the type, the name and the "value" of the
                # parameter. This value may be treated to get the actual
                # object (e.g: deserialize it, query the database in case of
                # persistent objects, etc.)
                self.reveal_objects(
                    args,
                    kwargs["compss_collections_layouts"],
                    kwargs["compss_python_MPI"],
                )
                if __debug__:
                    LOGGER.debug("Finished revealing objects")
                    LOGGER.debug("Building task parameters structures")

                # After this line all the objects in arg have a "content"
                # field, now we will segregate them in User positional and
                # variadic args
                user_args, user_kwargs, ret_params = self.segregate_objects(
                    args
                )
                num_returns = len(ret_params)

                if __debug__:
                    LOGGER.debug("Finished building parameters structures.")

                # Self definition (only used when defined in the task)
                # Save the self object type and value before executing the task
                # (it could be persisted inside if its a persistent object)
                self_type = -1
                self_value = None
                has_self = False
                if args and not isinstance(args[0], Parameter):
                    if __debug__:
                        LOGGER.debug("Detected self parameter")
                    # Then the first arg is self
                    has_self = True
                    self_type = get_compss_type(args[0])
                    if self_type == parameter.TYPE.EXTERNAL_PSCO:
                        if __debug__:
                            LOGGER.debug("\t - Self is a PSCO")
                        self_value = args[0].getID()
                    else:
                        # Since we are checking the type of the deserialized
                        # self parameter, get_compss_type will return that its
                        # type is parameter.TYPE.OBJECT, which, although it is
                        # an object, self is always a file for the runtime.
                        # So we must force its type to avoid that the return
                        # message notifies that it has a new type "object"
                        # which is not supported for python objects in the
                        # runtime.
                        self_type = parameter.TYPE.FILE
                        self_value = "null"

                # Call the user function with all the reconstructed parameters
                # and get the return values.
                if __debug__:
                    LOGGER.debug("Invoking user code")
                # Now execute the user code
                result = self.execute_user_code(
                    user_args, user_kwargs, kwargs["compss_tracing"], cache_on
                )
                user_returns, compss_exception, default_values = result

                # It is not necessary to wait for all nested tasks since we
                # delegate the serialization of their parameters or returns
                # to the inner tasks.

                if __debug__:
                    LOGGER.debug("Finished user code")

                python_mpi = False
                if kwargs["compss_python_MPI"]:
                    python_mpi = True

                # Deal with defaults if any
                if default_values:
                    self.manage_defaults(args, default_values)

                # Deal with INOUTs and COL_OUTs
                self.manage_inouts(args, python_mpi)

                # Deal with COMPSsExceptions
                if compss_exception is not None:
                    if __debug__:
                        LOGGER.warning("Detected COMPSs Exception. Raising.")
                    raise compss_exception

                # Deal with returns (if any)
                user_returns = self.manage_returns(
                    num_returns, user_returns, ret_params, python_mpi
                )

                with EventInsideWorker(TRACING_WORKER.manage_new_types):
                    # We must notify COMPSs when types are updated
                    new_types, new_values = self.manage_new_types_values(
                        num_returns,
                        user_returns,
                        args,
                        ret_params,
                        has_self,
                        self_type,
                        self_value,
                    )

                with EventInsideWorker(TRACING_WORKER.release_memory):
                    # Clean cached references
                    if self.cache.references:
                        # Let the garbage collector act
                        self.cache.references = []  # loose all references

                    # Release memory after task execution
                    self.__release_memory__(user_returns)

                if __debug__ and "COMPSS_WORKER_PROFILE_PATH" in os.environ:
                    self.__report_heap__()

            if __debug__:
                LOGGER.debug("Finished @task decorator")

        return (
            new_types,
            new_values,
            self.decorator_arguments.target_direction,
            args,
        )

    @staticmethod
    def __release_memory__(user_returns) -> None:
        """Release memory after task execution explicitly.

        :return: None.
        """
        SHARED_ARGUMENTS.delete_worker_args()
        # Call garbage collector: The memory may not be freed to the SO,
        # although the objects are removed.
        gc.collect()
        # Then try to deallocate the empty memory.
        try:
            import ctypes

            libc = ctypes.CDLL("libc.so.6")
            libc.malloc_trim(0)
        except Exception:  # pylint: disable=broad-except
            if __debug__:
                LOGGER.warning("Could NOT deallocate memory.")

        try:
            import cupy

            if user_returns is not None:
                for obj in user_returns:
                    if isinstance(obj, cupy.ndarray):
                        obj.data.mem.free()
        except ImportError:
            pass

    @staticmethod
    def __report_heap__() -> None:
        """Print the heap status.

        :return: None.
        """
        if __debug__:
            LOGGER.debug("Memory heap report:")
        try:
            import guppy  # noqa
        except ImportError:
            LOGGER.warning("Could NOT import Guppy.")
        else:
            if __debug__:
                LOGGER.debug(guppy.hpy().heap())

    def reveal_objects(
        self,
        args: tuple,
        collections_layouts: typing.Dict[str, typing.Tuple[int, int, int]],
        python_mpi: bool = False,
    ) -> None:
        """Get the objects from the args message.

        This function takes the arguments passed from the persistent worker
        and treats them to get the proper parameters for the user function.

        :param args: Arguments.
        :param python_mpi: If the task is python MPI.
        :param collections_layouts: Layouts of collections params for python
                                    MPI tasks.
        :return: None.
        """
        if self.storage_supports_pipelining():
            if __debug__:
                LOGGER.debug("The storage supports pipelining.")
            # Perform the pipelined getByID operation
            pscos = [
                x
                for x in args
                if x.content_type == parameter.TYPE.EXTERNAL_PSCO
            ]
            identifiers = [x.content for x in pscos]
            from storage.api import getByID  # noqa

            objects = getByID(*identifiers)
            # Just update the Parameter object with its content
            for obj, value in zip(objects, pscos):
                obj.content = value

        # Deal with all the parameters that are NOT returns
        if "COMPSS_THREADED_DESERIALIZATION" in os.environ:
            # Concurrent:
            if "COMPSS_NUM_CPUS" in os.environ:
                max_workers = int(os.environ["COMPSS_NUM_CPUS"])
            else:
                max_workers = 1
            if __debug__:
                LOGGER.debug(
                    "Parallelized retrieve_content (max_workers=%s)"
                    % str(max_workers)
                )
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for arg in [
                    x
                    for x in args
                    if isinstance(x, Parameter) and not is_return(x.name)
                ]:
                    futures.append(
                        executor.submit(
                            self.retrieve_content,
                            arg,
                            "",
                            python_mpi,
                            collections_layouts,
                        )
                    )
                wait(futures)
        else:
            for arg in [
                x
                for x in args
                if isinstance(x, Parameter) and not is_return(x.name)
            ]:
                self.retrieve_content(arg, "", python_mpi, collections_layouts)

    @staticmethod
    def storage_supports_pipelining() -> bool:
        """Check if storage supports pipelining.

        Some storage implementations use pipelining
        Pipelining means "accumulate the getByID queries and perform them
        in a single megaquery".
        If this feature is not available (storage does not support it)
        getByID operations will be performed one after the other.

        :return: True if pipelining is supported. False otherwise.
        """
        try:
            import storage.api  # noqa

            return storage.api.__pipelining__
        except (ImportError, AttributeError):
            return False

    def retrieve_content(
        self,
        argument: Parameter,
        name_prefix: str,
        python_mpi: bool,
        collections_layouts: typing.Dict[str, typing.Tuple[int, int, int]],
        depth: int = 0,
        force_file: bool = False,
    ) -> None:
        """Retrieve the content of a particular argument.

        :param argument: Argument.
        :param name_prefix: Name prefix.
        :param python_mpi: If the task is python MPI.
        :param collections_layouts: Layouts of collections params for python
                                    MPI tasks.
        :param depth: Collection depth (0 if not a collection).
        :param force_file: Force file type for collections or dict_collections
                           of files.
        :return: None
        """
        if __debug__:
            LOGGER.debug("\t - Revealing: " + str(argument.name))
            LOGGER.debug(
                "\t - checking: " + str(get_name_from_kwarg(argument.name))
            )
        # This case is special, as a FILE can actually mean a FILE or an
        # object that is serialized in a file
        if is_vararg(argument.name):
            self.param_varargs = argument.name
            if __debug__:
                LOGGER.debug("\t\t - It is vararg")

        content_type = argument.content_type
        type_file = parameter.TYPE.FILE
        type_directory = parameter.TYPE.DIRECTORY
        type_external_stream = parameter.TYPE.EXTERNAL_STREAM
        type_collection = parameter.TYPE.COLLECTION
        type_dict_collection = parameter.TYPE.DICT_COLLECTION
        type_external_psco = parameter.TYPE.EXTERNAL_PSCO

        if content_type == type_file:
            if (
                self.is_parameter_an_object(argument.name)
                and not force_file
                # and argument.content_type != parameter.TYPE.FILE
            ):
                # Get the direction from the decorator (could be taken from
                # the runtime in compss_types if sorted)
                _dec_arg = self.decorator_arguments.get_parameter_or_none(
                    argument.name
                )
                if _dec_arg:
                    argument.direction = _dec_arg.direction
                if argument.direction == parameter.DIRECTION.OUT:
                    # It is an OUT direction object: The file will not exist
                    # So, instantiate from info
                    if __debug__:
                        LOGGER.debug(
                            "\t\t - It is an OUT Object. "
                            "Instantiating type: %s",
                            str(argument.extra_content_type),
                        )
                    argument.content = create_object_by_con_type(
                        argument.extra_content_type
                    )
                    if __debug__:
                        LOGGER.debug("\t\t - Instantiation finished")
                else:
                    # The object is stored in some file, load and deserialize
                    if __debug__:
                        LOGGER.debug(
                            "\t\t - It is an OBJECT. "
                            "Deserializing from file: %s",
                            str(argument.file_name.original_path),
                        )
                    argument.content = self.recover_object(argument)
                    if __debug__:
                        LOGGER.debug("\t\t - Deserialization finished")
            else:
                # The object is a FILE, just forward the path of the file
                # as a string parameter
                argument.content = argument.file_name.original_path
                if __debug__:
                    LOGGER.debug(
                        "\t\t - It is FILE: %s", str(argument.content)
                    )
        elif content_type == type_directory:
            if __debug__:
                LOGGER.debug("\t\t - It is a DIRECTORY")
            argument.content = argument.file_name.original_path
        elif content_type == type_external_stream:
            if __debug__:
                LOGGER.debug("\t\t - It is an EXTERNAL STREAM")
            argument.content = self.recover_object(argument)
            # #################################################################
            # ## THIS IS TEMPORAL UNTIL THE EXTERNAL PSCO STREAM TYPE IS     ##
            # ## IMPLEMENTED                                                 ##
            # #################################################################
            if isinstance(argument.content, PscoStreamWrapper):
                # It is a persisted object hidden within a stream object
                psco_id = argument.content.get_psco_id()
                if __debug__:
                    LOGGER.debug(
                        "\t\t - It is hiding a PSCO STREAM with id: %s",
                        str(psco_id),
                    )
                # The object is a PSCO, do a single getByID of the PSCO
                from storage.api import getByID  # noqa

                argument.content = getByID(psco_id)
            # #################################################################
        elif content_type == type_collection:
            argument.content = []
            # This field is exclusive for COLLECTION_T parameters, so make
            # sure you have checked this parameter is a collection before
            # consulting it
            argument.collection_content = []
            col_f_name = str(argument.file_name.original_path)

            # maybe it is an inner-collection..
            _dec_arg = self.decorator_arguments.get_parameter_or_none(
                argument.name
            )
            _col_dir = _dec_arg.direction if _dec_arg else None
            _col_dep = _dec_arg.depth if _dec_arg else depth
            if __debug__:
                LOGGER.debug("\t\t - It is a COLLECTION: %s", col_f_name)
                LOGGER.debug("\t\t\t - Depth: %s", str(_col_dep))

            # Check if this collection is in layout
            # Three conditions:
            # 1- this is a mpi task
            # 2- it has a collection layout
            # 3- the current argument is the layout target
            in_mpi_collection_env = False
            if (
                python_mpi
                and collections_layouts
                and argument.name in collections_layouts
            ):
                in_mpi_collection_env = True
                from pycompss.util.mpi.helper import rank_distributor

                # Call rank_distributor if the current param is the target of
                # the layout for each rank, return its offset(s) in the
                # collection.
                rank_distribution = rank_distributor(
                    collections_layouts[argument.name]
                )
                rank_distr_len = len(rank_distribution)
                if __debug__:
                    LOGGER.debug(
                        "Rank distribution is: %s", str(rank_distribution)
                    )

            with open(col_f_name, "r") as col_f_name_fd:
                for i, line in enumerate(col_f_name_fd):
                    if in_mpi_collection_env and i not in rank_distribution:
                        # Isn't this my offset? skip
                        continue
                    elems = line.strip().split()
                    data_type = elems[0]
                    content_file = elems[1]
                    content_type_elem = elems[2]
                    # Same naming convention as in COMPSsRuntimeImpl.java
                    sub_name = f"{argument.name}.{i}"
                    if name_prefix:
                        sub_name = f"{name_prefix}.{argument.name}"
                    else:
                        sub_name = f"@{sub_name}"

                    if __debug__:
                        LOGGER.debug(
                            "\t\t\t - Revealing element: %s", str(sub_name)
                        )

                    is_file_collection = self.is_parameter_file_collection(
                        argument.name
                    )
                    is_really_file = (
                        is_file_collection or content_type_elem == "FILE"
                    )
                    sub_arg, _ = build_task_parameter(
                        int(data_type),
                        parameter.IOSTREAM.UNSPECIFIED,
                        "",
                        sub_name,
                        content_file,
                        str(argument.content_type),
                        logger=LOGGER,
                    )

                    # if direction of the collection is "out", it means we
                    # haven't received serialized objects from the Master
                    # (even though parameters have "file_name", those files
                    # haven't been created yet). plus, inner collections of
                    # col_out params do NOT have "direction", we identify
                    # them by "depth"..
                    if _col_dir == parameter.DIRECTION.OUT or (
                        (_col_dir is None) and _col_dep > 0
                    ):
                        # if we are at the last level of COL_OUT param,
                        # create "empty" instances of elements
                        if (
                            _col_dep == 1
                            or content_type_elem != "collection:list"
                        ):
                            # Not a nested collection anymore
                            if is_really_file:
                                sub_arg.content = content_file
                                sub_arg.content_type = parameter.TYPE.FILE
                            else:
                                temp = create_object_by_con_type(
                                    content_type_elem
                                )
                                sub_arg.content = temp
                            # In case that only one element is used in this
                            # mpi rank, the collection list is removed
                            if in_mpi_collection_env and rank_distr_len == 1:
                                argument.content = sub_arg.content
                                argument.content_type = sub_arg.content_type
                            else:
                                argument.content.append(sub_arg.content)
                            argument.collection_content.append(sub_arg)
                        else:
                            # Is nested collection
                            self.retrieve_content(
                                sub_arg,
                                sub_name,
                                python_mpi,
                                collections_layouts,
                                depth=_col_dep - 1,
                                force_file=is_really_file,
                            )
                            # In case that only one element is used in this mpi
                            # rank, the collection list is removed
                            if in_mpi_collection_env and rank_distr_len == 1:
                                argument.content = sub_arg.content
                                argument.content_type = sub_arg.content_type
                            else:
                                argument.content.append(sub_arg.content)
                            argument.collection_content.append(sub_arg)
                    else:
                        # Recursively call the retrieve method, fill the
                        # content field in our new taskParameter object
                        self.retrieve_content(
                            sub_arg,
                            sub_name,
                            python_mpi,
                            collections_layouts,
                            force_file=is_really_file,
                        )
                        # In case only one element is used in this mpi rank,
                        # the collection list is removed
                        if in_mpi_collection_env and rank_distr_len == 1:
                            argument.content = sub_arg.content
                            argument.content_type = sub_arg.content_type
                        else:
                            argument.content.append(sub_arg.content)
                        argument.collection_content.append(sub_arg)
        elif content_type == type_dict_collection:
            argument.content = {}
            # This field is exclusive for DICT_COLLECTION_T parameters, so
            # make sure you have checked this parameter is a dictionary
            # collection before consulting it
            argument.dict_collection_content = {}
            dict_col_f_name = argument.file_name.original_path
            # Uncomment if you want to check its contents:
            # print("Dictionary file name: " + str(dict_col_f_name))
            # print("Dictionary file contents:")
            # with open(dict_col_f_name, "r") as f:
            #     print(f.read())

            # Maybe it is an inner-dict-collection
            _dec_arg = self.decorator_arguments.get_parameter_or_none(
                argument.name
            )
            _dict_col_dir = _dec_arg.direction if _dec_arg else None
            _dict_col_dep = _dec_arg.depth if _dec_arg else depth

            with open(dict_col_f_name, "r") as dict_file:
                lines = dict_file.readlines()
            entries = group_iterable(lines, 2)
            i = 0
            for entry in entries:
                entry_k = entry[0]
                entry_v = entry[1]
                (
                    data_type_key,
                    content_file_key,
                    content_type_key,
                ) = entry_k.strip().split()
                (
                    data_type_value,
                    content_file_value,
                    content_type_value,
                ) = entry_v.strip().split()
                # Same naming convention as in COMPSsRuntimeImpl.java
                sub_name_key = f"{argument.name}.{i}"
                sub_name_value = f"{argument.name}.{i}"
                if name_prefix:
                    sub_name_key = f"{name_prefix}.{argument.name}"
                    sub_name_value = f"{name_prefix}.{argument.name}"
                else:
                    sub_name_key = f"@key{sub_name_key}"
                    sub_name_value = f"@value{sub_name_value}"

                sub_arg_key, _ = build_task_parameter(
                    int(data_type_key),
                    parameter.IOSTREAM.UNSPECIFIED,
                    "",
                    sub_name_key,
                    content_file_key,
                    str(argument.content_type),
                    logger=LOGGER,
                )
                sub_arg_value, _ = build_task_parameter(
                    int(data_type_value),
                    parameter.IOSTREAM.UNSPECIFIED,
                    "",
                    sub_name_value,
                    content_file_value,
                    str(argument.content_type),
                    logger=LOGGER,
                )

                # if direction of the dictionary collection is "out", it
                # means we haven't received serialized objects from the
                # Master (even though parameters have "file_name", those
                # files haven't been created yet). plus, inner dictionary
                # collections of dict_col_out params do NOT have
                # "direction", we identify them by "depth"..
                if _dict_col_dir == parameter.DIRECTION.OUT or (
                    (_dict_col_dir is None) and _dict_col_dep > 0
                ):
                    # if we are at the last level of DICT_COL_OUT param,
                    # create "empty" instances of elements
                    if (
                        _dict_col_dep == 1
                        or content_type_elem != "collection:dict"
                    ):
                        if content_type_elem == "FILE":
                            temp_k = content_file_key
                            temp_v = content_file_value
                        else:
                            temp_k = create_object_by_con_type(
                                content_type_key
                            )
                            temp_v = create_object_by_con_type(
                                content_type_value
                            )
                        sub_arg_key.content = temp_k
                        sub_arg_value.content = temp_v
                        argument.content[sub_arg_key.content] = (
                            sub_arg_value.content
                        )
                        argument.dict_collection_content[sub_arg_key] = (
                            sub_arg_value
                        )
                    else:
                        self.retrieve_content(
                            sub_arg_key,
                            sub_name_key,
                            python_mpi,
                            collections_layouts,
                            depth=_dict_col_dep - 1,
                        )
                        self.retrieve_content(
                            sub_arg_value,
                            sub_name_value,
                            python_mpi,
                            collections_layouts,
                            depth=_dict_col_dep - 1,
                        )
                        argument.content[sub_arg_key.content] = (
                            sub_arg_value.content
                        )
                        argument.dict_collection_content[sub_arg_key] = (
                            sub_arg_value
                        )
                else:
                    # Recursively call the retrieve method, fill the
                    # content field in our new taskParameter object
                    self.retrieve_content(
                        sub_arg_key,
                        sub_name_key,
                        python_mpi,
                        collections_layouts,
                    )
                    self.retrieve_content(
                        sub_arg_value,
                        sub_name_value,
                        python_mpi,
                        collections_layouts,
                    )
                    argument.content[sub_arg_key.content] = (
                        sub_arg_value.content
                    )  # noqa: E501
                    argument.dict_collection_content[sub_arg_key] = (
                        sub_arg_value  # noqa: E501
                    )
        elif (
            not self.storage_supports_pipelining()
            and content_type == type_external_psco
        ):
            if __debug__:
                LOGGER.debug("\t\t - It is a PSCO")
            # The object is a PSCO and the storage does not support
            # pipelining, do a single getByID of the PSCO
            from storage.api import getByID  # noqa

            argument.content = getByID(argument.content)
        # If we have not entered in any of these cases we will assume
        # that the object was a basic type and the content is already
        # available and properly casted by the python worker

    def recover_object(self, argument: Parameter) -> typing.Any:
        """Recover the object within a file.

        :param argument: Parameter object for the argument to recover.
        :return: The object associated to the given argument Parameter.
        """
        name = argument.name
        original_path = argument.file_name.original_path

        if is_vararg(name):
            param_name = name
        else:
            regex_param_name = r"(?:@|^)([a-zA-Z_]\w*)(?=\.\d+|$)"
            match = re.match(regex_param_name, name)
            if match:
                param_name = match.group(1)
            else:
                if __debug__:
                    LOGGER.debug(
                        "\t\t - Couldn't extract param name '%s'", str(name)
                    )
                param_name = name

        # Check if cache is available
        cache = (
            self.cache.in_queue is not None
            and self.cache.out_queue is not None
        )
        use_cache = False  # default store object in cache

        if cache:
            # Check if the user has defined that the parameter has or not to be
            # stored in cache explicitly
            if (
                not self.cache.profiler
                and param_name in self.decorator_arguments.parameters
            ):
                use_cache = self.decorator_arguments.parameters[
                    param_name
                ].cache
            else:
                if is_vararg(name):
                    vararg_name = get_name_from_vararg(name)
                    if (
                        not self.cache.profiler
                        and vararg_name in self.decorator_arguments.parameters
                    ):
                        use_cache = self.decorator_arguments.parameters[
                            vararg_name
                        ].cache
                elif self.cache.profiler:
                    use_cache = True
                else:
                    # if not explicitly said, the object is candidate to be
                    # stored in cache
                    use_cache = False
            argument.cache = use_cache
            if __debug__ and cache:
                LOGGER.debug(
                    "\t\t - Has to be saved in cache: %s", str(use_cache)
                )

        if NP and cache and use_cache:
            # Check if the object is already in cache
            if CACHE_TRACKER.in_cache(LOGGER, original_path, self.cache.ids):
                # The object is cached
                with EventInsideWorker(TRACING_WORKER.cache_hit_event):
                    if __debug__:
                        LOGGER.debug(
                            "\t\t - Found in cache (Cache hit) "
                            "- retrieving: %s",
                            str(original_path),
                        )
                (
                    retrieved,
                    existing_shm,
                ) = CACHE_TRACKER.retrieve_object_from_cache(
                    LOGGER,
                    self.cache.ids,
                    self.cache.in_queue,
                    self.cache.out_queue,
                    original_path,
                    name,
                    self.user_function,
                    self.cache.profiler,
                )
                self.cache.references.append(existing_shm)
                return retrieved
            # Else not in cache.
            # Retrieve from file and put in cache if possible
            # <SN>:<DN>:<KS>:<FV>:<ON>
            # Where:
            #   <SN> = source name
            #   <DN> = destination name
            #   <KS> = keep source
            #   <FV> = is write final value
            #   <ON> = original name
            # out or inout + is write final ==> do not put into cache?
            #                                   (currently only means if is
            #                                   different from read)
            # out + keep source ==> impossible
            # noqa inout + keep source ==> Lool for the destination name +
            #                              Put into cache after with the
            #                              destination name
            # If keep source == False ==> Look for the source name instead of
            #                             the destination mane
            # Do not put into cache if IN and keep source == False
            # If keep source == True ==> Put in cache if it is not already
            with EventInsideWorker(TRACING_WORKER.cache_miss_event):
                if __debug__:
                    LOGGER.debug(
                        "\t\t - Not found in cache (Cache miss) "
                        "- deserializing: %s",
                        str(original_path),
                    )
            obj = deserialize_from_file(original_path, LOGGER)
            if (
                argument.file_name.keep_source
                and argument.direction != parameter.DIRECTION.IN_DELETE
            ):
                # Try to insert in cache.
                # May not be inserted if there is other process inserting the
                # same file.
                CACHE_TRACKER.insert_object_into_cache(
                    LOGGER,
                    self.cache.in_queue,
                    self.cache.out_queue,
                    obj,
                    original_path,
                    name,
                    self.user_function,
                )
            return obj

        return deserialize_from_file(original_path, LOGGER)

    def segregate_objects(self, args: tuple) -> typing.Tuple[list, dict, list]:
        """Split a list of arguments.

        Segregates a list of arguments in user positional, variadic and
        return arguments.

        :return: list of user arguments, dictionary of user kwargs and a list
                 of return parameters.
        """
        # User args
        user_args = []
        # User named args (kwargs)
        user_kwargs = {}
        # Return parameters, save them apart to match the user returns with
        # the internal parameters
        ret_params = []

        for arg in args:
            # Just fill the three data structures declared above
            # Deal with the self parameter (if any)
            if not isinstance(arg, Parameter):
                user_args.append(arg)
            # All these other cases are all about regular parameters
            elif is_return(arg.name):
                ret_params.append(arg)
            elif is_kwarg(arg.name):
                user_kwargs[get_name_from_kwarg(arg.name)] = arg.content
            else:
                if is_vararg(arg.name):
                    self.param_varargs = get_name_from_vararg(arg.name)
                # Apart from the names we preserve the original order, so it
                # is guaranteed that named positional arguments will never be
                # swapped with variadic ones or anything similar
                user_args.append(arg.content)

        return user_args, user_kwargs, ret_params

    def execute_user_code(
        self,
        user_args: list,
        user_kwargs: dict,
        tracing: bool,
        cache_on: bool,
    ) -> typing.Tuple[
        typing.Any, typing.Optional[COMPSsException], typing.Optional[dict]
    ]:
        """Execute the user code.

        Disables the tracing hook if tracing is enabled. Restores it
        at the end of the user code execution.

        :param user_args: Function args.
        :param user_kwargs: Function kwargs.
        :param tracing: If tracing enabled.
        :return: The user function returns and the compss exception (if any).
        """
        with EventInsideWorker(TRACING_WORKER.execute_user_code_event):
            # Tracing hook is disabled by default during the user code of
            # the task.
            # The user can enable it with tracing_hook=True in @task decorator
            # for specific tasks or globally with the COMPSS_TRACING_HOOK=true
            # environment variable.
            restore_hook = False
            pro_f = None
            if tracing:
                global_tracing_hook = False
                if CONSTANTS.tracing_hook_env_var in os.environ:
                    hook_enabled = (
                        os.environ[CONSTANTS.tracing_hook_env_var] == "true"
                    )
                    global_tracing_hook = hook_enabled
                if (
                    self.decorator_arguments.tracing_hook
                    or global_tracing_hook
                ):
                    # The user wants to keep the tracing hook
                    pass
                else:
                    # When Extrae library implements the function to disable,
                    # use it, as:
                    #     import pyextrae
                    #     pro_f = pyextrae.shutdown()
                    # Since it is not available yet, we manage the tracing hook
                    # by ourselves
                    pro_f = sys.getprofile()
                    sys.setprofile(None)
                    restore_hook = True

            user_returns = None  # type: typing.Any
            compss_exception = None  # type: typing.Optional[COMPSsException]
            default_values = None  # type: typing.Optional[dict]
            if self.decorator_arguments.numba:
                # Import all supported functionalities
                from numba import jit
                from numba import njit
                from numba import generated_jit
                from numba import vectorize
                from numba import guvectorize
                from numba import stencil
                from numba import cfunc

                numba_mode = self.decorator_arguments.numba
                numba_flags = self.decorator_arguments.numba_flags
                if (
                    isinstance(numba_mode, dict)
                    or numba_mode is True
                    or numba_mode == "jit"
                ):
                    # Use the flags defined by the user
                    numba_flags["cache"] = True  # Always force cache
                    user_returns = jit(self.user_function, **numba_flags)(
                        *user_args, **user_kwargs
                    )
                    # Alternative way of calling:
                    # user_returns = jit(cache=True)(self.user_function) \
                    #                   (*user_args, **user_kwargs)
                elif numba_mode == "generated_jit":
                    user_returns = generated_jit(
                        self.user_function, **numba_flags
                    )(*user_args, **user_kwargs)
                elif numba_mode == "njit":
                    numba_flags["cache"] = True  # Always force cache
                    user_returns = njit(self.user_function, **numba_flags)(
                        *user_args, **user_kwargs
                    )
                elif numba_mode == "vectorize":
                    numba_signature = self.decorator_arguments.numba_signature
                    user_returns = vectorize(numba_signature, **numba_flags)(
                        self.user_function
                    )(*user_args, **user_kwargs)
                elif numba_mode == "guvectorize":
                    numba_signature = self.decorator_arguments.numba_signature
                    numba_decl = self.decorator_arguments.numba_declaration
                    user_returns = guvectorize(
                        numba_signature, numba_decl, **numba_flags
                    )(self.user_function)(*user_args, **user_kwargs)
                elif numba_mode == "stencil":
                    user_returns = stencil(**numba_flags)(self.user_function)(
                        *user_args, **user_kwargs
                    )
                elif numba_mode == "cfunc":
                    numba_signature = self.decorator_arguments.numba_signature
                    user_returns = cfunc(numba_signature)(
                        self.user_function
                    ).ctypes(*user_args, **user_kwargs)
                else:
                    raise PyCOMPSsException("Unsupported numba mode.")
            else:
                try:
                    # Normal task execution
                    user_returns = self.user_function(
                        *user_args, **user_kwargs
                    )
                except COMPSsException as compss_exc:
                    # Perform any required action on failure
                    user_returns, default_values = self.manage_exception()
                    compss_exception = compss_exc
                    # Set target direction in the exception
                    compss_exception.target_direction = get_direction_from_key(
                        self.decorator_arguments.target_direction
                    )
                except Exception as exc:  # pylint: disable=broad-except
                    if self.on_failure == "IGNORE":
                        # Perform any required action on failure
                        user_returns, default_values = self.manage_exception()
                    else:
                        # Re-raise the exception
                        raise exc

            # Reestablish the hook if it was disabled
            if restore_hook:
                sys.setprofile(pro_f)

            if cache_on:
                clean_cupy_env()

            return user_returns, compss_exception, default_values

    def manage_exception(
        self,
    ) -> typing.Tuple[typing.Optional[int], typing.Optional[dict]]:
        """Deal with exceptions (on failure action).

        :return: The default return and values.
        """
        user_returns = None
        default_values = None
        if self.on_failure == "IGNORE":
            # Provide default return
            user_returns = self.defaults.pop("returns", None)
            # Provide defaults to the runtime
            default_values = self.defaults
        return user_returns, default_values

    def manage_defaults(self, args: tuple, default_values: dict) -> None:
        """Deal with default values.

        WARNING! Updates args with the appropriate object or file.

        :param args: Argument list.
        :param default_values: Dictionary containing the default values.
        :return: None.
        """
        if __debug__:
            LOGGER.debug("Dealing with default values")
        for arg in args:
            # Skip non-task-parameters
            if not isinstance(arg, Parameter):
                continue
            # Skip returns
            if is_return(arg.name):
                continue
            if self.is_parameter_an_object(arg.name):
                # Update object
                arg.content = default_values[arg.name]
            else:
                # Update file
                copyfile(str(default_values[arg.name]), str(arg.content))

    def manage_inouts(self, args: tuple, python_mpi: bool) -> None:
        """Deal with INOUTS.

        Serializes the result of INOUT parameters.

        :param args: Argument list.
        :param python_mpi: Boolean if python mpi.
        :return: None.
        """
        if __debug__:
            LOGGER.debug("Dealing with INOUTs and OUTS")
            if python_mpi:
                LOGGER.debug("\t - Managing with MPI policy")

        # Manage all the possible outputs of the task and build the return new
        # types and values
        for arg in args:
            # Handle only task parameters that are objects

            # Skip files and non-task-parameters
            if not isinstance(
                arg, Parameter
            ) or not self.is_parameter_an_object(
                arg.name,
            ):
                continue

            original_name = get_name_from_kwarg(arg.name)
            real_direction = get_default_direction(
                original_name, self.decorator_arguments, self.param_args
            )
            param = self.decorator_arguments.get_parameter(
                original_name, real_direction
            )
            # Update args
            arg.direction = param.direction

            # File collections are objects, but must be skipped as well
            if self.is_parameter_file_collection(arg.name):
                continue

            # Skip psco: since param.content_type has the old type, we can
            # not use:  param.content_type != parameter.TYPE.EXTERNAL_PSCO
            _is_psco_true = (
                arg.content_type == parameter.TYPE.EXTERNAL_PSCO
                or is_psco(arg.content)
            )
            if _is_psco_true:
                continue

            # skip non-inouts or non-col_outs
            _is_col_out = (
                arg.content_type == parameter.TYPE.COLLECTION
                and param.direction == parameter.DIRECTION.OUT
            )

            _is_dict_col_out = (
                arg.content_type == parameter.TYPE.DICT_COLLECTION
                and param.direction == parameter.DIRECTION.OUT
            )

            _is_inout = param.direction in (
                parameter.DIRECTION.INOUT,
                parameter.DIRECTION.COMMUTATIVE,
            )

            _is_out = (
                param.direction == parameter.DIRECTION.OUT
                and arg.content_type != parameter.TYPE.EXTERNAL_STREAM
            )

            if not (_is_inout or _is_out or _is_col_out or _is_dict_col_out):
                continue

            # Now it is "INOUT" or "OUT" or "COLLECTION_OUT" or
            # "DICT_COLLECTION_OUT" or not has been delegated.
            # object param, serialize to a file.
            if arg.content_type == parameter.TYPE.COLLECTION:
                if __debug__:
                    LOGGER.debug("Serializing collection: %s", str(arg.name))
                # handle collections recursively
                for content, elem in get_collection_objects(arg.content, arg):
                    if elem.file_name:
                        _is_delegated = False
                        if CONTEXT.is_nesting_enabled():
                            if self._check_if_delegated(elem):
                                # Skip serialization
                                _is_delegated = True

                        if not _is_delegated:
                            f_name = elem.file_name.original_path
                            if __debug__:
                                LOGGER.debug(
                                    "\t - Serializing element: %s to %s",
                                    str(arg.name),
                                    str(f_name),
                                )
                            if python_mpi:
                                serialize_to_file_mpienv(
                                    content, f_name, False, LOGGER
                                )
                            else:
                                serialize_to_file(content, f_name, LOGGER)
                                self.update_object_in_cache(content, arg)
                    else:
                        # It is None --> PSCO
                        pass
            elif arg.content_type == parameter.TYPE.DICT_COLLECTION:
                if __debug__:
                    LOGGER.debug(
                        "Serializing dictionary collection: " + str(arg.name)
                    )
                # handle dictionary collections recursively
                for content, elem in get_dict_collection_objects(
                    arg.content, arg
                ):
                    if elem.file_name:
                        _is_delegated = False
                        if CONTEXT.is_nesting_enabled():
                            if self._check_if_delegated(elem):
                                # Skip serialization
                                _is_delegated = True

                        if not _is_delegated:
                            f_name = elem.file_name.original_path
                            if __debug__:
                                LOGGER.debug(
                                    "\t - Serializing element: %s to %s",
                                    str(arg.name),
                                    str(f_name),
                                )
                            if python_mpi:
                                serialize_to_file_mpienv(
                                    content, f_name, False, LOGGER
                                )
                            else:
                                serialize_to_file(content, f_name, LOGGER)
                                self.update_object_in_cache(content, arg)
                    else:
                        # It is None --> PSCO
                        pass
            else:
                # NOTE: Synchronization with nesting is not needed anymore
                #       since we are delegating the writing of the file to
                #       the nested tasks.
                if CONTEXT.is_nesting_enabled():
                    if self._check_if_delegated(arg):
                        # Skip serialization
                        continue

                f_name = arg.file_name.original_path

                if __debug__:
                    LOGGER.debug(
                        "Serializing object: %s to %s",
                        str(arg.name),
                        str(f_name),
                    )

                if python_mpi:
                    serialize_to_file_mpienv(
                        arg.content, f_name, False, LOGGER
                    )
                else:
                    serialize_to_file(arg.content, f_name, LOGGER)
                    self.update_object_in_cache(arg.content, arg)

    @staticmethod
    def _check_if_delegated(arg: Parameter) -> bool:
        """Check if the argument is delegated.

        CAUTION: Modifies arg if is delegated!

        :param arg: Parameter to be checked (and updated if necessary).
        :return: True if delegated (serialization must be skipped).
                 False otherwise.
        """
        obj_id = OT.is_tracked(arg.content)
        _is_delegated = obj_id != ""
        if _is_delegated:
            arg.is_future = _is_delegated
            # Update the file path to the delegated file name
            obj_file_path = OT.get_file_name(obj_id)
            arg.file_name.source_path = obj_file_path
            arg.file_name.destination_name = obj_id
            old_original_path = arg.file_name.original_path
            arg.file_name.original_path = obj_file_path
            if __debug__:
                LOGGER.debug(
                    "Parameter %s serialization has been delegated "
                    "to a nested task (%s).",
                    old_original_path,
                    arg.file_name.original_path,
                )
            # Skip serialization
            return True
        return False

    def update_object_in_cache(
        self, content: typing.Any, argument: Parameter
    ) -> None:
        """Update the object into cache if possible.

        :param content: Object to be updated.
        :param argument: Parameter object for the argument to be updated.
        :return: None.
        """
        name = argument.name
        original_path = argument.file_name.original_path

        cache = (
            self.cache.in_queue is not None
            and self.cache.out_queue is not None
        )
        if (
            not self.cache.profiler
            and name in self.decorator_arguments.parameters
        ):
            use_cache = self.decorator_arguments.parameters[name].cache
        elif self.cache.profiler:
            use_cache = True
        else:
            # if not explicitly said, the object is candidate to be cached
            use_cache = False
        if NP and cache and use_cache:
            if CACHE_TRACKER.in_cache(LOGGER, original_path, self.cache.ids):
                CACHE_TRACKER.replace_object_into_cache(
                    LOGGER,
                    self.cache.in_queue,
                    self.cache.out_queue,
                    content,
                    original_path,
                    name,
                    self.user_function,
                )
            else:
                with EventInsideWorker(TRACING_WORKER.cache_miss_event):
                    pass
                CACHE_TRACKER.insert_object_into_cache(
                    LOGGER,
                    self.cache.in_queue,
                    self.cache.out_queue,
                    content,
                    original_path,
                    name,
                    self.user_function,
                )

    def manage_returns(
        self,
        num_returns: int,
        user_returns: typing.Any,
        ret_params: list,
        python_mpi: bool,
    ) -> typing.Any:
        """Manage task returns.

        WARNING: Modifies ret_params, which is included into args.

        :param num_returns: Number of returns.
        :param user_returns: User returns.
        :param ret_params: Return parameters.
        :param python_mpi: Boolean if is python mpi code.
        :return: User returns.
        """
        if __debug__:
            LOGGER.debug("Dealing with returns: %s", str(num_returns))
        if num_returns > 0:
            if num_returns == 1:
                # Generalize the return case to multi-return to simplify the
                # code
                user_returns = [user_returns]
            elif num_returns > 1 and python_mpi:
                user_returns = [user_returns]
                ret_params = get_ret_rank(ret_params)
            # Note that we are implicitly assuming that the length of the user
            # returns matches the number of return parameters
            for obj, param in zip(user_returns, ret_params):
                # Store the object int ret_params (included in args)
                param.content = obj
                param.direction = parameter.DIRECTION.OUT

                f_name = param.file_name.original_path

                # If the object is a PSCO, do not serialize to file
                if (
                    param.content_type == parameter.TYPE.EXTERNAL_PSCO
                    or is_psco(obj)
                ):
                    continue

                if CONTEXT.is_nesting_enabled():
                    obj_id = OT.is_tracked(param.content)
                    _is_delegated = obj_id != ""
                    if _is_delegated:
                        param.is_future = _is_delegated
                        # Update the file path to the delegated file name
                        obj_file_path = OT.get_file_name(obj_id)
                        param.file_name.destination_name = obj_id
                        param.file_name.original_path = obj_file_path

                    if isinstance(param.content, Future) or param.is_future:
                        if __debug__:
                            LOGGER.debug(
                                "Return %s delegated to a nested task (%s).",
                                f_name,
                                param.file_name.original_path,
                            )
                        continue

                # Serialize the object
                # Note that there is no "command line optimization" in the
                # returns, as we always pass them as files.
                # This is due to the asymmetry in worker-master communications
                # and because it also makes it easier for us to deal with
                # returns in that format

                if __debug__:
                    LOGGER.debug("Serializing return: %s", str(f_name))
                if python_mpi:
                    if num_returns > 1:
                        rank_zero_reduce = False
                    else:
                        rank_zero_reduce = True

                    serialize_to_file_mpienv(
                        obj, f_name, rank_zero_reduce, LOGGER
                    )
                else:
                    serialize_to_file(obj, f_name, LOGGER)
                if (
                    self.cache.in_queue is not None
                    and self.cache.out_queue is not None
                    and (
                        self.cache.profiler
                        or self.decorator_arguments.cache_returns
                    )
                    and not CACHE_TRACKER.in_cache(
                        LOGGER, f_name, self.cache.ids
                    )
                ):
                    with EventInsideWorker(TRACING_WORKER.cache_miss_event):
                        if __debug__:
                            LOGGER.debug("Storing return in cache")
                    CACHE_TRACKER.insert_object_into_cache(
                        LOGGER,
                        self.cache.in_queue,
                        self.cache.out_queue,
                        obj,
                        f_name,
                        "Return",
                        self.user_function,
                    )
        return user_returns

    def is_parameter_an_object(self, name: str) -> bool:
        """Given the name of a parameter, determine if it is an object or not.

        :param name: Name of the parameter.
        :return: True if the parameter is a (serializable) object.
        """
        original_name = get_name_from_kwarg(name)
        # Get the args parameter object
        if is_vararg(original_name):
            varargs_direction = self.decorator_arguments.varargs_type
            param = get_new_parameter(varargs_direction)
            return param.content_type == -1
        # Is this parameter annotated in the decorator?
        if original_name in self.decorator_arguments.parameters:
            annotated = [
                parameter.TYPE.COLLECTION,
                parameter.TYPE.DICT_COLLECTION,
                parameter.TYPE.EXTERNAL_STREAM,
                parameter.TYPE.OBJECT,
                -1,
            ]
            return (
                self.decorator_arguments.parameters[original_name].content_type
                in annotated
            )
        # The parameter is not annotated in the decorator, so return default
        return True

    def is_parameter_file_collection(self, name: str) -> bool:
        """Determine if the given parameter name it's a file collection or not.

        :param name: Name of the parameter.
        :return: True if the parameter is a file collection.
        """
        original_name = get_name_from_kwarg(name)
        # Get the args parameter object
        if is_vararg(original_name):
            varargs_direction = self.decorator_arguments.varargs_type
            param = get_new_parameter(varargs_direction)
            return param.is_file_collection
        # Is this parameter annotated in the decorator?
        if original_name in self.decorator_arguments.parameters:
            return self.decorator_arguments.parameters[
                original_name
            ].is_file_collection
        # The parameter is not annotated in the decorator, so (by default)
        # return False
        return False

    def manage_new_types_values(
        self,
        num_returns: int,
        user_returns: typing.Any,
        args: tuple,
        ret_params: list,
        has_self: bool,
        self_type: int,
        self_value: typing.Any,
    ) -> typing.Tuple[list, list]:
        """Manage new types and values.

        We must notify COMPSs when types are updated
        Potential update candidates are returns and INOUTs
        But the whole types and values list must be returned
        new_types and new_values correspond to "parameters self returns"

        :param num_returns: Number of returns.
        :param user_returns: User returns.
        :param args: Arguments.
        :param ret_params: Return parameters.
        :param has_self: If has self.
        :param self_type: Self type.
        :param self_value: Self value.
        :return: List new types, List new values.
        """
        new_types, new_values = [], []
        if __debug__:
            LOGGER.debug("Building types update")

        def build_collection_types_values(
            _content: typing.Any, _arg: Parameter, direction: int
        ) -> list:
            """Retrieve collection type-value recursively.

            :param _content: Object or list of objects.
            :param _arg: Argument or list of arguments of the given objects.
            :param direction: Direction of the object/s.
            :returns: The collection representation.
            """
            coll = []  # type: list
            for _cont, _elem in zip(_arg.content, _arg.collection_content):
                if isinstance(_elem, str):
                    coll.append([parameter.TYPE.FILE, "null"])
                else:
                    if _elem.content_type == parameter.TYPE.COLLECTION:
                        coll.append(
                            build_collection_types_values(
                                _cont, _elem, direction
                            )
                        )
                    elif (
                        _elem.content_type == parameter.TYPE.EXTERNAL_PSCO
                        and is_psco(_cont)
                        and direction != parameter.DIRECTION.IN
                    ):
                        coll.append([_elem.content_type, _cont.getID()])
                    elif (
                        _elem.content_type == parameter.TYPE.FILE
                        and is_psco(_cont)
                        and direction != parameter.DIRECTION.IN
                    ):
                        coll.append(
                            [parameter.TYPE.EXTERNAL_PSCO, _cont.getID()]
                        )
                    else:
                        if CONTEXT.is_nesting_enabled():
                            if _elem.is_future:
                                coll.append(
                                    [
                                        _elem.content_type,
                                        _elem.file_name.original_path,
                                    ]
                                )
                            else:
                                coll.append([_elem.content_type, "null"])
                        else:
                            coll.append([_elem.content_type, "null"])
            return coll

        # Add parameter types and value
        params_start = 1 if has_self else 0
        params_end = len(args) - num_returns + 1
        # Update new_types and new_values with the args list
        # The results parameter is a boolean to distinguish the error message.
        for arg in args[params_start : params_end - 1]:  # noqa: E203
            # Loop through the arguments and update new_types and new_values
            if not isinstance(arg, Parameter):
                raise PyCOMPSsException(
                    "ERROR: A task parameter arrived as an object instead as"
                    " a TaskParameter when building the task result message."
                )
            original_name = get_name_from_kwarg(arg.name)
            real_direction = get_default_direction(
                original_name, self.decorator_arguments, self.param_args
            )
            param = self.decorator_arguments.get_parameter(
                original_name, real_direction
            )
            if arg.content_type in (
                parameter.TYPE.EXTERNAL_PSCO,
                parameter.TYPE.FILE,
            ):
                # It was originally a persistent object
                if is_psco(arg.content):
                    new_types.append(parameter.TYPE.EXTERNAL_PSCO)
                    new_values.append(arg.content.getID())
                else:
                    new_types.append(arg.content_type)
                    if CONTEXT.is_nesting_enabled():
                        if arg.is_future:
                            new_values.append(arg.file_name.original_path)
                        else:
                            new_values.append("null")
                    else:
                        new_values.append("null")
            elif arg.content_type == parameter.TYPE.COLLECTION:
                # There is a collection that can contain persistent objects
                collection_new_values = build_collection_types_values(
                    arg.content, arg, param.direction
                )
                new_types.append(parameter.TYPE.COLLECTION)
                new_values.append(collection_new_values)
            else:
                # Any other return object: same type and null value
                new_types.append(arg.content_type)
                if CONTEXT.is_nesting_enabled():
                    if arg.is_future:
                        new_values.append(arg.file_name.original_path)
                    else:
                        new_values.append("null")
                else:
                    new_values.append("null")

        # Add self type and value if exist
        if has_self:
            if (
                self.decorator_arguments.target_direction
                == parameter.INOUT.key
            ):
                # Check if self is a PSCO that has been persisted inside the
                # task and target_direction.
                # Update self type and value
                self_type = get_compss_type(args[0])
                if self_type == parameter.TYPE.EXTERNAL_PSCO:
                    self_value = args[0].getID()
                else:
                    # Self can only be of type FILE, so avoid the last update
                    # of self_type
                    if is_psco(args[0]):
                        self_type = parameter.TYPE.EXTERNAL_PSCO
                        self_value = args[0].getID()
                    else:
                        self_type = parameter.TYPE.FILE
                        self_value = "null"
            new_types.append(self_type)
            new_values.append(self_value)

        # Add return types and values
        # Loop through the rest of the arguments and update new_types and
        #  new_values.
        # assert len(args[params_end - 1:]) == len(user_returns)
        # add_parameter_new_types_and_values(args[params_end - 1:], True)
        if num_returns > 0:
            for ret, param in zip(user_returns, ret_params):
                ret_type = get_compss_type(ret)
                ret_value = "null"
                if ret_type == parameter.TYPE.EXTERNAL_PSCO:
                    ret_value = ret.getID()
                elif ret_type == parameter.TYPE.COLLECTION:
                    collection_ret_values = []
                    for elem in ret:
                        if elem.type in (
                            parameter.TYPE.EXTERNAL_PSCO,
                            parameter.TYPE.FILE,
                        ):
                            if is_psco(elem.content):
                                collection_ret_values.append(elem.key)
                            else:
                                if CONTEXT.is_nesting_enabled():
                                    if param.is_future:
                                        collection_ret_values.append(
                                            elem.file_name.original_path
                                        )
                                    else:
                                        collection_ret_values.append("null")
                                else:
                                    collection_ret_values.append("null")
                        else:
                            if CONTEXT.is_nesting_enabled():
                                if param.is_future:
                                    collection_ret_values.append(
                                        elem.file_name.original_path
                                    )
                                else:
                                    collection_ret_values.append("null")
                            else:
                                collection_ret_values.append("null")
                    new_types.append(parameter.TYPE.COLLECTION)
                    new_values.append(collection_ret_values)
                else:
                    # Returns can only be of type FILE, so avoid the last
                    # update of ret_type
                    ret_type = parameter.TYPE.FILE
                    if CONTEXT.is_nesting_enabled():
                        if param.is_future:
                            ret_value = param.file_name.original_path
                        else:
                            ret_value = "null"
                    else:
                        ret_value = "null"
                new_types.append(ret_type)
                new_values.append(ret_value)

        return new_types, new_values


#######################
# AUXILIARY FUNCTIONS #
#######################


def clean_cupy_env():
    """Clean cupy environment.

    :return: None
    """
    try:
        import cupy

        CACHE_TRACKER.close_cupy_mem_handles()
    except ImportError:
        pass


def get_collection_objects(
    content: typing.Any, argument: Parameter
) -> typing.Generator[typing.Tuple[typing.Any, Parameter], None, None]:
    """Retrieve collection objects recursively generator.

    WARNING! Updates the collection with any modification from content.

    :param content: Object or list of objects.
    :param argument: Argument or list of arguments of the given objects.
    :return: The collection representation.
    """
    if argument.content_type == parameter.TYPE.COLLECTION:
        for new_con, _elem in zip(
            argument.content, argument.collection_content
        ):
            # Update the sub-parameter content with the existing content
            # to keep track of the synchronized.
            _elem.content = new_con
            for sub_el, sub_param in get_collection_objects(new_con, _elem):
                # Update the sub-parameter content with the existing content
                # to keep track of the synchronized.
                sub_param.content = sub_el
                yield sub_el, sub_param
    else:
        # Update the sub-parameter content with the existing content
        # to keep track of the synchronized.
        argument.content = content
        # NOTE: Synchronization with nesting is not needed anymore since
        #       we are delegating the writing of the file to the nested
        #       task.
        yield content, argument


def get_dict_collection_objects(
    content: typing.Any, argument: Parameter
) -> typing.Generator[typing.Tuple[typing.Any, Parameter], None, None]:
    """Retrieve the dictionary collection objects recursively generator.

    WARNING! Updates the dictionary collection with any modification
             from content.

    :param content: Object or list of objects.
    :param argument: Argument or list of arguments of the given objects.
    :return: The collection representation.
    """
    if argument.content_type == parameter.TYPE.DICT_COLLECTION:
        elements = []
        for content_k, content_v in argument.content.items():
            elements.extend([content_k, content_v])
        # Prepare dict_collection_content per key
        element_parameters_preproc = {}
        for (
            dict_coll_k,
            dict_coll_v,
        ) in argument.dict_collection_content.items():
            element_parameters_preproc[dict_coll_k.content] = [
                dict_coll_k,
                dict_coll_v,
            ]
        # Ensure that the element parameters are in the same order as
        # argument.content
        elements_parameters = []
        for content_k in argument.content.keys():
            elements_parameters.extend(
                [
                    element_parameters_preproc[content_k][0],
                    element_parameters_preproc[content_k][1],
                ]
            )
        # Loop recursively
        for new_con, _elem in zip(elements, elements_parameters):
            _elem.content = new_con
            for sub_el, sub_param in get_dict_collection_objects(
                new_con, _elem
            ):
                # Update the sub-parameter content with the existing content
                # to keep track of the synchronized.
                sub_param.content = sub_el
                yield sub_el, sub_param
    else:
        # Update the sub-parameter content with the existing content
        # to keep track of the synchronized.
        argument.content = content
        # NOTE: Synchronization with nesting is not needed anymore since
        #       we are delegating the writing of the file to the nested
        #       task.
        yield content, argument


def get_ret_rank(_ret_params: list) -> list:
    """Retrieve the rank id within MPI.

    :param _ret_params: Return parameters.
    :return: Integer return rank.
    """
    from mpi4py import MPI  # pylint: disable=import-outside-toplevel

    return [_ret_params[MPI.COMM_WORLD.rank]]
