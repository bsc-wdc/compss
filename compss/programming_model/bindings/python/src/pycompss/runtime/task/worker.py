#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import os
import sys

import pycompss.api.parameter as parameter
from pycompss.api.exceptions import COMPSsException
from pycompss.runtime.task.commons import TaskCommons
from pycompss.runtime.commons import TRACING_HOOK_ENV_VAR
from pycompss.runtime.task.parameter import Parameter
from pycompss.runtime.task.parameter import get_compss_type
from pycompss.runtime.task.arguments import get_varargs_name
from pycompss.runtime.task.arguments import get_name_from_kwarg
from pycompss.runtime.task.arguments import is_vararg
from pycompss.runtime.task.arguments import is_kwarg
from pycompss.runtime.task.arguments import is_return
from pycompss.util.objects.properties import create_object_by_con_type
from pycompss.util.storages.persistent import is_psco
from pycompss.util.serialization.serializer import deserialize_from_file
from pycompss.util.serialization.serializer import serialize_to_file
from pycompss.util.serialization.serializer import serialize_to_file_mpienv
from pycompss.util.std.redirects import std_redirector
from pycompss.util.std.redirects import not_std_redirector
from pycompss.worker.commons.worker import build_task_parameter

if __debug__:
    import logging
    logger = logging.getLogger(__name__)


class TaskWorker(TaskCommons):
    """
    Task code for the Worker:

    Process the task decorator and prepare call the user function.
    """

    def __init__(self,
                 decorator_arguments,
                 user_function):
        # Initialize TaskCommons
        super(self.__class__, self).__init__(decorator_arguments, None, None)
        # User function
        self.user_function = user_function

    def call(self, *args, **kwargs):
        # type: (tuple, dict) -> (list, list, list)
        """ Main task code at worker side.
        This function deals with task calls in the worker's side
        Note that the call to the user function is made by the worker,
        not by the user code.

        :return: A function that calls the user function with the given
                 parameters and does the proper serializations and updates
                 the affected objects.
        """
        # Grab logger from kwargs (shadows outer logger since it is set by
        # the worker).
        logger = kwargs['logger']  # noqa
        if __debug__:
            logger.debug("Starting @task decorator worker call")

        if __debug__:
            logger.debug("Revealing objects")
        # All parameters are in the same args list. At the moment we only know
        # the type, the name and the "value" of the parameter. This value may
        # be treated to get the actual object (e.g: deserialize it, query the
        # database in case of persistent objects, etc.)
        self.reveal_objects(args, logger)
        if __debug__:
            logger.debug("Finished revealing objects")
            logger.debug("Building task parameters structures")

        # After this line all the objects in arg have a "content" field, now
        # we will segregate them in User positional and variadic args
        user_args, user_kwargs, ret_params = self.segregate_objects(args)
        num_returns = len(ret_params)

        if __debug__:
            logger.debug("Finished building parameters structures.")

        # Self definition (only used when defined in the task)
        # Save the self object type and value before executing the task
        # (it could be persisted inside if its a persistent object)
        self_type = None
        self_value = None
        has_self = False
        if args and not isinstance(args[0], Parameter):
            if __debug__:
                logger.debug("Detected self parameter")
            # Then the first arg is self
            has_self = True
            self_type = get_compss_type(args[0])
            if self_type == parameter.TYPE.EXTERNAL_PSCO:
                if __debug__:
                    logger.debug("\t - Self is a PSCO")
                self_value = args[0].getID()
            else:
                # Since we are checking the type of the deserialized self
                # parameter, get_compss_type will return that its type is
                # parameter.TYPE.OBJECT, which although it is an object, self
                # is always a file for the runtime. So we must force its type
                # to avoid that the return message notifies that it has a new
                # type "object" which is not supported for python objects in
                # the runtime.
                self_type = parameter.TYPE.FILE
                self_value = 'null'

        # Call the user function with all the reconstructed parameters and
        # get the return values.
        redirect_std = True
        if kwargs['compss_log_files']:
            # Redirect all stdout and stderr during the user code execution
            # jo job out and err files.
            job_out, job_err = kwargs['compss_log_files']
        else:
            job_out, job_err = None, None
            redirect_std = False

        if __debug__:
            logger.debug("Redirecting stdout to: " + str(job_out))
            logger.debug("Redirecting stderr to: " + str(job_err))

        with std_redirector(job_out, job_err) if redirect_std else not_std_redirector():  # noqa: E501
            if __debug__:
                logger.debug("Invoking user code")
            # Now execute the user code
            result = self.execute_user_code(user_args,
                                            user_kwargs,
                                            kwargs['_compss_tracing'])
            user_returns, compss_exception = result
            if __debug__:
                logger.debug("Finished user code")

        python_mpi = False
        if kwargs["python_MPI"]:
            python_mpi = True

        # Deal with INOUTs and COL_OUTs
        self.manage_inouts(args, python_mpi)

        # Deal with COMPSsExceptions
        if compss_exception is not None:
            if __debug__:
                logger.debug("Detected COMPSs Exception. Raising.")
            raise compss_exception

        # Deal with returns (if any)
        user_returns = self.manage_returns(num_returns, user_returns,
                                           ret_params, python_mpi)

        # Check old targetDirection
        if 'targetDirection' in self.decorator_arguments:
            target_label = 'targetDirection'
        else:
            target_label = 'target_direction'

        # We must notify COMPSs when types are updated
        new_types, new_values = self.manage_new_types_values(num_returns,
                                                             user_returns,
                                                             args,
                                                             has_self,
                                                             target_label,
                                                             self_type,
                                                             self_value)

        if __debug__:
            logger.debug("Finished @task decorator")

        return new_types, new_values, self.decorator_arguments[target_label]

    def reveal_objects(self, args, logger):  # noqa
        # type: (tuple, logger) -> None
        """ Get the objects from the args message.
        This function takes the arguments passed from the persistent worker
        and treats them to get the proper parameters for the user function.

        :param args: Arguments.
        :param logger: Logger (shadows outer logger since this is only used
                               in the worker to reveal the parameter objects).
        :return: None
        """
        if self.storage_supports_pipelining():
            if __debug__:
                logger.debug("The storage supports pipelining.")
            # Perform the pipelined getByID operation
            pscos = [x for x in args if
                     x.content_type == parameter.TYPE.EXTERNAL_PSCO]
            identifiers = [x.content for x in pscos]
            from storage.api import getByID  # noqa
            objects = getByID(*identifiers)
            # Just update the Parameter object with its content
            for (obj, value) in zip(objects, pscos):
                obj.content = value

        # Deal with all the parameters that are NOT returns
        for arg in [x for x in args if
                    isinstance(x, Parameter) and not is_return(x.name)]:
            self.retrieve_content(arg, "")

    @staticmethod
    def storage_supports_pipelining():
        # type: () -> bool
        """ Check if storage supports pipelining.
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

    def retrieve_content(self, argument, name_prefix, depth=0):
        # type: (Parameter, str, int) -> None
        """ Retrieve the content of a particular argument.

        :param argument: Argument.
        :param name_prefix: Name prefix.
        :param depth: Collection depth (0 if not a collection).
        :return: None
        """
        if __debug__:
            logger.debug("\t - Revealing: " + str(argument.name))
        # This case is special, as a FILE can actually mean a FILE or an
        # object that is serialized in a file
        if is_vararg(argument.name):
            self.param_varargs = argument.name
            if __debug__:
                logger.debug("\t\t - It is vararg")
        if argument.content_type == parameter.TYPE.FILE:
            if self.is_parameter_an_object(argument.name):
                # The object is stored in some file, load and deserialize
                f_name = argument.file_name.split(':')[-1]
                if __debug__:
                    logger.debug("\t\t - It is an OBJECT. Deserializing from file: " + str(f_name))  # noqa: E501
                argument.content = deserialize_from_file(f_name)
                if __debug__:
                    logger.debug("\t\t - Deserialization finished")
            else:
                # The object is a FILE, just forward the path of the file
                # as a string parameter
                argument.content = argument.file_name.split(':')[-1]
                if __debug__:
                    logger.debug("\t\t - It is FILE: " + str(argument.content))
        elif argument.content_type == parameter.TYPE.DIRECTORY:
            if __debug__:
                logger.debug("\t\t - It is a DIRECTORY")
            argument.content = argument.file_name.split(":")[-1]
        elif argument.content_type == parameter.TYPE.EXTERNAL_STREAM:
            if __debug__:
                logger.debug("\t\t - It is an EXTERNAL STREAM")
            argument.content = deserialize_from_file(argument.file_name)
        elif argument.content_type == parameter.TYPE.COLLECTION:
            argument.content = []
            # This field is exclusive for COLLECTION_T parameters, so make
            # sure you have checked this parameter is a collection before
            # consulting it
            argument.collection_content = []
            col_f_name = argument.file_name.split(':')[-1]

            # maybe it is an inner-collection..
            _dec_arg = self.decorator_arguments.get(argument.name, None)
            _col_dir = _dec_arg.direction if _dec_arg else None
            _col_dep = _dec_arg.depth if _dec_arg else depth

            if __debug__:
                logger.debug("\t\t - It is a COLLECTION: " +
                             str(col_f_name))
                logger.debug("\t\t\t - Depth: " + str(_col_dep))

            for (i, line) in enumerate(open(col_f_name, 'r')):
                data_type, content_file, content_type = line.strip().split()  # noqa: E501
                # Same naming convention as in COMPSsRuntimeImpl.java
                sub_name = "%s.%d" % (argument.name, i)
                if name_prefix:
                    sub_name = "%s.%s" % (name_prefix, argument.name)
                else:
                    sub_name = "@%s" % sub_name

                if __debug__:
                    logger.debug("\t\t\t - Revealing element: " +
                                 str(sub_name))

                if not self.is_parameter_file_collection(argument.name):
                    sub_arg, _ = build_task_parameter(int(data_type),
                                                      parameter.IOSTREAM.UNSPECIFIED,  # noqa: E501
                                                      "",
                                                      sub_name,
                                                      content_file,
                                                      argument.content_type)

                    # if direction of the collection is 'out', it means we
                    # haven't received serialized objects from the Master
                    # (even though parameters have 'file_name', those files
                    # haven't been created yet). plus, inner collections of
                    # col_out params do NOT have 'direction', we identify
                    # them by 'depth'..
                    if _col_dir == parameter.DIRECTION.OUT or \
                            ((_col_dir is None) and _col_dep > 0):

                        # if we are at the last level of COL_OUT param,
                        # create 'empty' instances of elements
                        if _col_dep == 1:
                            temp = create_object_by_con_type(content_type)
                            sub_arg.content = temp
                            argument.content.append(sub_arg.content)
                            argument.collection_content.append(sub_arg)
                        else:
                            self.retrieve_content(sub_arg, sub_name,
                                                  depth=_col_dep - 1)
                            argument.content.append(sub_arg.content)
                            argument.collection_content.append(sub_arg)
                    else:
                        # Recursively call the retrieve method, fill the
                        # content field in our new taskParameter object
                        self.retrieve_content(sub_arg, sub_name)
                        argument.content.append(sub_arg.content)
                        argument.collection_content.append(sub_arg)
                else:
                    argument.content.append(content_file)
                    argument.collection_content.append(content_file)

        elif not self.storage_supports_pipelining() and \
                argument.content_type == parameter.TYPE.EXTERNAL_PSCO:
            if __debug__:
                logger.debug("\t\t - It is a PSCO")
            # The object is a PSCO and the storage does not support
            # pipelining, do a single getByID of the PSCO
            from storage.api import getByID  # noqa
            argument.content = getByID(argument.content)
            # If we have not entered in any of these cases we will assume
            # that the object was a basic type and the content is already
            # available and properly casted by the python worker

    def segregate_objects(self, args):
        # type: (tuple) -> (list, dict, list)
        """ Split a list of arguments.
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
                user_kwargs[get_name_from_kwarg(arg.name)] = \
                    arg.content
            else:
                if is_vararg(arg.name):
                    self.param_varargs = get_varargs_name(arg.name)
                # Apart from the names we preserve the original order, so it
                # is guaranteed that named positional arguments will never be
                # swapped with variadic ones or anything similar
                user_args.append(arg.content)

        return user_args, user_kwargs, ret_params

    def execute_user_code(self, user_args, user_kwargs, tracing):
        # type: (list, dict, bool) -> (object, COMPSsException)
        """ Executes the user code.
        Disables the tracing hook if tracing is enabled. Restores it
        at the end of the user code execution.

        :param user_args: Function args.
        :param user_kwargs: Function kwargs.
        :param tracing: If tracing enabled.
        :return: The user function returns and the compss exception (if any).
        """
        # Tracing hook is disabled by default during the user code of the task.
        # The user can enable it with tracing_hook=True in @task decorator for
        # specific tasks or globally with the COMPSS_TRACING_HOOK=true
        # environment variable.
        restore_hook = False
        pro_f = None
        if tracing:
            global_tracing_hook = False
            if TRACING_HOOK_ENV_VAR in os.environ:
                hook_enabled = os.environ[TRACING_HOOK_ENV_VAR] == "true"
                global_tracing_hook = hook_enabled
            if self.decorator_arguments['tracing_hook'] or global_tracing_hook:
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

        user_returns = None
        compss_exception = None
        if self.decorator_arguments['numba']:
            # Import all supported functionalities
            from numba import jit
            from numba import njit
            from numba import generated_jit
            from numba import vectorize
            from numba import guvectorize
            from numba import stencil
            from numba import cfunc
            numba_mode = self.decorator_arguments['numba']
            numba_flags = self.decorator_arguments['numba_flags']
            if type(numba_mode) is dict:
                # Use the flags defined by the user
                numba_flags['cache'] = True  # Always force cache
                user_returns = jit(self.user_function,
                                   **numba_flags)(*user_args, **user_kwargs)
            elif numba_mode is True or numba_mode == 'jit':
                numba_flags['cache'] = True  # Always force cache
                user_returns = jit(self.user_function,
                                   **numba_flags)(*user_args, **user_kwargs)
                # Alternative way of calling:
                # user_returns = jit(cache=True)(self.user_function) \
                #                   (*user_args, **user_kwargs)
            elif numba_mode == 'generated_jit':
                user_returns = generated_jit(self.user_function,
                                             **numba_flags)(*user_args,
                                                            **user_kwargs)
            elif numba_mode == 'njit':
                numba_flags['cache'] = True  # Always force cache
                user_returns = njit(self.user_function,
                                    **numba_flags)(*user_args, **user_kwargs)
            elif numba_mode == 'vectorize':
                numba_signature = self.decorator_arguments['numba_signature']  # noqa: E501
                user_returns = vectorize(
                    numba_signature,
                    **numba_flags
                )(self.user_function)(*user_args, **user_kwargs)
            elif numba_mode == 'guvectorize':
                numba_signature = self.decorator_arguments['numba_signature']  # noqa: E501
                numba_decl = self.decorator_arguments['numba_declaration']
                user_returns = guvectorize(
                    numba_signature,
                    numba_decl,
                    **numba_flags
                )(self.user_function)(*user_args, **user_kwargs)
            elif numba_mode == 'stencil':
                user_returns = stencil(
                    **numba_flags
                )(self.user_function)(*user_args, **user_kwargs)
            elif numba_mode == 'cfunc':
                numba_signature = self.decorator_arguments['numba_signature']  # noqa: E501
                user_returns = cfunc(
                    numba_signature
                )(self.user_function).ctypes(*user_args, **user_kwargs)
            else:
                raise Exception("Unsupported numba mode.")
        else:
            try:
                # Normal task execution
                user_returns = self.user_function(*user_args, **user_kwargs)
            except COMPSsException as ce:
                compss_exception = ce
                # Check old targetDirection
                if 'targetDirection' in self.decorator_arguments:
                    target_label = 'targetDirection'
                else:
                    target_label = 'target_direction'
                compss_exception.target_direction = self.decorator_arguments[target_label]  # noqa: E501

        # Reestablish the hook if it was disabled
        if restore_hook:
            sys.setprofile(pro_f)

        return user_returns, compss_exception

    def manage_inouts(self, args, python_mpi):
        # type: (tuple, bool) -> None
        """ Deal with INOUTS. Serializes the result of INOUT parameters.

        :param args: Argument list.
        :param python_mpi: Boolean if python mpi.
        :return: None
        """
        if __debug__:
            logger.debug("Dealing with INOUTs and OUTS")
            if python_mpi:
                logger.debug("\t - Managing with MPI policy")

        # Manage all the possible outputs of the task and build the return new
        # types and values
        for arg in args:
            # handle only task parameters that are objects

            # skip files and non-task-parameters
            if not isinstance(arg, Parameter) or \
                    not self.is_parameter_an_object(arg.name):
                continue

            # file collections are objects, but must be skipped as well
            if self.is_parameter_file_collection(arg.name):
                continue

            # skip psco
            # since param.content_type has the old type, we can not use:
            #     param.content_type != parameter.TYPE.EXTERNAL_PSCO
            _is_psco_true = (arg.content_type == parameter.TYPE.EXTERNAL_PSCO or
                             is_psco(arg.content))
            if _is_psco_true:
                continue

            original_name = get_name_from_kwarg(arg.name)
            param = self.decorator_arguments.get(
                original_name, self.get_default_direction(original_name))

            # skip non-inouts or non-col_outs
            _is_col_out = (arg.content_type == parameter.TYPE.COLLECTION and
                           param.direction == parameter.DIRECTION.OUT)

            _is_inout = (param.direction == parameter.DIRECTION.INOUT or
                         param.direction == parameter.DIRECTION.COMMUTATIVE)

            if not (_is_inout or _is_col_out):
                continue

            # Now it's 'INOUT' or 'COLLLECTION_OUT' object param, serialize
            # to a file
            if arg.content_type == parameter.TYPE.COLLECTION:
                if __debug__:
                    logger.debug("Serializing collection: " + str(arg.name))
                # handle collections recursively
                for (content, elem) in __get_collection_objects__(arg.content, arg):  # noqa: E501
                    if elem.file_name:
                        f_name = __get_file_name__(elem.file_name)
                        if __debug__:
                            logger.debug("\t - Serializing element: " +
                                         str(arg.name) + " to " + str(f_name))
                        if python_mpi:
                            serialize_to_file_mpienv(content, f_name, False)
                        else:
                            serialize_to_file(content, f_name)
                    else:
                        # It is None --> PSCO
                        pass
            else:
                f_name = __get_file_name__(arg.file_name)
                if __debug__:
                    logger.debug("Serializing object: " +
                                 str(arg.name) + " to " + str(f_name))
                if python_mpi:
                    serialize_to_file_mpienv(arg.content, f_name, False)
                else:
                    serialize_to_file(arg.content, f_name)

    @staticmethod
    def manage_returns(num_returns, user_returns, ret_params, python_mpi):
        # type: (int, list, list, bool) -> list
        """ Manage task returns.

        :param num_returns: Number of returns.
        :param user_returns: User returns.
        :param ret_params: Return parameters.
        :param python_mpi: Boolean if is python mpi code.
        :return: User returns.
        """
        if __debug__:
            logger.debug("Dealing with returns: " + str(num_returns))
        if num_returns > 0:
            if num_returns == 1:
                # Generalize the return case to multi-return to simplify the
                # code
                user_returns = [user_returns]
            elif num_returns > 1 and python_mpi:
                user_returns = [user_returns]
                ret_params = __get_ret_rank__(ret_params)
            # Note that we are implicitly assuming that the length of the user
            # returns matches the number of return parameters
            for (obj, param) in zip(user_returns, ret_params):
                # If the object is a PSCO, do not serialize to file
                if param.content_type == parameter.TYPE.EXTERNAL_PSCO or is_psco(obj):
                    continue
                # Serialize the object
                # Note that there is no "command line optimization" in the
                # returns, as we always pass them as files.
                # This is due to the asymmetry in worker-master communications
                # and because it also makes it easier for us to deal with
                # returns in that format
                f_name = __get_file_name__(param.file_name)
                if __debug__:
                    logger.debug("Serializing return: " + str(f_name))
                if python_mpi:
                    if num_returns > 1:
                        rank_zero_reduce = False
                    else:
                        rank_zero_reduce = True

                    serialize_to_file_mpienv(obj, f_name, rank_zero_reduce)
                else:
                    serialize_to_file(obj, f_name)
        return user_returns

    def is_parameter_an_object(self, name):
        # type: (str) -> bool
        """ Given the name of a parameter, determine if it is an object or not.

        :param name: Name of the parameter.
        :return: True if the parameter is a (serializable) object.
        """
        original_name = get_name_from_kwarg(name)
        # Get the args parameter object
        if is_vararg(original_name):
            return self.get_varargs_direction().content_type is None
        # Is this parameter annotated in the decorator?
        if original_name in self.decorator_arguments:
            annotated = [parameter.TYPE.COLLECTION,
                         parameter.TYPE.EXTERNAL_STREAM,
                         None]
            return self.decorator_arguments[original_name].content_type in annotated
        # The parameter is not annotated in the decorator, so (by default)
        # return True
        return True

    def is_parameter_file_collection(self, name):
        # type: (str) -> bool
        """ Given the name of a parameter, determine if it is a file
        collection or not.

        :param name: Name of the parameter.
        :return: True if the parameter is a file collection.
        """
        original_name = get_name_from_kwarg(name)
        # Get the args parameter object
        if is_vararg(original_name):
            return self.get_varargs_direction().is_file_collection
        # Is this parameter annotated in the decorator?
        if original_name in self.decorator_arguments:
            return self.decorator_arguments[original_name].is_file_collection
        # The parameter is not annotated in the decorator, so (by default)
        # return False
        return False

    def manage_new_types_values(self,
                                num_returns,   # type: int
                                user_returns,  # type: list
                                args,          # type: tuple
                                has_self,      # type: bool
                                target_label,  # type: str
                                self_type,     # type: str
                                self_value     # type: object
                                ):
        # type: (...) -> (list, list)
        """ Manage new types and values.
        We must notify COMPSs when types are updated
        Potential update candidates are returns and INOUTs
        But the whole types and values list must be returned
        new_types and new_values correspond to "parameters self returns"

        :param num_returns: Number of returns.
        :param user_returns: User returns.
        :param args: Arguments.
        :param has_self: If has self.
        :param target_label: Target label (self, cls, etc.).
        :param self_type: Self type.
        :param self_value: Self value.
        :return: List new types, List new values.
        """
        new_types, new_values = [], []

        if __debug__:
            logger.debug("Building types update")

        # Add parameter types and value
        params_start = 1 if has_self else 0
        params_end = len(args) - num_returns + 1
        # Update new_types and new_values with the args list
        # The results parameter is a boolean to distinguish the error message.
        for arg in args[params_start:params_end - 1]:
            # Loop through the arguments and update new_types and new_values
            if not isinstance(arg, Parameter):
                raise Exception('ERROR: A task parameter arrived as an' +
                                ' object instead as a Parameter' +
                                ' when building the task result message.')
            else:
                original_name = get_name_from_kwarg(arg.name)
                param = self.decorator_arguments.get(original_name,
                                                     self.get_default_direction(original_name))  # noqa: E501
                if arg.content_type == parameter.TYPE.EXTERNAL_PSCO:
                    # It was originally a persistent object
                    new_types.append(parameter.TYPE.EXTERNAL_PSCO)
                    new_values.append(arg.content)
                elif is_psco(arg.content) and \
                        param.direction != parameter.DIRECTION.IN:
                    # It was persisted in the task
                    new_types.append(parameter.TYPE.EXTERNAL_PSCO)
                    new_values.append(arg.content.getID())
                else:
                    # Any other return object: same type and null value
                    new_types.append(arg.content_type)
                    new_values.append('null')

        # Add self type and value if exist
        if has_self:
            if self.decorator_arguments[target_label].direction == parameter.DIRECTION.INOUT:  # noqa: E501
                # Check if self is a PSCO that has been persisted inside the
                # task and target_direction.
                # Update self type and value
                self_type = get_compss_type(args[0])
                if self_type == parameter.TYPE.EXTERNAL_PSCO:
                    self_value = args[0].getID()
                else:
                    # Self can only be of type FILE, so avoid the last update
                    # of self_type
                    self_type = parameter.TYPE.FILE
                    self_value = 'null'
            new_types.append(self_type)
            new_values.append(self_value)

        # Add return types and values
        # Loop through the rest of the arguments and update new_types and
        #  new_values.
        # assert len(args[params_end - 1:]) == len(user_returns)
        # add_parameter_new_types_and_values(args[params_end - 1:], True)
        if num_returns > 0:
            for ret in user_returns:
                ret_type = get_compss_type(ret)
                if ret_type == parameter.TYPE.EXTERNAL_PSCO:
                    ret_value = ret.getID()
                else:
                    # Returns can only be of type FILE, so avoid the last
                    # update of ret_type
                    ret_type = parameter.TYPE.FILE
                    ret_value = 'null'
                new_types.append(ret_type)
                new_values.append(ret_value)

        return new_types, new_values


#######################
# AUXILIARY FUNCTIONS #
#######################

def __get_collection_objects__(content, argument):
    """ Retrieve collection objects recursively. """
    if argument.content_type == parameter.TYPE.COLLECTION:
        for (new_con, _elem) in zip(argument.content,
                                    argument.collection_content):
            for sub_el in __get_collection_objects__(new_con, _elem):
                yield sub_el
    else:
        yield content, argument


def __get_file_name__(file_path):
    # type: (str) -> str
    """ Retrieve the file name from an absolute file path.

    :param file_path: Absolute file path.
    :return: File name.
    """
    return file_path.split(':')[-1]


def __get_ret_rank__(_ret_params):
    # type: (list) -> list
    """ Retrieve the rank id within MPI.

    :param _ret_params: Return parameters.
    :return: Integer return rank.
    """
    from mpi4py import MPI
    return [_ret_params[MPI.COMM_WORLD.rank]]
