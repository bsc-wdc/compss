#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
"""
import json
import inspect
from functools import wraps

from pycompss.runtime.task.commons import get_varargs_direction
from pycompss.util.context import CONTEXT
from pycompss.api import binary
from pycompss.runtime.task.arguments import is_kwarg
from pycompss.runtime.task.arguments import is_return
from pycompss.runtime.task.arguments import is_vararg
from pycompss.util.typing_helper import typing
from pycompss.runtime.task.parameter import Parameter
from pycompss.runtime.task.arguments import get_name_from_kwarg
from pycompss.runtime.task.arguments import get_name_from_vararg


if __debug__:
    import logging

    logger = logging.getLogger(__name__)


class SoftConverter:  # pylint: disable=too-few-public-methods, too-many-instance-attributes
    """
    """

    __slots__ = [
        "decorator_name",
        "args",
        "kwargs",
        "scope",
        "core_element",
        "core_element_configured",
        "task_type",
        "config_args",
        "decor",
        "constraints",
        "container",
        "prolog",
        "epilog",
        "parameters",
        "is_workflow",
        "param_varargs"
    ]

    def __init__(self, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Parse the config file and store arguments passed to the decorator.

        self = itself.
        args = not used.
        kwargs = dictionary with the given @software parameter (config_file).

        :param args: Arguments
        :param kwargs: Keyword arguments
        """
        decorator_name = "".join(("@", SoftConverter.__name__.lower()))
        # super(Software, self).__init__(decorator_name, *args, **kwargs)
        self.task_type = None  # type: typing.Any
        self.config_args = None  # type: typing.Any
        self.decor = None  # type: typing.Any
        self.constraints = None  # type: typing.Any
        self.container = None  # type: typing.Any
        self.prolog = None  # type: typing.Any
        self.epilog = None  # type: typing.Any
        self.parameters = dict()  # type: typing.Dict
        self.is_workflow = False  # type: bool
        self.param_varargs = None  # type: typing.Any

        self.decorator_name = decorator_name
        self.args = args
        self.kwargs = kwargs
        self.scope = CONTEXT.in_pycompss()
        self.core_element = None  # type: typing.Any
        self.core_element_configured = False

        if self.scope and CONTEXT.in_master():
            if __debug__:
                logger.debug("Replacing @software decorator via converter..")

    def __call__(self, user_function: typing.Callable) -> typing.Callable:
        """
        """

        @wraps(user_function)
        def converter_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            if not self.scope:
                # todo: what to do?
                pass

            if CONTEXT.in_master():
                self.parameters = kwargs.pop("compss_task_parameters")
                orig_func = kwargs.pop("compss_task_func")
                self.replace_param_types()
                kwargs.update(self.parameters)
                tmp_file = generate_task_file(orig_func, self.parameters)
                return user_function(*args, compss_task_file=tmp_file, **kwargs)
            else:
                print("__________________ user args")
                for arg in args:
                    print(arg)
                print("___________________________ user_kwargs")
                self.reveal_objects(
                    args,
                    kwargs["compss_collections_layouts"],
                    kwargs["compss_python_MPI"],
                )
                user_args, user_kwargs, ret_params = \
                    self.segregate_objects(args)
                print("__________________ user args")
                print(user_args)
                print("___________________________ user_kwargs")
                print(user_kwargs)
                print("___________________________")
                return "hola"

        converter_f.__doc__ = user_function.__doc__
        return converter_f

    def replace_param_types(self):
        from pycompss.api import parameter
        if not self.parameters:
            return
        for k, v in self.parameters.items():
            if isinstance(v, str) and hasattr(parameter, v):
                self.parameters[k] = getattr(parameter, v)

    def reveal_objects(
        self,
        args: tuple,
        collections_layouts: typing.Any,
        # typing.Union[typing.Dict[str, typing.Union[tuple, list]], None]
        python_mpi: typing.Any = False,
        # typing.Union[bool, None]
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
            pscos = [x for x in args if x.content_type == parameter.TYPE.EXTERNAL_PSCO]
            identifiers = [x.content for x in pscos]
            from storage.api import getByID  # noqa

            objects = getByID(*identifiers)
            # Just update the Parameter object with its content
            for (obj, value) in zip(objects, pscos):
                obj.content = value

        # Deal with all the parameters that are NOT returns
        for arg in [
            x for x in args if isinstance(x, Parameter) and not is_return(x.name)
        ]:
            self.retrieve_content(arg, "", python_mpi, collections_layouts)

    def is_parameter_an_object(self, name: str) -> bool:
        """Given the name of a parameter, determine if it is an object or not.

        :param name: Name of the parameter.
        :return: True if the parameter is a (serializable) object.
        """
        original_name = get_name_from_kwarg(name)
        # Get the args parameter object
        if is_vararg(original_name):
            self.param_varargs, varargs_direction = get_varargs_direction(
                self.param_varargs, self.parameters
            )
            return varargs_direction.content_type == -1
        # Is this parameter annotated in the decorator?
        from pycompss.api import parameter
        if original_name in self.parameters:
            annotated = [
                parameter.TYPE.COLLECTION,
                parameter.TYPE.DICT_COLLECTION,
                parameter.TYPE.EXTERNAL_STREAM,
                -1,
            ]
            return self.parameters[original_name].content_type in annotated
        # The parameter is not annotated in the decorator, so return default
        return True

    def retrieve_content(
        self,
        argument: Parameter,
        name_prefix: str,
        python_mpi: typing.Any,
        # Union[bool, None]
        collections_layouts: typing.Any,
        # typing.Union[typing.Dict[str, typing.Union[tuple, list]], typing.Any]
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
        :param force_file: Force file type for collections or dict_collections of files.
        :return: None
        """
        if __debug__:
            LOGGER.debug("\t - Revealing: " + str(argument.name))
            LOGGER.debug("\t - checking: " + str(get_name_from_kwarg(argument.name)))
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
                # The object is stored in some file, load and deserialize
                if __debug__:
                    LOGGER.debug(
                        "\t\t - It is an OBJECT. Deserializing from file: %s",
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
                    LOGGER.debug("\t\t - It is FILE: %s", str(argument.content))
        elif content_type == type_directory:
            if __debug__:
                LOGGER.debug("\t\t - It is a DIRECTORY")
            argument.content = argument.file_name.original_path
        elif content_type == type_external_stream:
            if __debug__:
                LOGGER.debug("\t\t - It is an EXTERNAL STREAM")
            argument.content = self.recover_object(argument)
        elif content_type == type_collection:
            argument.content = []
            # This field is exclusive for COLLECTION_T parameters, so make
            # sure you have checked this parameter is a collection before
            # consulting it
            argument.collection_content = []
            col_f_name = str(argument.file_name.original_path)

            # maybe it is an inner-collection..
            _dec_arg = self.parameters.get(argument.name, None)
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
                rank_distribution = rank_distributor(collections_layouts[argument.name])
                rank_distr_len = len(rank_distribution)
                if __debug__:
                    LOGGER.debug("Rank distribution is: %s", str(rank_distribution))

            with open(col_f_name, "r") as col_f_name_fd:
                for (i, line) in enumerate(col_f_name_fd):
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
                        LOGGER.debug("\t\t\t - Revealing element: %s", str(sub_name))

                    is_file_collection = self.is_parameter_file_collection(
                        argument.name
                    )
                    is_really_file = is_file_collection or content_type_elem == "FILE"
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
                        if _col_dep == 1 or content_type_elem != "collection:list":
                            # Not a nested collection anymore
                            if is_really_file:
                                sub_arg.content = content_file
                                sub_arg.content_type = parameter.TYPE.FILE
                            else:
                                temp = create_object_by_con_type(content_type_elem)
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
            _dec_arg = self.parameters.get(argument.name, None)
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
                    if _dict_col_dep == 1 or content_type_elem != "collection:dict":
                        if content_type_elem == "FILE":
                            temp_k = content_file_key
                            temp_v = content_file_value
                        else:
                            temp_k = create_object_by_con_type(content_type_key)
                            temp_v = create_object_by_con_type(content_type_value)
                        sub_arg_key.content = temp_k
                        sub_arg_value.content = temp_v
                        argument.content[sub_arg_key.content] = sub_arg_value.content
                        argument.dict_collection_content[sub_arg_key] = sub_arg_value
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
                        argument.content[sub_arg_key.content] = sub_arg_value.content
                        argument.dict_collection_content[sub_arg_key] = sub_arg_value
                else:
                    # Recursively call the retrieve method, fill the
                    # content field in our new taskParameter object
                    self.retrieve_content(
                        sub_arg_key, sub_name_key, python_mpi, collections_layouts
                    )
                    self.retrieve_content(
                        sub_arg_value, sub_name_value, python_mpi, collections_layouts
                    )
                    argument.content[
                        sub_arg_key.content
                    ] = sub_arg_value.content  # noqa: E501
                    argument.dict_collection_content[
                        sub_arg_key
                    ] = sub_arg_value  # noqa: E501
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

    def is_parameter_file_collection(self, name: str) -> bool:
        """Determine if the given parameter name it is a file collection or not.

        :param name: Name of the parameter.
        :return: True if the parameter is a file collection.
        """
        original_name = get_name_from_kwarg(name)
        # Get the args parameter object
        if is_vararg(original_name):
            self.param_varargs, varargs_direction = get_varargs_direction(
                self.param_varargs, self.parameters
            )
            return varargs_direction.is_file_collection
        # Is this parameter annotated in the decorator?
        if original_name in self.parameters:
            return self.parameters[original_name].is_file_collection
        # The parameter is not annotated in the decorator, so (by default)
        # return False
        return False

    def segregate_objects(self, args):
        # type: (tuple) -> typing.Tuple[list, dict, list]
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


def generate_task_file(user_function, parameters):

    tc = inspect.getsource(user_function)
    lines = tc.split("\n")
    lines = lines[1:]
    tbi = "@task(" + str(format_json(parameters)) + ")"
    lines.insert(0, tbi)
    lines = "\n".join(lines)
    orig_name = inspect.getsourcefile(user_function)
    fp = "dt_" + orig_name
    imports = get_imports(orig_name)
    with open(fp, "a") as helper:
        helper.writelines(imports)
        helper.write("\n")
        helper.write(lines)
        helper.write("\n")

    # import importlib
    # mod = importlib.import_module(fp[:-3])
    # func = getattr(mod, str(user_function.__name__))
    # return func(*args, **kwargs)
    return fp

def get_imports(file_name) -> list:
    imports = []

    with open(file_name, "r") as fayl:
        lines = fayl.read().split("\n")

    for line in lines:
        if (
            line.startswith("from") or line.startswith("import")
        ) and "pycompss.interactive" not in line:
            imports.append(line + "\n")
    return imports

def format_json(jeyson):
    ret = ""
    for k in jeyson:
        ret += k + "=" + str(jeyson[k]) + ", "
    return ret[:-2]


# ########################################################################### #
# ##################### Software DECORATOR ALTERNATIVE NAME ################# #
# ########################################################################### #


conv = SoftConverter  # pylint: disable=invalid-name
