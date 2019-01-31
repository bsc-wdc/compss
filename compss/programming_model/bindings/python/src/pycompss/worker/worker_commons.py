#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import sys

from pycompss.util.serializer import SerializerException
from pycompss.runtime.commons import IS_PYTHON3
import pycompss.api.parameter as parameter


def get_input_params(num_params, logger, args):
    """
    Get and prepare the input parameters from string to lists.

    :param num_params: Number of parameters
    :param logger: Logger
    :param args: Arguments (complete list of parameters with type, stream, prefix and value)
    :return: A list of TaskParameter objects
    """
    from pycompss.api.parameter import TaskParameter
    pos = 0

    ret = []

    for i in range(0, num_params):
        p_type = int(args[pos])
        p_stream = int(args[pos + 1])
        p_prefix = args[pos + 2]
        p_name = args[pos + 3]
        p_value = args[pos + 4]

        if __debug__:
            logger.debug("Parameter : %s" % str(i))
            logger.debug("\t * Type : %s" % str(p_type))
            logger.debug("\t * Stream : %s" % str(p_stream))
            logger.debug("\t * Prefix : %s" % str(p_prefix))
            logger.debug("\t * Name : %s" % str(p_name))
            logger.debug("\t * Value: %r" % p_value)

        if p_type == parameter.TYPE.FILE:
            # Maybe the file is a object, we dont care about this here
            # We will decide whether to deserialize or to forward the value
            # when processing parameters in the task decorator
            ret.append(
                TaskParameter(
                    p_type=p_type,
                    stream=p_stream,
                    prefix=p_prefix,
                    name=p_name,
                    file_name=p_value
                )
            )
        elif p_type == parameter.TYPE.EXTERNAL_PSCO:
            ret.append(
                TaskParameter(
                    p_type=p_type,
                    stream=p_stream,
                    prefix=p_prefix,
                    name=p_name,
                    key=p_value
                )
            )
            pos += 1  # Skip info about direction (R, W)
        elif p_type == parameter.TYPE.STRING:
            num_substrings = int(p_value)
            aux = ''
            first_substring = True
            for j in range(5, num_substrings + 5):
                if not first_substring:
                    aux += ' '
                first_substring = False
                aux += args[pos + j]
            # Decode the received string
            # Note that we prepend a sharp to all strings in order to avoid
            # getting empty encodings in the case of empty strings, so we need
            # to remove it when decoding
            import base64
            aux = base64.b64decode(aux.encode())[1:]
            if aux:
                #######
                # Check if the string is really an object
                # Required in order to recover objects passed as parameters.
                # - Option object_conversion
                real_value = aux
                try:
                    from pycompss.util.serializer import deserialize_from_string
                    from pycompss.runtime.commons import STR_ESCAPE
                    # try to recover the real object
                    if IS_PYTHON3:
                        # decode removes double backslash, and encode returns as binary
                        aux = deserialize_from_string(aux.decode(STR_ESCAPE).encode())
                    else:
                        # decode removes double backslash, and str casts the output
                        aux = deserialize_from_string(str(aux.decode(STR_ESCAPE)))
                except (SerializerException, ValueError, EOFError):
                    # was not an object
                    aux = str(real_value.decode())
                    #######
            if __debug__:
                logger.debug("\t * Final Value: %s" % str(aux))
            pos += num_substrings

            if IS_PYTHON3 and isinstance(aux, bytes):
                aux = aux.decode('utf-8')

            ret.append(
                TaskParameter(
                    p_type=p_type,
                    stream=p_stream,
                    prefix=p_prefix,
                    name=p_name,
                    content=aux
                )
            )
        else:
            # Basic numeric types. These are passed as command line arguments and only
            # a cast is needed
            val = None
            if p_type == parameter.TYPE.INT:
                val = int(p_value)
            elif p_type == parameter.TYPE.LONG:
                val = parameter.PYCOMPSS_LONG(p_value)
                if val > parameter.JAVA_MAX_INT or val < parameter.JAVA_MIN_INT:
                    # A Python inparameter.t was converted to a Java long to prevent overflow
                    # We are sure we will not overflow Python int, otherwise this
                    # would have been passed as a serialized object.
                    val = int(val)
            elif p_type == parameter.TYPE.DOUBLE:
                val = float(p_value)
            elif p_type == parameter.TYPE.BOOLEAN:
                val = (p_value == 'true')
            ret.append(
                TaskParameter(
                    p_type=p_type,
                    stream=p_stream,
                    prefix=p_prefix,
                    name=p_name,
                    content=val
                )
            )
        pos += 5

    return ret


def task_execution(logger, module, method_name, types, values, compss_kwargs):
    """
    Task execution function.

    :param logger: Logger
    :param module: Module which contains the function
    :param method_name: Function to invoke
    :param types: List of the parameter's types
    :param values: List of the parameter's values
    :param compss_kwargs: PyCOMPSs keywords
    :return: new types, new_values, and isModifier
    """

    if __debug__:
        logger.debug("Starting task execution")
        logger.debug("Types : %s " % str(types))
        logger.debug("Values: %s " % str(values))

    # WARNING: the following call will not work if a user decorator overrides the return of the task decorator.
    # new_types, new_values = getattr(module, method_name)(*values, compss_types=types, **compss_kwargs)
    # If the @task is decorated with a user decorator, may include more return values, and consequently,
    # the new_types and new_values will be within a tuple at position 0.
    # Force users that use decorators on top of @task to return the task results first.
    # This is tested with the timeit decorator in test 19.

    task_output = getattr(module, method_name)(*values, compss_types=types, **compss_kwargs)

    if isinstance(task_output[0], tuple):  # Weak but effective way to check it without doing inspect.
        # Another decorator has added another return thing.
        # TODO: Should we consider here to create a list with all elements and serialize it to a file with the real task output plus the decorator results? == task_output[1:]
        # TODO: Currently, the extra result is ignored.
        new_types = task_output[0][0]
        new_values = task_output[0][1]
        is_modifier = task_output[0][2]
    else:
        # The task_output is composed by the new_types and new_values returned by the task decorator.
        new_types = task_output[0]
        new_values = task_output[1]
        is_modifier = task_output[2]

    if __debug__:
        # The types may change (e.g. if the user does a makePersistent within the task)
        logger.debug("Return Types : %s " % str(new_types))
        logger.debug("Return Values: %s " % str(new_values))
        logger.debug("Return isModifier: %s " % str(is_modifier))
        logger.debug("Finished task execution")

    return new_types, new_values, is_modifier


def execute_task(process_name, storage_conf, params, tracing, logger):
    """
    ExecuteTask main method.

    :param process_name: Process name
    :param storage_conf: Storage configuration file path
    :param params: List of parameters
    :param tracing: Tracing flag
    :param logger: Logger to use
    :return: exit code, new types and new values
    """

    if __debug__:
        logger.debug("Begin task execution in %s" % process_name)

    persistent_storage = False
    if storage_conf != 'null':
        persistent_storage = True
        from pycompss.util.persistent_storage import storage_task_context

    # Retrieve the parameters from the params argument
    path = params[0]
    method_name = params[1]
    num_slaves = int(params[2])
    slaves = []
    for i in range(2, 2 + num_slaves):
        slaves.append(params[i])
    arg_position = 3 + num_slaves

    args = params[arg_position:]
    cus = args[0]

    args = args[1:]
    has_target = args[0]
    return_type = args[1]
    return_length = int(args[2])
    num_params = int(args[3])

    args = args[4:]

    # COMPSs keywords for tasks (ie: tracing, process name...)
    compss_kwargs = {
        'compss_tracing': tracing,
        'compss_process_name': process_name,
        'compss_storage_conf': storage_conf,
        'compss_return_length': return_length
    }

    if __debug__:
        logger.debug("Storage conf: %s" % str(storage_conf))
        logger.debug("Params: %s" % str(params))
        logger.debug("Path: %s" % str(path))
        logger.debug("Method name: %s" % str(method_name))
        logger.debug("Num slaves: %s" % str(num_slaves))
        logger.debug("Slaves: %s" % str(slaves))
        logger.debug("Cus: %s" % str(cus))
        logger.debug("Has target: %s" % str(has_target))
        logger.debug("Num Params: %s" % str(num_params))
        logger.debug("Return Length: %s" % str(return_length))
        logger.debug("Args: %r" % args)

    # Get all parameter values
    if __debug__:
        logger.debug("Processing parameters:")
    from pycompss.worker.worker_commons import get_input_params
    values = get_input_params(num_params, logger, args)
    types = [x.type for x in values]

    if __debug__:
        logger.debug("RUN TASK with arguments:")
        logger.debug("\t- Path: %s" % path)
        logger.debug("\t- Method/function name: %s" % method_name)
        logger.debug("\t- Has target: %s" % str(has_target))
        logger.debug("\t- # parameters: %s" % str(num_params))
        logger.debug("\t- Values:")
        for v in values:
            logger.debug("\t\t %r" % v)
        logger.debug("\t- COMPSs types:")
        for t in types:
            logger.debug("\t\t %s" % str(t))

    import_error = False

    new_types = []
    new_values = []

    try:
        # Try to import the module (for functions)
        if __debug__:
            logger.debug("Trying to import the user module: %s" % path)
        if sys.version_info >= (2, 7):
            import importlib
            module = importlib.import_module(path)  # Python 2.7
            if __debug__:
                logger.debug("Module successfully loaded (Python version >= 2.7)")
        else:
            module = __import__(path, globals(), locals(), [path], -1)
            if __debug__:
                logger.debug("Module successfully loaded (Python version < 2.7")

        def task_execution_1():
            from pycompss.worker.worker_commons import task_execution
            return task_execution(logger, module, method_name, types, values, compss_kwargs)

        if persistent_storage:
            with storage_task_context(logger, values, config_file_path=storage_conf):
                new_types, new_values, is_modifier = task_execution_1()
        else:
            new_types, new_values, is_modifier = task_execution_1()

    # ==========================================================================
    except AttributeError:
        # Appears with functions that have not been well defined.
        exc_type, exc_value, exc_traceback = sys.exc_info()
        import traceback
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("WORKER EXCEPTION IN %s - Attribute Error Exception" % process_name)
        logger.exception(''.join(line for line in lines))
        logger.exception("Check that all parameters have been defined with an absolute import path (even if in the same file)")
        # If exception is raised during the task execution, new_types and
        # new_values are empty
        return 1, new_types, new_values
    # ==========================================================================
    except ImportError:
        import_error = True
    # ==========================================================================
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        import traceback
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("WORKER EXCEPTION IN %s" % process_name)
        logger.exception(''.join(line for line in lines))
        # If exception is raised during the task execution, new_types and new_values are empty
        return 1, new_types, new_values

    if import_error:
        if __debug__:
            logger.debug("Could not import the module. Reason: Method in class.")

        # Not the path of a module, it ends with a class name
        class_name = path.split('.')[-1]
        module_name = '.'.join(path.split('.')[0:-1])

        if '.' in path:
            module_name = '.'.join(path.split('.')[0:-1])
        else:
            module_name = path
        module = __import__(module_name, fromlist=[class_name])
        klass = getattr(module, class_name)

        if __debug__:
            logger.debug("Method in class %s of module %s" % (class_name, module_name))
            logger.debug("Has target: %s" % str(has_target))

        if has_target == 'true':
            # Instance method
            # The self object needs to be an object in order to call the function.
            # Consequently, it can not be done in the @task decorator.
            # Since the args structure is parameters + self + returns we pop the corresponding considering the
            # return_length notified by the runtime (-1 due to index starts from 0).
            self_index = num_params - return_length - 1
            self_elem = values.pop(self_index)
            self_type = types.pop(self_index)
            if self_type == parameter.TYPE.EXTERNAL_PSCO:
                if __debug__:
                    logger.debug("Last element (self) is a PSCO with id: %s" % str(self_elem.key))
                from pycompss.util.persistent_storage import get_by_id
                obj = get_by_id(self_elem.key)
            else:
                obj = None
                file_name = None
                if self_elem.key is None:
                    file_name = self_elem.file_name.split(':')[-1]
                    if __debug__:
                        logger.debug("Deserialize self from file.")
                    from pycompss.util.serializer import deserialize_from_file
                    obj = deserialize_from_file(file_name)
                    logger.debug('DESERIALIZED OBJECT IS %s' % self_elem.content)
                    if __debug__:
                        logger.debug("Processing callee, a hidden object of %s in file %s" % (file_name, type(self_elem.content)))
            values.insert(0, obj)
            types.insert(0, parameter.TYPE.OBJECT if not self_type == parameter.TYPE.EXTERNAL_PSCO else parameter.TYPE.EXTERNAL_PSCO)

            def task_execution_2():
                from pycompss.worker.worker_commons import task_execution
                return task_execution(logger, klass, method_name, types, values, compss_kwargs)

            try:
                if persistent_storage:
                    with storage_task_context(logger, values, config_file_path=storage_conf):
                        new_types, new_values, is_modifier = task_execution_2()
                else:
                    new_types, new_values, is_modifier = task_execution_2()
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                import traceback
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.exception("WORKER EXCEPTION IN %s" % process_name)
                logger.exception(''.join(line for line in lines))
                # If exception is raised during the task execution, new_types and new_values are empty
                return 1, new_types, new_values

            # Depending on the isModifier option, it is necessary to serialize again self or not.
            # Since this option is only visible within the task decorator, the task_execution returns
            # the value of isModifier in order to know here if self has to be serialized.
            # This solution avoids to use inspect.
            if is_modifier:
                from pycompss.util.persistent_storage import is_psco
                if is_psco(self_elem):
                    # There is no explicit update if self is a PSCO.
                    # Consequently, the changes on the PSCO must have been pushed into the storage automatically
                    # on each PSCO modification.
                    # This may lead to errors if isModifier=False, because it will not happen. The changes will
                    # be made, and the storage must deal with concurrent modifications.
                    if __debug__:
                        logger.debug("The changes on the PSCO must have been automatically updated by the storage.")
                    pass
                else:
                    if __debug__:
                        logger.debug("Serializing self to file: %s" % file_name)
                    from pycompss.util.serializer import serialize_to_file
                    serialize_to_file(obj, file_name)
                if __debug__:
                    logger.debug("Serializing self to file.")
                from pycompss.util.serializer import serialize_to_file
                serialize_to_file(obj, file_name)
                if __debug__:
                    logger.debug("Obj: %r" % obj)
        else:
            # Class method - class is not included in values (e.g. values = [7])
            types.append(None)  # class must be first type

            def task_execution_3():
                from pycompss.worker.worker_commons import task_execution
                return task_execution(logger, klass, method_name, types, values, compss_kwargs)

            try:
                if persistent_storage:
                    with storage_task_context(logger, values, config_file_path=storage_conf):
                        new_types, new_values, is_modifier = task_execution_3()
                else:
                    new_types, new_values, is_modifier = task_execution_3()
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                import traceback
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.exception("WORKER EXCEPTION IN %s" % process_name)
                logger.exception(''.join(line for line in lines))
                # If exception is raised during the task execution, new_types and new_values are empty
                return 1, new_types, new_values

    # EVERYTHING OK
    if __debug__:
        logger.debug("End task execution. Status: Ok")

    return 0, new_types, new_values  # Exit code, updated params
