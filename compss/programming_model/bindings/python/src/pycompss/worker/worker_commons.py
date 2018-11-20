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
from pycompss.util.serializer import SerializerException
from pycompss.runtime.commons import IS_PYTHON3
import pycompss.api.parameter as parameter
import sys

def get_input_params(num_params, logger, args, process_name):
    '''Get and prepare the input parameters from string to lists.

    :param num_params: Number of parameters
    :param logger: Logger
    :param args: Arguments (complete list of parameters with type, stream, prefix and value)
    :param process_name: Process name
    :return: A list of TaskParameter objects
    '''
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
            logger.debug("[PYTHON WORKER %s] Parameter : %s" % (process_name, str(i)))
            logger.debug("[PYTHON WORKER %s] \t * Type : %s" % (process_name, str(p_type)))
            logger.debug("[PYTHON WORKER %s] \t * Stream : %s" % (process_name, str(p_stream)))
            logger.debug("[PYTHON WORKER %s] \t * Prefix : %s" % (process_name, str(p_prefix)))
            logger.debug("[PYTHON WORKER %s] \t * Name : %s" % (process_name, str(p_name)))
            logger.debug("[PYTHON WORKER %s] \t * Value: %r" % (process_name, p_value))

        if p_type == parameter.TYPE.FILE:
            # Maybe the file is a object, we dont care about this here
            # We will decide whether to deserialize or to forward the value
            # when processing parameters in the task decorator
            ret.append(
                TaskParameter(
                    type = p_type,
                    stream = p_stream,
                    prefix = p_prefix,
                    name = p_name,
                    file_name = p_value
                )
            )
        elif p_type == parameter.TYPE.EXTERNAL_PSCO:
            ret.append(
                TaskParameter(
                    type = p_type,
                    stream = p_stream,
                    prefix = p_prefix,
                    name = p_name,
                    key = p_value
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
                logger.debug("[PYTHON WORKER %s] \t * Final Value: %s" % (process_name, str(aux)))
            pos += num_substrings

            if IS_PYTHON3 and isinstance(aux, bytes):
                aux = aux.decode('utf-8')

            ret.append(
                TaskParameter(
                    type = p_type,
                    stream = p_stream,
                    prefix = p_prefix,
                    name = p_name,
                    content = aux
                )
            )
        else:
            # Basic numeric types. These are passed as command line arguments and only
            # a cast is needed
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
                    type = p_type,
                    stream = p_stream,
                    prefix = p_prefix,
                    name = p_name,
                    content = val
                )
            )
        pos += 5

    return ret


def task_execution(logger, process_name, module, method_name, types, values, compss_kwargs):
    '''Task execution function.

    :param logger: Logger
    :param process_name: Process name
    :param module: Module which contains the function
    :param method_name: Function to invoke
    :param types: List of the parameter's types
    :param values: List of the parameter's values
    :param compss_kwargs: PyCOMPSs keywords
    :return: new types, new_values, and isModifier
    '''

    if __debug__:
        logger.debug("[PYTHON WORKER %s] Starting task execution" % process_name)
        logger.debug("[PYTHON WORKER %s] Types : %s " % (process_name, str(types)))
        logger.debug("[PYTHON WORKER %s] Values: %s " % (process_name, str(values)))

    # WARNING: the following call will not work if a user decorator overrides the return of the task decorator.
    # new_types, new_values = getattr(module, method_name)(*values, compss_types=types, **compss_kwargs)
    # If the @task is decorated with a user decorator, may include more return values, and consequently,
    # the new_types and new_values will be within a tuple at position 0.
    # Force users that use decorators on top of @task to return the task results first.
    # This is tested with the timeit decorator in test 19.
    task_output = getattr(module, method_name)(*values, compss_types = types, **compss_kwargs)

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
        logger.debug("[PYTHON WORKER %s] Return Types : %s " % (process_name, str(new_types)))
        logger.debug("[PYTHON WORKER %s] Return Values: %s " % (process_name, str(new_values)))
        logger.debug("[PYTHON WORKER %s] Return isModifier: %s " % (process_name, str(is_modifier)))
        logger.debug("[PYTHON WORKER %s] Finished task execution" % process_name)

    return new_types, new_values, is_modifier


def execute_task(process_name, storage_conf, params, TRACING):
    """
    ExecuteTask main method.

    :param process_name: Process name
    :param storage_conf: Storage configuration file path
    :param params: List of parameters
    :return: exit code, new types and new values
    """
    import logging
    logger = logging.getLogger('pycompss.worker.worker')

    if __debug__:
        logger.debug("[PYTHON WORKER %s] Begin task execution" % process_name)

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
        'compss_tracing': TRACING,
        'compss_process_name': process_name,
        'compss_storage_conf': storage_conf,
        'compss_return_length': return_length
    }

    if __debug__:
        logger.debug("[PYTHON WORKER %s] Storage conf: %s" % (str(process_name), str(storage_conf)))
        logger.debug("[PYTHON WORKER %s] Params: %s" % (str(process_name), str(params)))
        logger.debug("[PYTHON WORKER %s] Path: %s" % (str(process_name), str(path)))
        logger.debug("[PYTHON WORKER %s] Method name: %s" % (str(process_name), str(method_name)))
        logger.debug("[PYTHON WORKER %s] Num slaves: %s" % (str(process_name), str(num_slaves)))
        logger.debug("[PYTHON WORKER %s] Slaves: %s" % (str(process_name), str(slaves)))
        logger.debug("[PYTHON WORKER %s] Cus: %s" % (str(process_name), str(cus)))
        logger.debug("[PYTHON WORKER %s] Has target: %s" % (str(process_name), str(has_target)))
        logger.debug("[PYTHON WORKER %s] Num Params: %s" % (str(process_name), str(num_params)))
        logger.debug("[PYTHON WORKER %s] Return Length: %s" % (str(process_name), str(return_length)))
        logger.debug("[PYTHON WORKER %s] Args: %r" % (str(process_name), args))

    # Get all parameter values
    if __debug__:
        logger.debug("[PYTHON WORKER %s] Processing parameters:" % process_name)
    from pycompss.worker.worker_commons import get_input_params
    values  = get_input_params(num_params, logger, args, process_name)
    types = [x.type for x in values]

    if __debug__:
        logger.debug("[PYTHON WORKER %s] RUN TASK with arguments: " % process_name)
        logger.debug("[PYTHON WORKER %s] \t- Path: %s" % (process_name, path))
        logger.debug("[PYTHON WORKER %s] \t- Method/function name: %s" % (process_name, method_name))
        logger.debug("[PYTHON WORKER %s] \t- Has target: %s" % (process_name, str(has_target)))
        logger.debug("[PYTHON WORKER %s] \t- # parameters: %s" % (process_name, str(num_params)))
        logger.debug("[PYTHON WORKER %s] \t- Values:" % process_name)
        for v in values:
            logger.debug("[PYTHON WORKER %s] \t\t %r" % (process_name, v))
        logger.debug("[PYTHON WORKER %s] \t- COMPSs types:" % process_name)
        for t in types:
            logger.debug("[PYTHON WORKER %s] \t\t %s" % (process_name, str(t)))

    import_error = False

    new_types = []
    new_values = []

    try:
        # Try to import the module (for functions)
        if __debug__:
            logger.debug("[PYTHON WORKER %s] Trying to import the user module: %s" % (process_name, path))
        if sys.version_info >= (2, 7):
            import importlib
            module = importlib.import_module(path)  # Python 2.7
            if __debug__:
                logger.debug("[PYTHON WORKER %s] Module successfully loaded (Python version >= 2.7)" % process_name)
        else:
            module = __import__(path, globals(), locals(), [path], -1)
            if __debug__:
                logger.debug("[PYTHON WORKER %s] Module successfully loaded (Python version < 2.7" % process_name)

        def task_execution_1():
            from pycompss.worker.worker_commons import task_execution
            return task_execution(logger, process_name, module, method_name, types, values, compss_kwargs)

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
        logger.exception("[PYTHON WORKER %s] WORKER EXCEPTION - Attribute Error Exception" % process_name)
        logger.exception(''.join(line for line in lines))
        logger.exception("[PYTHON WORKER %s] Check that all parameters have been defined with an absolute import path (even if in the same file)" % process_name)
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
        logger.exception("[PYTHON WORKER %s] WORKER EXCEPTION" % process_name)
        logger.exception(''.join(line for line in lines))
        # If exception is raised during the task execution, new_types and new_values are empty
        return 1, new_types, new_values

    if import_error:
        if __debug__:
            logger.debug("[PYTHON WORKER %s] Could not import the module. Reason: Method in class." % process_name)

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
            logger.debug("[PYTHON WORKER %s] Method in class %s of module %s" % (process_name, class_name, module_name))

        logger.debug('HAS TARGET IS %s' % str(has_target))

        if has_target == 'true':
            # Instance method
            # The self object needs to be an object in order to call the function.
            # Consequently, it can not be done in the @task decorator.
            last_elem = values.pop()
            logger.debug('LAST ELEM ###')
            logger.debug(last_elem.name)
            if last_elem.key is None:
                file_name = last_elem.file_name.split(':')[-1]
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Deserialize self from file." % process_name)
                from pycompss.util.serializer import deserialize_from_file
                obj = deserialize_from_file(file_name)
                logger.debug('DESERIALIZED OBJECT IS %s' % last_elem.content)
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Processing callee, a hidden object of %s in file %s" % (
                        process_name, file_name, type(last_elem.content)))
            values.insert(0, obj)
            types.pop()
            from pycompss.util.persistent_storage import is_psco, get_by_id
            types.insert(0, parameter.TYPE.OBJECT if not is_psco(last_elem.content) else parameter.TYPE.EXTERNAL_PSCO)

            def task_execution_2():
                from pycompss.worker.worker_commons import task_execution
                return task_execution(logger, process_name, klass, method_name, types, values, compss_kwargs)

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
                logger.exception("[PYTHON WORKER %s] WORKER EXCEPTION" % process_name)
                logger.exception(''.join(line for line in lines))
                # If exception is raised during the task execution, new_types and new_values are empty
                return 1, new_types, new_values

            # Depending on the isModifier option, it is necessary to serialize again self or not.
            # Since this option is only visible within the task decorator, the task_execution returns
            # the value of isModifier in order to know here if self has to be serialized.
            # This solution avoids to use inspect.
            if is_modifier:
                from pycompss.util.persistent_storage import is_psco, get_by_id
                if is_psco(last_elem):
                    # There is no update PSCO on the storage API. Consequently, the changes on the PSCO must have been
                    # pushed into the storage automatically on each PSCO modification.
                    if True or __debug__:
                        # TODO: this may not be correct if the user specifies isModifier=False.
                        logger.debug("[PYTHON WORKER %s] The changes on the PSCO must have been automatically updated by the storage." % process_name)
                    pass
                else:
                    if True or __debug__:
                        logger.debug("[PYTHON WORKER %s] Serializing self to file: %s" % (process_name, file_name))
                    from pycompss.util.serializer import serialize_to_file
                    serialize_to_file(obj, file_name)
                if True or __debug__:
                    logger.debug("[PYTHON WORKER %s] Serializing self to file." % process_name)
                from pycompss.util.serializer import serialize_to_file
                serialize_to_file(obj, file_name)
            if True or __debug__:
                logger.debug("[PYTHON WORKER %s] Obj: %r" % (process_name, obj))
        else:
            # Class method - class is not included in values (e.g. values = [7])
            types.append(None)  # class must be first type

            def task_execution_3():
                from pycompss.worker.worker_commons import task_execution
                return task_execution(logger, process_name, klass, method_name, types, values, compss_kwargs)

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
                logger.exception("[PYTHON WORKER %s] WORKER EXCEPTION" % process_name)
                logger.exception(''.join(line for line in lines))
                # If exception is raised during the task execution, new_types and new_values are empty
                return 1, new_types, new_values

    # EVERYTHING OK
    if __debug__:
        logger.debug("[PYTHON WORKER %s] End task execution. Status: Ok" % process_name)

    return 0, new_types, new_values  # Exit code, updated params
