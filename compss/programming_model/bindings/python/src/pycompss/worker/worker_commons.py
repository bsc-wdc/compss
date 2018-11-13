from pycompss.util.serializer import SerializerException
from pycompss.runtime.commons import IS_PYTHON3
import pycompss.api.parameter as parameter

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
    """
    Task execution function.

    :param logger: Logger
    :param process_name: Process name
    :param module: Module which contains the function
    :param method_name: Function to invoke
    :param types: List of the parameter's types
    :param values: List of the parameter's values
    :param compss_kwargs: PyCOMPSs keywords
    :return: new types, new_values, and isModifier
    """

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
