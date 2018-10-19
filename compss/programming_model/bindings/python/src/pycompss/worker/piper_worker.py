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

# -*- coding: utf-8 -*-

"""
PyCOMPSs Persistent Worker
===========================
    This file contains the worker code.
"""

import logging
import os
import signal
import sys
import traceback
from multiprocessing import Process
from multiprocessing import Queue
import base64
import thread_affinity
import pycompss.api.parameter as parameter
from pycompss.runtime.commons import EMPTY_STRING_KEY
from pycompss.runtime.commons import STR_ESCAPE
from pycompss.runtime.commons import IS_PYTHON3
from pycompss.util.location import set_pycompss_context
from pycompss.util.serializer import serialize_to_file
from pycompss.util.serializer import deserialize_from_file
from pycompss.util.serializer import deserialize_from_string
from pycompss.util.serializer import SerializerException
from pycompss.util.logs import init_logging_worker
from pycompss.util.persistent_storage import is_psco, get_by_id

SYNC_EVENTS = 8000666

# Should be equal to Tracer.java definitions (but only worker running all other are trace through
# with function-list
TASK_EVENTS = 60000100
WORKER_RUNNING = 102

# Persistent worker global variables
TRACING = False
PROCESSES = []

# if sys.version_info >= (2, 7):
#     import importlib


#####################
#  Tag variables
#####################
INIT = "init"  # -- worker.py debug tracing #thr pipes_CMD pipes_RESULT
EXECUTE_TASK_TAG = "task"  # -- "task" taskId jobOut jobErr task_params
END_TASK_TAG = "endTask"  # -- "endTask" taskId endStatus
QUIT_TAG = "quit"  # -- "quit"


######################
#  Processes body
######################
def worker(queue, process_name, input_pipe, output_pipe, storage_conf):
    """
    Thread main body - Overrides Threading run method.
    Iterates over the input pipe in order to receive tasks (with their
    parameters) and process them.
    Notifies the runtime when each task  has finished with the
    corresponding output value.
    Finishes when the "quit" message is received.

    :param queue: Queue where to put exception messages
    :param process_name: Process name (Thread-X, where X is the thread id).
    :param input_pipe: Input pipe for the thread. To receive messages from the runtime.
    :param output_pipe: Output pipe for the thread. To send messages to the runtime.
    :param storage_conf: Storage configuration file
    :return: None
    """

    logger = logging.getLogger('pycompss.worker.worker')
    handler = logger.handlers[0]
    level = logger.getEffectiveLevel()
    formatter = logging.Formatter(handler.formatter._fmt)

    if storage_conf != 'null':
        try:
            from storage.api import initWorkerPostFork as initStorageAtWorkerPostFork
            initStorageAtWorkerPostFork()
        except ImportError:
            if __debug__:
                logger.info("[PYTHON WORKER] Could not find initWorkerPostFork storage call. Ignoring it.")

    alive = True
    stdout = sys.stdout
    stderr = sys.stderr

    if __debug__:
        logger.debug("[PYTHON WORKER] Starting process " + str(process_name))

    while alive:
        in_pipe = open(input_pipe, 'r')  # , 0) # 0 just for python 2

        affinity_ok = True

        def process_task(current_line, pipe):
            if __debug__:
                logger.debug("[PYTHON WORKER] Received message: %s" % str(current_line))
            current_line = current_line.split()
            pipe.close()
            if current_line[0] == EXECUTE_TASK_TAG:
                # CPU binding
                binded_cpus = current_line[-3]

                def bind_cpus(binded_cpus):
                    if binded_cpus != "-":
                        os.environ['COMPSS_BINDED_CPUS'] = binded_cpus
                        if __debug__:
                            logger.debug("[PYTHON WORKER] Assigning affinity %s" % str(binded_cpus))
                        binded_cpus = list(map(int, binded_cpus.split(",")))
                        try:
                            thread_affinity.setaffinity(binded_cpus)
                        except Exception:
                            if __debug__:
                                logger.error("[PYTHON WORKER] Warning: could not assign affinity %s" % str(binded_cpus))
                            affinity_ok = False

                bind_cpus(binded_cpus)

                # GPU binding
                binded_gpus = current_line[-2]

                def bind_gpus(current_binded_gpus):
                    if current_binded_gpus != "-":
                        os.environ['COMPSS_BINDED_GPUS'] = current_binded_gpus
                        os.environ['CUDA_VISIBLE_DEVICES'] = current_binded_gpus
                        os.environ['GPU_DEVICE_ORDINAL'] = current_binded_gpus

                bind_gpus(binded_gpus)

                # Host list
                host_list = line[-1]

                def treat_host_list(host_list):
                    os.environ['COMPSS_HOSTNAMES'] = host_list
                treat_host_list(host_list)

                # Remove the last elements: cpu and gpu bindings
                current_line = current_line[0:-3]

                # task jobId command
                job_id = current_line[1]
                job_out = current_line[2]
                job_err = current_line[3]
                # line[4] = <boolean> = tracing
                # line[5] = <integer> = task id
                # line[6] = <boolean> = debug
                # line[7] = <string>  = storage conf.
                # line[8] = <string>  = operation type (e.g. METHOD)
                # line[9] = <string>  = module
                # line[10]= <string>  = method
                # line[11]= <integer> = Number of slaves (worker nodes) == #nodes
                # <<list of slave nodes>>
                # line[11 + #nodes] = <integer> = computing units
                # line[12 + #nodes] = <boolean> = has target
                # line[13 + #nodes] = <string>  = has return (always 'null')
                # line[14 + #nodes] = <integer> = Number of parameters
                # <<list of parameters>>
                #       !---> type, stream, prefix , value

                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Received task." % str(process_name))
                    logger.debug("[PYTHON WORKER %s] - TASK CMD: %s" % (str(process_name), str(current_line)))

                # Swap logger from stream handler to file handler.   #### TODO: FIX LOGGER! it may not be the first if the user defines its own.
                logger.removeHandler(logger.handlers[0])
                out_file_handler = logging.FileHandler(job_out)
                out_file_handler.setLevel(level)
                out_file_handler.setFormatter(formatter)
                logger.addHandler(out_file_handler)
                err_file_handler = logging.FileHandler(job_err)
                err_file_handler.setLevel(logging.ERROR)
                err_file_handler.setFormatter(formatter)
                logger.addHandler(err_file_handler)

                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Received task." % str(process_name))
                    logger.debug("[PYTHON WORKER %s] - TASK CMD: %s" % (str(process_name), str(current_line)))

                try:
                    out = open(job_out, 'a')
                    err = open(job_err, 'a')
                    sys.stdout = out
                    sys.stderr = err
                    if not affinity_ok:
                        err.write('WARNING: This task is going to be executed with default thread affinity %s' % thread_affinity.getaffinity())
                    exit_value, new_types, new_values = execute_task(process_name, storage_conf, current_line[9:])
                    sys.stdout = stdout
                    sys.stderr = stderr
                    sys.stdout.flush()
                    sys.stderr.flush()
                    out.close()
                    err.close()

                    if exit_value == 0:
                        # Task has finished without exceptions
                        # endTask jobId exitValue message
                        params = build_return_params_message(current_line[9:], new_types, new_values)
                        message = END_TASK_TAG + " " + str(job_id) \
                                  + " " + str(exit_value) \
                                  + " " + str(params) + "\n"
                    else:
                        # An exception has been raised in task
                        message = END_TASK_TAG + " " + str(job_id) + " " + str(exit_value) + "\n"

                    if __debug__:
                        logger.debug("[PYTHON WORKER %s] - Pipe %s END TASK MESSAGE: %s" % (str(process_name),
                                                                                            str(output_pipe),
                                                                                            str(message)))
                    # The return message is:
                    #
                    # TaskResult ==> jobId exitValue List<Object>
                    #
                    # Where List<Object> has D length:
                    # D = #parameters + (has_target ? 1 : 0) + (has_return ? 1 : 0)
                    # And contains:
                    # - Null if it NOT a PSCO
                    # - PSCOId (String) if is a PSCO
                    #
                    # This is sent through the pipe with the endTask message.
                    # If the task had an object or file as parameter and the worker returns the id,
                    # the runtime can change the type (and locations) to a EXTERNAL_OBJ_T.

                    with open(output_pipe, 'w') as out_pipe:
                        out_pipe.write(message)
                except Exception as e:
                    logger.exception("[PYTHON WORKER %s] Exception %s" % (str(process_name), str(e)))
                    queue.put("EXCEPTION")

                if binded_cpus != "-":
                    del os.environ['COMPSS_BINDED_CPUS']
                if binded_gpus != "-":
                    del os.environ['COMPSS_BINDED_GPUS']
                    del os.environ['CUDA_VISIBLE_DEVICES']
                    del os.environ['GPU_DEVICE_ORDINAL']
                del os.environ['COMPSS_HOSTNAMES']
                # Restore logger
                logger.removeHandler(out_file_handler)
                logger.removeHandler(err_file_handler)
                logger.addHandler(handler)

            elif current_line[0] == QUIT_TAG:
                # Received quit message -> Suicide
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Received quit." % str(process_name))
                return False
            return True

        for line in in_pipe:
            if line != "":
                alive = process_task(line, in_pipe)
                break

    if storage_conf != 'null':
        try:
            from storage.api import finishWorkerPostFork as finishStorageAtWorkerPostFork
            finishStorageAtWorkerPostFork()
        except ImportError:
            if __debug__:
                logger.info("[PYTHON WORKER] Could not find finishWorkerPostFork storage call. Ignoring it.")

    sys.stdout.flush()
    sys.stderr.flush()
    print("[PYTHON WORKER] Exiting process ", process_name)


def build_return_params_message(params, types, values):
    """
    Build the return message with the parameters output.

    :param params: List of parameters
    :param types: List of the parameter's types
    :param values: List of the parameter's values
    :return: Message as string
    """

    assert len(types) == len(values), 'Inconsistent state: return type-value length mismatch for return message.'

    # Analyse the input parameters to get has_target and has_return
    # More information that requested can be gathered and returned in the return message if necessary.
    num_slaves = int(params[2])
    arg_position = 3 + num_slaves
    args = params[arg_position + 1:]
    has_target = args[0]
    if has_target == 'false':
        has_target = False
    else:
        has_target = True
    return_type = args[1]
    if return_type == 'null':
        has_return = False
    else:
        has_return = True

    pairs = list(zip(types, values))
    num_params = len(pairs)
    params = ''
    for p in pairs:
        params = params + str(p[0]) + ' ' + str(p[1]) + ' '
    total_params = num_params + (1 if has_target else 0) + (1 if has_return else 0)
    message = str(total_params) + ' ' + params
    return message


#####################################
# Execute Task Method - Task handler
#####################################
def execute_task(process_name, storage_conf, params):
    """
    ExecuteTask main method.

    :param process_name: Process name
    :param storage_conf: Storage configuration file path
    :param params: List of parameters
    :return: exit code, new types and new values
    """

    logger = logging.getLogger('pycompss.worker.worker')

    if __debug__:
        logger.debug("[PYTHON WORKER %s] Begin task execution" % process_name)

    persistent_storage = False
    if storage_conf != 'null':
        persistent_storage = True
        from pycompss.util.persistent_storage import storage_task_context

    print('---- TASK PARAMS ----')
    print(params)
    print('---- END TASK PARAMS ----')

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

        if has_target == 'true':
            # Instance method
            # The self object needs to be an object in order to call the function.
            # Consequently, it can not be done in the @task decorator.
            last_elem = values.pop()
            if is_psco(last_elem):
                obj = last_elem
            else:
                file_name = last_elem.split(':')[-1]
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Deserialize self from file." % process_name)
                obj = deserialize_from_file(file_name)
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Processing callee, a hidden object of %s in file %s" % (
                        process_name, file_name, type(obj)))
            values.insert(0, obj)
            types.pop()
            types.insert(0, parameter.TYPE.OBJECT if not is_psco(last_elem) else parameter.TYPE.EXTERNAL_PSCO)

            def task_execution_2():
                return task_execution(logger, process_name, klass, method_name, types, values, compss_kwargs)

            try:
                if persistent_storage:
                    with storage_task_context(logger, values, config_file_path=storage_conf):
                        new_types, new_values, is_modifier = task_execution_2()
                else:
                    new_types, new_values, is_modifier = task_execution_2()
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
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
                if is_psco(last_elem):
                    # There is no update PSCO on the storage API. Consequently, the changes on the PSCO must have been
                    # pushed into the storage automatically on each PSCO modification.
                    if __debug__:
                        # TODO: this may not be correct if the user specifies isModifier=False.
                        logger.debug("[PYTHON WORKER %s] The changes on the PSCO must have been automatically updated by the storage." % process_name)
                    pass
                else:
                    if __debug__:
                        logger.debug("[PYTHON WORKER %s] Serializing self to file: %s" % (process_name, file_name))
                    serialize_to_file(obj, file_name)
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Serializing self to file." % process_name)
                serialize_to_file('self', obj, file_name)
            if __debug__:
                logger.debug("[PYTHON WORKER %s] Obj: %r" % (process_name, obj))
        else:
            # Class method - class is not included in values (e.g. values = [7])
            types.insert(0, None)  # class must be first type

            def task_execution_3():
                return task_execution(logger, process_name, klass, method_name, types, values, compss_kwargs)

            try:
                if persistent_storage:
                    with storage_task_context(logger, values, config_file_path=storage_conf):
                        new_types, new_values, is_modifier = task_execution_3()
                else:
                    new_types, new_values, is_modifier = task_execution_3()
            except Exception:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.exception("[PYTHON WORKER %s] WORKER EXCEPTION" % process_name)
                logger.exception(''.join(line for line in lines))
                # If exception is raised during the task execution, new_types and new_values are empty
                return 1, new_types, new_values

    # EVERYTHING OK
    if __debug__:
        logger.debug("[PYTHON WORKER %s] End task execution. Status: Ok" % process_name)

    return 0, new_types, new_values  # Exit code, updated params



def get_input_params(num_params, logger, args, process_name):
    """
    Get and prepare the input parameters from string to lists.

    :param num_params: Number of parameters
    :param logger: Logger
    :param args: Arguments (complete list of parameters with type, stream, prefix and value)
    :param process_name: Process name
    :return: A list of TaskParameter objects
    """
    pos = 0

    class TaskParameter(object):
        '''An internal wrapper for parameters. It makes it easier for the task decorator to know
        any aspect of the parameters (should they be updated or can changes be discarded, should they
        be deserialized or read from some storage, etc etc)
        '''

        def __init__(self, name = None, type = None, file_name = None,
                     key = None, content = None, stream = None, prefix = None):
            self.name = name
            self.type = type
            self.file_name = file_name
            self.key = key
            self.content = content
            self.stream = stream
            self.prefix = prefix

        def __repr__(self):
            return'\nParameter %s' % self.name + '\n' + \
                  '\tType %s' % str(self.type) + '\n' + \
                  '\tFile Name %s' % self.file_name + '\n' + \
                  '\tKey %s' % str(self.key) + '\n' + \
                  '\tContent %s' % str(self.content) + '\n' + \
                  '\tStream %s' % str(self.stream) + '\n' + \
                  '\tPrefix %s' % str(self.prefix) + '\n' + \
                  '-' * 20 + '\n'


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
            aux = base64.b64decode(aux.encode())[1:]
            if aux:
                #######
                # Check if the string is really an object
                # Required in order to recover objects passed as parameters.
                # - Option object_conversion
                real_value = aux
                try:
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


def shutdown_handler(signal, frame):
    """
    Shutdown handler (do not remove the parameters).

    :param signal: shutdown signal
    :param frame: Frame
    :return: None
    """

    for proc in PROCESSES:
        if proc.is_alive():
            proc.terminate()


######################
# Main method
######################


def compss_persistent_worker():
    """
    Persistent worker main function.
    Retrieves the initial configuration and spawns the worker processes.

    :return: None
    """

    # Set the binding in worker mode
    import pycompss.util.context as context
    context.set_pycompss_context(context.WORKER)

    # Get args
    debug = (sys.argv[1] == 'true')
    global TRACING
    TRACING = (sys.argv[2] == 'true')
    storage_conf = sys.argv[3]
    tasks_x_node = int(sys.argv[4])
    in_pipes = sys.argv[5:5 + tasks_x_node]
    out_pipes = sys.argv[5 + tasks_x_node:]

    if TRACING:
        import pyextrae.multiprocessing as pyextrae
        pyextrae.eventandcounters(SYNC_EVENTS, 1)
        pyextrae.eventandcounters(TASK_EVENTS, WORKER_RUNNING)

    if debug:
        assert tasks_x_node == len(in_pipes)
        assert tasks_x_node == len(out_pipes)

    persistent_storage = False
    if storage_conf != 'null':
        persistent_storage = True
        from storage.api import initWorker as initStorageAtWorker
        from storage.api import finishWorker as finishStorageAtWorker

    # Load log level configuration file
    worker_path = os.path.dirname(os.path.realpath(__file__))
    if debug:
        # Debug
        init_logging_worker(worker_path + '/../../log/logging.json.debug')
    else:
        # Default
        init_logging_worker(worker_path + '/../../log/logging.json.off')

    if __debug__:
        logger = logging.getLogger('pycompss.worker.worker')
        logger.debug("[PYTHON WORKER] piper_worker.py wake up")
        logger.debug("[PYTHON WORKER] -----------------------------")
        logger.debug("[PYTHON WORKER] Persistent worker parameters:")
        logger.debug("[PYTHON WORKER] -----------------------------")
        logger.debug("[PYTHON WORKER] Debug          : " + str(debug))
        logger.debug("[PYTHON WORKER] Tracing        : " + str(TRACING))
        logger.debug("[PYTHON WORKER] Tasks per node : " + str(tasks_x_node))
        logger.debug("[PYTHON WORKER] In Pipes       : " + str(in_pipes))
        logger.debug("[PYTHON WORKER] Out Pipes      : " + str(out_pipes))
        logger.debug("[PYTHON WORKER] Storage conf.  : " + str(storage_conf))
        logger.debug("[PYTHON WORKER] -----------------------------")

    if persistent_storage:
        # Initialize storage
        initStorageAtWorker(config_file_path=storage_conf)

    # Create new threads
    queues = []
    for i in range(0, tasks_x_node):
        if __debug__:
            logger.debug("[PYTHON WORKER] Launching process " + str(i))
        process_name = 'Process-' + str(i)
        queues.append(Queue())

        def create_threads():
            PROCESSES.append(Process(target=worker, args=(queues[i],
                                                          process_name,
                                                          in_pipes[i],
                                                          out_pipes[i],
                                                          storage_conf)))
            PROCESSES[i].start()
        create_threads()

    # Catch SIGTERM send by bindings_piper to exit all PROCESSES
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Wait for all threads
    for i in range(0, tasks_x_node):
        PROCESSES[i].join()

    # Check if there is any exception message from the threads
    for i in range(0, tasks_x_node):
        if not queues[i].empty:
            print(queues[i].get())

    for q in queues:
        q.close()
        q.join_thread()

    if persistent_storage:
        # Finish storage
        finishStorageAtWorker()

    if __debug__:
        logger.debug("[PYTHON WORKER] Finished")

    if TRACING:
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(SYNC_EVENTS, 0)


############################
# Main -> Calls main method
############################

if __name__ == '__main__':
    compss_persistent_worker()
