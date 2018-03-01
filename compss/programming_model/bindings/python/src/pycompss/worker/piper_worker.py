#!/usr/bin/python

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
import thread_affinity
import base64

if sys.version_info >= (3, 0):
    long = int
    str_escape = 'unicode_escape'
else:
    # Exception moved to built-in
    from exceptions import ValueError
    str_escape = 'string_escape'


from pycompss.api.parameter import TYPE
from pycompss.api.parameter import JAVA_MIN_INT, JAVA_MAX_INT
from pycompss.util.serializer import serialize_to_file
from pycompss.util.serializer import deserialize_from_file
from pycompss.util.serializer import deserialize_from_string
from pycompss.util.serializer import SerializerException
from pycompss.util.logs import init_logging_worker
from pycompss.util.persistent_storage import is_PSCO, get_by_ID

SYNC_EVENTS = 8000666

# Should be equal to Tracer.java definitions
TASK_EVENTS = 60000100

PROCESS_CREATION = 100
WORKER_RUNNING = 102
PARAMETER_PROCESSING = 103
LOGGING = 104
TASK_EXECUTION = 105
WORKER_END = 106
PROCESS_DESTRUCTION = 107
MODULES_IMPORT = 108

# Persistent worker global variables
tracing = False
processes = []

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
    :return: Nothing
    """

    logger = logging.getLogger('pycompss.worker.worker')
    handler = logger.handlers[0]
    level = logger.getEffectiveLevel()
    formatter = logging.Formatter(handler.formatter._fmt)

    if storage_conf != 'null':
        try:
            from storage.api import initWorkerPostFork as initStorageAtWorkerPostFork
            initStorageAtWorkerPostFork()
        except:
            if __debug__:
                logger.info("[PYTHON WORKER] Could not find initWorkerPostFork storage call. Ignoring it.")

    # TRACING
    # if tracing:
    #     pyextrae.eventandcounters(TASK_EVENTS, 0)

    alive = True
    stdout = sys.stdout
    stderr = sys.stderr

    if __debug__:
        logger.debug("[PYTHON WORKER] Starting process " + str(process_name))

    while alive:
        in_pipe = open(input_pipe, 'r')  # , 0) # 0 just for python 2

        affinity_ok = True

        def process_task(line, pipe):
            if __debug__:
                logger.debug("[PYTHON WORKER] Received message: %s" % str(line))
            line = line.split()
            pipe.close()
            if line[0] == EXECUTE_TASK_TAG:
                # CPU binding
                binded_cpus = line[-3]

                def bind_cpus(binded_cpus):
                    if binded_cpus != "-":
                        os.environ['COMPSS_BINDED_CPUS'] = binded_cpus
                        if __debug__:
                            logger.debug("[PYTHON WORKER] Assigning affinity %s" % str(binded_cpus))
                        binded_cpus = list(map(int, binded_cpus.split(",")))
                        try:
                            thread_affinity.setaffinity(binded_cpus)
                        except:
                            if __debug__:
                                logger.error("[PYTHON WORKER] Warning: could not assign affinity %s" % str(binded_cpus))
                            affinity_ok = False
                bind_cpus(binded_cpus)

                # GPU binding
                binded_gpus = line[-2]

                def bind_gpus(binded_gpus):
                    if binded_gpus != "-":
                        os.environ['COMPSS_BINDED_GPUS'] = binded_gpus
                        os.environ['CUDA_VISIBLE_DEVICES'] = binded_gpus
                        os.environ['GPU_DEVICE_ORDINAL'] = binded_gpus
                bind_gpus(binded_gpus)

                # Hostlist
                hostlist = line[-1]

                def treat_hostlist(hostlist):
                    os.environ['COMPSS_HOSTNAMES'] = hostlist
                treat_hostlist(hostlist)

                # Remove the last elements: cpu and gpu bindings
                line = line[0:-3]

                # task jobId command
                job_id = line[1]
                job_out = line[2]
                job_err = line[3]
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
                    logger.debug("[PYTHON WORKER %s] - TASK CMD: %s" % (str(process_name), str(line)))

                # Swap logger from stream handler to file handler.   #### TODO: FIX LOGGER!
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
                    logger.debug("[PYTHON WORKER %s] - TASK CMD: %s" % (str(process_name), str(line)))

                try:
                    out = open(job_out, 'w')
                    err = open(job_err, 'w')
                    sys.stdout = out
                    sys.stderr = err
                    if not affinity_ok:
                        err.write('WARNING: This task is going to be executed with default thread affinity %s' % thread_affinity.getaffinity())
                    exitvalue, newTypes, newValues = execute_task(process_name, storage_conf, line[9:])
                    sys.stdout = stdout
                    sys.stderr = stderr
                    sys.stdout.flush()
                    sys.stderr.flush()
                    out.close()
                    err.close()

                    if exitvalue == 0:
                        # Task has finished without exceptions
                        # endTask jobId exitValue message
                        params = buildReturnParamsMessage(line[9:], newTypes, newValues)
                        message = END_TASK_TAG + " " + str(job_id) \
                                               + " " + str(exitvalue) \
                                               + " " + str(params) + "\n"
                    else:
                        # An exception has been raised in task
                        message = END_TASK_TAG + " " + str(job_id) \
                                  + " " + str(exitvalue) + "\n"

                    if __debug__:
                        logger.debug("[PYTHON WORKER %s] - Pipe %s END TASK MESSAGE: %s" % (str(process_name),
                                                                                            str(output_pipe),
                                                                                            str(message)))
                    # The return message is:
                    #
                    # TaskResult ==> jobId exitValue List<Object>
                    #
                    # Where List<Object> has D length:
                    # D = #parameters + (hasTarget ? 1 : 0) + (hasReturn ? 1 : 0)
                    # And contains:
                    # - Null if it NOT a PSCO
                    # - PSCOId (String) if is a PSCO
                    #
                    # This is sent through the pipe with the endTask message.
                    # If the task had an object or file as parameter and the worker returns the id,
                    # the runtime can change the type (and locations) to a EXTERNAL_OBJ_T.

                    with open(output_pipe, 'w') as out_pipe:  # removed w+ TODO: should be a?
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

            elif line[0] == QUIT_TAG:
                # Received quit message -> Suicide
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Received quit." % str(process_name))
                return False
            return True

        for line in in_pipe:
            if line != "":
                alive = process_task(line, in_pipe)
                break

    # TRACING
    # if tracing:
    #     pyextrae.eventandcounters(TASK_EVENTS, PROCESS_DESTRUCTION)
    sys.stdout.flush()
    sys.stderr.flush()
    print("[PYTHON WORKER] Exiting process ", process_name)


def buildReturnParamsMessage(params, types, values):
    assert len(types) == len(values), 'Inconsistent state: return type-value length mismatch for return message.'

    # Analize the input parameters to get has_target and has_return
    # More information that requested can be gathered and returned in the return message if necessary.
    numSlaves = int(params[2])
    argPosition = 3 + numSlaves
    args = params[argPosition + 1:]
    has_target = args[0]
    if has_target == 'false':
        hasTarget = False
    else:
        hasTarget = True
    return_type = args[1]
    if return_type == 'null':
        hasReturn = False
    else:
        hasReturn = True

    pairs = list(zip(types, values))
    num_params = len(pairs)
    params = ''
    for p in pairs:
        params = params + str(p[0]) + ' ' + str(p[1]) + ' '
    totalParams = num_params + (1 if hasTarget else 0) + (1 if hasReturn else 0)
    message = str(totalParams) + ' ' + params
    return message


#####################################
# Execute Task Method - Task handler
#####################################
def execute_task(process_name, storage_conf, params):
    """
    ExecuteTask main method
    """
    logger = logging.getLogger('pycompss.worker.worker')

    if __debug__:
        logger.debug("[PYTHON WORKER %s] Begin task execution" % process_name)

    persistent_storage = False
    if storage_conf != 'null':
        persistent_storage = True
        from pycompss.util.persistent_storage import storage_task_context

    # COMPSs keywords for tasks (ie: tracing, process name...)
    compss_kwargs = {
        'compss_tracing': tracing,
        'compss_process_name': process_name,
        'compss_storage_conf': storage_conf
    }

    # Retrieve the parameters from the params argument
    path = params[0]
    method_name = params[1]
    numSlaves = int(params[2])
    slaves = []
    for i in range(2, 2 + numSlaves):
        slaves.append(params[i])
    argPosition = 3 + numSlaves

    args = params[argPosition:]
    cus = args[0]

    args = args[1:]
    has_target = args[0]
    return_type = args[1]
    num_params = int(args[2])

    args = args[3:]

    if __debug__:
        logger.debug("[PYTHON WORKER %s] Storage conf: %s" % (str(process_name), str(storage_conf)))
        logger.debug("[PYTHON WORKER %s] Params: %s" % (str(process_name), str(params)))
        logger.debug("[PYTHON WORKER %s] Path: %s" % (str(process_name), str(path)))
        logger.debug("[PYTHON WORKER %s] Method name: %s" % (str(process_name), str(method_name)))
        logger.debug("[PYTHON WORKER %s] Num slaves: %s" % (str(process_name), str(numSlaves)))
        logger.debug("[PYTHON WORKER %s] Slaves: %s" % (str(process_name), str(slaves)))
        logger.debug("[PYTHON WORKER %s] Cus: %s" % (str(process_name), str(cus)))
        logger.debug("[PYTHON WORKER %s] Has target: %s" % (str(process_name), str(has_target)))
        logger.debug("[PYTHON WORKER %s] Num Params: %s" % (str(process_name), str(num_params)))
        logger.debug("[PYTHON WORKER %s] Args: %r" % (str(process_name), args))

    # if tracing:
    #     pyextrae.event(TASK_EVENTS, 0)
    #     pyextrae.event(TASK_EVENTS, PARAMETER_PROCESSING)

    # Get all parameter values
    if __debug__:
        logger.debug("[PYTHON WORKER %s] Processing parameters:" % process_name)
    values, types, streams, prefixes = get_input_params(num_params, logger, args, process_name, persistent_storage)

    # if tracing:
    #     pyextrae.event(TASK_EVENTS, 0)
    #     pyextrae.event(TASK_EVENTS, LOGGING)

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

    # if tracing:
    #     pyextrae.event(TASK_EVENTS, 0)
    #     pyextrae.event(TASK_EVENTS, MODULES_IMPORT)

    import_error = False

    newTypes = []
    newValues = []

    try:
        # Try to import the module (for functions)
        if __debug__:
            logger.debug("[PYTHON WORKER %s] Trying to import the user module." % process_name)
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
                newTypes, newValues = task_execution_1()
        else:
            newTypes, newValues = task_execution_1()

    # ==========================================================================
    except AttributeError:
        # Appears with functions that have not been well defined.
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("[PYTHON WORKER %s] WORKER EXCEPTION - Attribute Error Exception" % process_name)
        logger.exception(''.join(line for line in lines))
        logger.exception("[PYTHON WORKER %s] Check that all parameters have been defined with an absolute import path (even if in the same file)" % process_name)
        # If exception is raised during the task execution, newTypes and
        # newValues are empty
        return 1, newTypes, newValues
    # ==========================================================================
    except ImportError:
        import_error = True
    # ==========================================================================
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("[PYTHON WORKER %s] WORKER EXCEPTION" % process_name)
        logger.exception(''.join(line for line in lines))
        # If exception is raised during the task execution, newTypes and
        # newValues are empty
        return 1, newTypes, newValues

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
            last_elem = values.pop()
            if is_PSCO(last_elem):
                obj = last_elem
            else:
                file_name = last_elem.split(':')[-1]
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Deserialize self from file." % process_name)
                obj = deserialize_from_file(file_name)
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Processing callee, a hidden object of %s in file %s" % (process_name, file_name, type(obj)))
            values.insert(0, obj)
            types.pop()
            types.insert(0, TYPE.OBJECT if not is_PSCO(last_elem) else TYPE.EXTERNAL_PSCO)

            def task_execution_2():
                return task_execution(logger, process_name, klass, method_name, types, values, compss_kwargs)

            if persistent_storage:
                with storage_task_context(logger, values, config_file_path=storage_conf):
                    newTypes, newValues = task_execution_2()
            else:
                newTypes, newValues = task_execution_2()

            if is_PSCO(last_elem):
                # There is no update PSCO on the storage API. Consequently, the changes on the PSCO must have been
                # pushed into the storage automatically on each PSCO modification.
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] The changes on the PSCO must have been automatically updated by the storage." % process_name)
                pass
            else:
                if __debug__:
                    logger.debug("[PYTHON WORKER %s] Serializing self to file." % process_name)
                serialize_to_file(obj, file_name)
            if __debug__:
                logger.debug("[PYTHON WORKER %s] Obj: %r" % (process_name, obj))
        else:
            # Class method - class is not included in values (e.g. values = [7])
            types.insert(0, None)  # class must be first type

            def task_execution_3():
                return task_execution(logger, process_name, klass, method_name, types, values, compss_kwargs)

            if persistent_storage:
                with storage_task_context(logger, values, config_file_path=storage_conf):
                    newTypes, newValues = task_execution_3()
            else:
                newTypes, newValues = task_execution_3()

    # EVERYTHING OK
    if __debug__:
        logger.debug("[PYTHON WORKER %s] End task execution. Status: Ok" % process_name)

    # if tracing:
    #     pyextrae.eventandcounters(TASK_EVENTS, 0)

    return 0, newTypes, newValues   # Exit code, updated params


def get_input_params(num_params, logger, args, process_name, persistent_storage):
    pos = 0
    values = []
    types = []
    streams = []
    prefixes = []

    def is_redis():
        try:
            import storage.api
            return storage.api.__name__ == "redispycompss"
        except:
            # Could not import storage api
            return False

    if is_redis():
        pre_pipeline = []

    for i in range(0, num_params):
        pType = int(args[pos])
        pStream = int(args[pos + 1])
        pPrefix = args[pos + 2]
        pValue = args[pos + 3]

        if __debug__:
            logger.debug("[PYTHON WORKER %s] Parameter : %s" % (process_name, str(i)))
            logger.debug("[PYTHON WORKER %s] \t * Type : %s" % (process_name, str(pType)))
            logger.debug("[PYTHON WORKER %s] \t * Stream : %s" % (process_name, str(pStream)))
            logger.debug("[PYTHON WORKER %s] \t * Prefix : %s" % (process_name, str(pPrefix)))
            logger.debug("[PYTHON WORKER %s] \t * Value: %r" % (process_name, pValue))

        types.append(pType)
        streams.append(pStream)
        prefixes.append(pPrefix)

        if pType == TYPE.FILE:
            values.append(pValue)
        elif pType == TYPE.EXTERNAL_PSCO:
            if is_redis():
                po = pValue
                pre_pipeline.append((po, len(values)))
            else:
                po = get_by_ID(pValue)
            values.append(po)
            pos += 1  # Skip info about direction (R, W)
        elif pType == TYPE.STRING:
            num_substrings = int(pValue)
            aux = ''
            first_substring = True
            for j in range(4, num_substrings + 4):
                if not first_substring:
                    aux += ' '
                first_substring = False
                aux += args[pos + j]
            # Decode the string received
            aux = base64.b64decode(aux.encode())
            #######
            # Check if the string is really an object
            # Required in order to recover objects passed as parameters.
            # - Option object_conversion
            real_value = aux
            try:
                # try to recover the real object
                aux = deserialize_from_string(aux.decode(str_escape))
            except (SerializerException, ValueError, EOFError):
                # was not an object
                aux = str(real_value.decode())
            #######
            values.append(aux)
            if __debug__:
                logger.debug("[PYTHON WORKER %s] \t * Final Value: %s" % (process_name, str(aux)))
            pos += num_substrings
        elif pType == TYPE.INT:
            values.append(int(pValue))
        elif pType == TYPE.LONG:
            my_l = long(pValue)
            if my_l > JAVA_MAX_INT or my_l < JAVA_MIN_INT:
                # A Python int was converted to a Java long to prevent overflow
                # We are sure we will not overflow Python int, otherwise this
                # would have been passed as a serialized object.
                my_l = int(my_l)
            values.append(my_l)
        elif pType == TYPE.DOUBLE:
            values.append(float(pValue))
        elif pType == TYPE.BOOLEAN:
            if pValue == 'true':
                values.append(True)
            else:
                values.append(False)
        # elif (pType == TYPE.OBJECT):
        #    pass
        else:
            logger.fatal("[PYTHON WORKER %s] Invalid type (%d) for parameter %d" % (process_name, pType, i))
            exit(1)
        pos += 4
    if is_redis() and pre_pipeline:
        ids = [ident for (ident, _) in pre_pipeline]
        from storage.api import getByID
        retrieved_objects = getByID(*ids)
        if len(ids) == 1: retrieved_objects = [retrieved_objects]
        objindex = zip(retrieved_objects, [index for (_, index) in pre_pipeline])
        for (obj, index) in objindex:
            values[index] = obj

    return values, types, streams, prefixes


def task_execution(logger, process_name, module, method_name, types, values, compss_kwargs):
    # if tracing:
    #    pyextrae.eventandcounters(TASK_EVENTS, 0)
    #    pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)

    if __debug__:
        logger.debug("[PYTHON WORKER %s] Starting task execution" % process_name)
        logger.debug("[PYTHON WORKER %s] Types : %s " % (process_name, str(types)))
        logger.debug("[PYTHON WORKER %s] Values: %s " % (process_name, str(values)))

    # WARNING: the following call will not work if a user decorator overrides the return of the task decorator.
    # newTypes, newValues = getattr(module, method_name)(*values, compss_types=types, **compss_kwargs)
    # If the @task is decorated with a user decorator, may include more return values, and consequently,
    # the newTypes and newValues will be within a tuple at position 0.
    # Force users that use decorators on top of @task to return the task results first.
    # This is tested with the timeit decorator in test 19.
    taskOutput = getattr(module, method_name)(*values, compss_types=types, **compss_kwargs)

    if isinstance(taskOutput[0], tuple):  # Weak but effective way to check it without doing inspect.
        # Another decorator has added another return thing.
        # TODO: Should we consider here to create a list with all elements and serialize it to a file with the real task output plus the decorator results? == taskOutput[1:]
        # TODO: Currently, the extra result is ignored.
        newTypes = taskOutput[0][0]
        newValues = taskOutput[0][1]
    else:
        # The taskOutput is composed by the newTypes and newValues returned by the task decorator.
        newTypes = taskOutput[0]
        newValues = taskOutput[1]

        if __debug__:
            # The types may change (e.g. if the user does a makePersistent within the task)
            logger.debug("[PYTHON WORKER %s] Return Types : %s " % (process_name, str(newTypes)))
            logger.debug("[PYTHON WORKER %s] Return Values: %s " % (process_name, str(newValues)))
            logger.debug("[PYTHON WORKER %s] Finished task execution" % process_name)

    # if tracing:
    #    pyextrae.eventandcounters(TASK_EVENTS, 0)
    #    pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
    return newTypes, newValues


def shutdown_handler(signal, frame):
    for proc in processes:
        if proc.is_alive():
            proc.terminate()


######################
# Main method
######################


def compss_persistent_worker():

    # Get args
    debug = (sys.argv[1] == 'true')
    tracing = (sys.argv[2] == 'true')
    storage_conf = sys.argv[3]
    tasks_x_node = int(sys.argv[4])
    in_pipes = sys.argv[5:5 + tasks_x_node]
    out_pipes = sys.argv[5 + tasks_x_node:]

    if tracing:
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
        logger.debug("[PYTHON WORKER] Tracing        : " + str(tracing))
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
            processes.append(Process(target=worker, args=(queues[i],
                                                          process_name,
                                                          in_pipes[i],
                                                          out_pipes[i],
                                                          storage_conf)))
            processes[i].start()
        create_threads()

    # Catch SIGTERM send by bindings_piper to exit all processes
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Wait for all threads
    for i in range(0, tasks_x_node):
        processes[i].join()

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

    if tracing:
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(SYNC_EVENTS, 0)


############################
# Main -> Calls main method
############################

if __name__ == '__main__':
    compss_persistent_worker()
