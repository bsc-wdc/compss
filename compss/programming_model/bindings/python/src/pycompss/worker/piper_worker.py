#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
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
"""
@author: fconejer
@author: cramonco
@author: srodrig1

PyCOMPSs Persistent Worker
===========================
    This file contains the worker code.
"""

import logging
import os
import signal
import sys
import traceback
import thread_affinity
from exceptions import ValueError
from multiprocessing import Process, Queue, Pipe
import thread_affinity

from pycompss.api.parameter import Type, JAVA_MAX_INT, JAVA_MIN_INT
from pycompss.util.serializer import serialize_to_file, deserialize_from_file, deserialize_from_string, SerializerException
from pycompss.util.logs import init_logging_worker

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
debug = True
processes = []

#if sys.version_info >= (2, 7):
#    import importlib

try:
    # Import storage libraries if possible
    from storage.api import getByID
    from storage.api import TaskContext
    from storage.api import initWorker as initStorageAtWorker
    from storage.api import finishWorker as finishStorageAtWorker
except ImportError:
    # If not present, import dummy functions
    from pycompss.storage.api import getByID
    from pycompss.storage.api import TaskContext
    from pycompss.storage.api import initWorker as initStorageAtWorker
    from pycompss.storage.api import finishWorker as finishStorageAtWorker


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
def worker(queue, process_name, input_pipe, output_pipe, cache_queue, cache_pipe, storage_conf):
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
    :param cache_queue: Queue where to put interprocess cache requests
    :param cache_pipe: Pipe to read interprocess cache results
    :return: Nothing
    """

    logger = logging.getLogger('pycompss.worker.worker')
    handler = logger.handlers[0]
    level = logger.getEffectiveLevel()
    formatter = logging.Formatter(handler.formatter._fmt)

    # TRACING
    # if tracing:
    #     pyextrae.eventandcounters(TASK_EVENTS, 0)

    alive = True
    stdout = sys.stdout
    stderr = sys.stderr


    local_cache = None
    if USE_CACHE:
        from persistent_cache import Cache
        local_cache = Cache(size_limit = 200 * 1024**2)

    logger.debug("[PYTHON WORKER] Starting process " + str(process_name))
    while alive:
        with open(input_pipe, 'r', 0) as in_pipe:
            for line in in_pipe:
                def process_task(line):
                    if line != "":
                        line = line.split()
                        if line[0] == EXECUTE_TASK_TAG:
                            # CPU binding
                            binded_cpus = line[-1]

                            def bind_cpus(binded_cpus):
                                if binded_cpus != "-":
                                    binded_cpus = map(int, binded_cpus.split(","))
                                    thread_affinity.setaffinity(binded_cpus)

                            bind_cpus(binded_cpus)

                            line = line[0:-1]
                            # task jobId command
                            job_id = line[1]
                            job_out = line[2]
                            job_err = line[3]
                            # line[4] = <boolean> = ?
                            # line[5] = <integer> = ?
                            # line[6] = <boolean> = ?
                            # line[7] = null      = ?
                            # line[8] = <string>  = operation type (e.g. METHOD)

                            # Swap logger from stream handler to file handler.
                            logger.removeHandler(logger.handlers[0])
                            out_file_handler = logging.FileHandler(job_out)
                            out_file_handler.setLevel(level)
                            out_file_handler.setFormatter(formatter)
                            logger.addHandler(out_file_handler)
                            err_file_handler = logging.FileHandler(job_err)
                            err_file_handler.setLevel(logging.ERROR)
                            err_file_handler.setFormatter(formatter)
                            logger.addHandler(err_file_handler)

                            logger.debug("[PYTHON WORKER %s] Received task." % str(process_name))
                            logger.debug("[PYTHON WORKER %s] - TASK CMD: %s" % (str(process_name), str(line)))
                            try:
                                out = open(job_out, 'w')
                                err = open(job_err, 'w')
                                sys.stdout = out
                                sys.stderr = err
                                exitvalue = execute_task(process_name, storage_conf, line[9:], cache_queue, cache_pipe, local_cache)
                                sys.stdout = stdout
                                sys.stderr = stderr
                                out.close()
                                err.close()

                                # endTask jobId exitValue
                                message = END_TASK_TAG + " " + str(job_id) + " " + str(exitvalue) + "\n"
                                logger.debug("[PYTHON WORKER %s] - Pipe %s END TASK MESSAGE: %s" %(str(process_name),
                                                                                                   str(output_pipe),
                                                                                                   str(message)))
                                with open(output_pipe, 'w+') as out_pipe:
                                    out_pipe.write(message)
                            except Exception, e:
                                logger.exception("[PYTHON WORKER %s] Exception %s" %(str(process_name), str(e)))
                                queue.put("EXCEPTION")

                            # Restore logger
                            logger.removeHandler(out_file_handler)
                            logger.removeHandler(err_file_handler)
                            logger.addHandler(handler)

                        elif line[0] == QUIT_TAG:
                            # Received quit message -> Suicide
                            logger.debug("[PYTHON WORKER %s] Received quit." % str(process_name))
                            return False
                    return True
                alive = process_task(line)

    # TRACING
    # if tracing:
    #     pyextrae.eventandcounters(TASK_EVENTS, PROCESS_DESTRUCTION)
    sys.stdout.flush()
    sys.stderr.flush()
    print("[PYTHON WORKER] Exiting process ", process_name)

#####################################
# Execute Task Method - Task handler
#####################################
def execute_task(process_name, storage_conf, params, cache_queue, cache_pipe, local_cache):
    """
    ExecuteTask main method
    """
    logger = logging.getLogger('pycompss.worker.worker')

    logger.debug("[PYTHON WORKER %s] Begin task execution" % process_name)
    # COMPSs keywords for tasks (ie: tracing, cache data structures...)
    compss_kwargs = {
        'compss_tracing' : tracing,
        'compss_cache_queue': cache_queue,
        'compss_cache_pipe': cache_pipe,
        'compss_process_name': process_name,
        'compss_local_cache': local_cache,
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

    logger.debug("[PYTHON WORKER %s] Storage conf: %s" % (str(process_name), str(storage_conf)))
    logger.debug("[PYTHON WORKER %s] Params: %s" % (str(process_name), str(params)))
    logger.debug("[PYTHON WORKER %s] Path: %s" % (str(process_name), str(path)))
    logger.debug("[PYTHON WORKER %s] Method name: %s" % (str(process_name), str(method_name)))
    logger.debug("[PYTHON WORKER %s] Num slaves: %s" % (str(process_name), str(numSlaves)))
    logger.debug("[PYTHON WORKER %s] Slaves: %s" % (str(process_name), str(slaves)))
    logger.debug("[PYTHON WORKER %s] Cus: %s" % (str(process_name), str(cus)))
    logger.debug("[PYTHON WORKER %s] Has target: %s" % (str(process_name), str(has_target)))
    logger.debug("[PYTHON WORKER %s] Num Params: %s" % (str(process_name), str(num_params)))
    logger.debug("[PYTHON WORKER %s] Args: %s" % (str(process_name), str(args)))

    pos = 0

    values = []
    types = []
    streams = []
    prefixes = []

    #if tracing:
    #    pyextrae.event(TASK_EVENTS, 0)
    #    pyextrae.event(TASK_EVENTS, PARAMETER_PROCESSING)

    # Get all parameter values
    logger.debug("[PYTHON WORKER %s] Processing parameters:" % process_name)
    for i in range(0, num_params):
        pType = int(args[pos])
        pStream = int(args[pos + 1])
        pPrefix = args[pos + 2]
        pValue = args[pos + 3]

        logger.debug("[PYTHON WORKER %s] Parameter : %s" % (process_name, str(i)))
        logger.debug("[PYTHON WORKER %s] \t * Type : %s" % (process_name, str(pType)))
        logger.debug("[PYTHON WORKER %s] \t * Stream : %s" % (process_name, str(pStream)))
        logger.debug("[PYTHON WORKER %s] \t * Prefix : %s" % (process_name, str(pPrefix)))
        logger.debug("[PYTHON WORKER %s] \t * Value: %s" % (process_name, str(pValue)))

        types.append(pType)
        streams.append(pStream)
        prefixes.append(pPrefix)

        if pType == Type.FILE:
            # check if it is a persistent object
            if 'getID' in dir(pValue) and pValue.getID() is not None:
                po = getByID(pValue.getID())
                values.append(po)
            else:
                values.append(pValue)
        elif pType == Type.EXTERNAL_PSCO:
            po = getByID(pValue)
            values.append(po)
            pos += 1  # Skip info about direction (R, W)
        elif pType == Type.STRING:
            num_substrings = int(pValue)
            aux = ''
            first_substring = True
            for j in range(4, num_substrings + 4):
                if not first_substring:
                    aux += ' '
                first_substring = False
                aux += args[pos + j]
            #######
            # Check if the string is really an object
            # Required in order to recover objects passed as parameters.
            # - Option object_conversion
            real_value = aux
            try:
                # try to recover the real object
                aux = deserialize_from_string(aux)
            except (SerializerException, ValueError, EOFError):
                # was not an object
                aux = real_value
            #######
            values.append(aux)
            logger.debug("[PYTHON WORKER %s] \t * Final Value: %s" % (process_name, str(aux)))
            pos += num_substrings
        elif pType == Type.INT:
            values.append(int(pValue))
        elif pType == Type.LONG:
            l = long(pValue)
            if l > JAVA_MAX_INT or l < JAVA_MIN_INT:
                # A Python int was converted to a Java long to prevent overflow
                # We are sure we will not overflow Python int, otherwise this
                # would have been passed as a serialized object.
                l = int(l)
            values.append(l)
        elif pType == Type.DOUBLE:
            values.append(float(pValue))
        elif pType == Type.BOOLEAN:
            if pValue == 'true':
                values.append(True)
            else:
                values.append(False)
        # elif (pType == Type.OBJECT):
        #    pass
        else:
            logger.fatal("[PYTHON WORKER %s] Invalid type (%d) for parameter %d" % (process_name, pType, i))
            exit(1)
        pos += 4

    #if tracing:
    #    pyextrae.event(TASK_EVENTS, 0)
    #    pyextrae.event(TASK_EVENTS, LOGGING)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("[PYTHON WORKER %s] RUN TASK with arguments: " % process_name)
        logger.debug("[PYTHON WORKER %s] \t- Path: %s" % (process_name, path))
        logger.debug("[PYTHON WORKER %s] \t- Method/function name: %s" % (process_name, method_name))
        logger.debug("[PYTHON WORKER %s] \t- Has target: %s" % (process_name, str(has_target)))
        logger.debug("[PYTHON WORKER %s] \t- # parameters: %s" % (process_name, str(num_params)))
        logger.debug("[PYTHON WORKER %s] \t- Values:" % process_name)
        for v in values:
            logger.debug("[PYTHON WORKER %s] \t\t %s" % (process_name, str(v)))
        logger.debug("[PYTHON WORKER %s] \t- COMPSs types:" % process_name)
        for t in types:
            logger.debug("[PYTHON WORKER %s] \t\t %s" % (process_name, str(t)))

    #if tracing:
    #    pyextrae.event(TASK_EVENTS, 0)
    #    pyextrae.event(TASK_EVENTS, MODULES_IMPORT)

    try:
        # Try to import the module (for functions)
        logger.debug("[PYTHON WORKER %s] Trying to import the user module." % process_name)
        if sys.version_info >= (2, 7):
            import importlib
            module = importlib.import_module(path)  # Python 2.7
            logger.debug("[PYTHON WORKER %s] Module successfully loaded (Python version >= 2.7)" % process_name)
        else:
            module = __import__(path, globals(), locals(), [path], -1)
            logger.debug("[PYTHON WORKER %s] Module successfully loaded (Python version < 2.7" % process_name)

        with TaskContext(logger, values, config_file_path=storage_conf):
            #if tracing:
            #    pyextrae.eventandcounters(TASK_EVENTS, 0)
            #    pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)
            logger.debug("[PYTHON WORKER %s] Starting task execution" % process_name)
            def task_execution():
                getattr(module, method_name)(*values, compss_types=types, **compss_kwargs)
            task_execution()
            logger.debug("[PYTHON WORKER %s] Finished task execution" % process_name)
            #if tracing:
            #    pyextrae.eventandcounters(TASK_EVENTS, 0)
            #    pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
    # ==========================================================================
    except AttributeError:
        # Appears with functions that have not been well defined.
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("[PYTHON WORKER %s] WORKER EXCEPTION - Attribute Error Exception" % process_name)
        logger.exception(''.join(line for line in lines))
        logger.exception("[PYTHON WORKER %s] Check that all parameters have been defined with an absolute import path (even if in the same file)" % process_name)
        exit(1)
    # ==========================================================================
    except ImportError:
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
        logger.debug("[PYTHON WORKER %s] Method in class %s of module %s" % (process_name, class_name, module_name))

        if has_target == 'true':
            # Instance method
            file_name = values.pop()
            logger.debug("[PYTHON WORKER %s] Deserialize self from file." % process_name)
            obj = deserialize_from_file(file_name)

            logger.debug("[PYTHON WORKER %s] Processing callee, a hidden object of %s in file %s" % (process_name, file_name, type(obj)))
            values.insert(0, obj)
            types.pop()
            types.insert(0, Type.OBJECT)

            with TaskContext(logger, values, config_file_path=storage_conf):
                #if tracing:
                #    pyextrae.eventandcounters(TASK_EVENTS, 0)
                #    pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)
                logger.debug("[PYTHON WORKER %s] Starting task execution")
                def task_execution():
                    getattr(klass, method_name)(*values, compss_types=types, **compss_kwargs)
                task_execution()
                logger.debug("[PYTHON WORKER %s] Finished task execution")
                #if tracing:
                #    pyextrae.eventandcounters(TASK_EVENTS, 0)
                #    pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
            logger.debug("[PYTHON WORKER %s] Serializing self to file." % process_name)
            logger.debug("[PYTHON WORKER %s] Obj: %s" % (process_name,str(obj)))
            serialize_to_file(obj, file_name)
        else:
            # Class method - class is not included in values (e.g. values = [7])
            types.insert(0, None)  # class must be first type

            with TaskContext(logger, values, config_file_path=storage_conf):
                #if tracing:
                #    pyextrae.eventandcounters(TASK_EVENTS, 0)
                #    pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)
                logger.debug("[PYTHON WORKER %s] Starting task execution")
                def task_execution():
                    getattr(klass, method_name)(*values, compss_types=types, **compss_kwargs)
                task_execution()
                logger.debug("[PYTHON WORKER %s] Finished task execution")
                #if tracing:
                #    pyextrae.eventandcounters(TASK_EVENTS, 0)
                #    pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
    # ==========================================================================
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("[PYTHON WORKER %s] WORKER EXCEPTION" % process_name)
        logger.exception(''.join(line for line in lines))
        return 1

    # EVERYTHING OK
    logger.debug("[PYTHON WORKER %s] End task execution. Status: Ok" % process_name)

    # if tracing:
    #     pyextrae.eventandcounters(TASK_EVENTS, 0)

    return 0




def shutdown_handler(signal, frame):
    for proc in processes:
        if proc.is_alive():
            proc.terminate()


USE_CACHE = False
# tamanyo
# politica/s de borrado

def cache_proc(cache_queue, cache_pipes):
    from persistent_cache import Cache
    from shm_manager import shm_manager as SHM
    cache = Cache(size_limit = 1024)

    while True:
        msg = cache_queue.get()
        if msg == '##END##':
            return
        # if msg is not the poisoned pill
        # then it is a triplet (id, filename, comments)
        # that must be read as "the process with identifier id wants the file
        # named filename and the additional given comments must be followed"
        process_id, file_name, comments = msg
        suf_file_name = os.path.split(file_name)[-1]
        process_ord = int(process_id)
        # some worker has created an SHM segment and wants it to be added to
        # the persistent worker's cache
        if comments[0] == 'ATTACH_SEGMENT':
            segment_key, segment_bytes = map(long, comments[1:])
            manager = SHM(segment_key, segment_bytes, 0666)
            # we have this object, lets update it
            if cache.has_object(suf_file_name):
                cache.hit(suf_file_name)
                cache.set_object(suf_file_name, manager, segment_bytes)
            # add the object
            else:
                cache.add(suf_file_name, manager, 1)
            # notify the worker that we are done
            cache_pipes[process_ord].send('DONE')
        elif cache.has_object(suf_file_name):
            cache.hit(suf_file_name)
            shm_manager = cache.get(suf_file_name)
            cache_pipes[process_ord].send((shm_manager.key, shm_manager.bytes))
            # let's wait until the requesting process has attached the SHM
            # and notified it to the Cache process
            # note that attaching does not mean immediate remapping or
            # reading, so this wait does not prevent workers to read in
            # parallel from SHMs
            cache_pipes[process_ord].recv()
        else:
            # we dont have the object, so lets tell the caller to read the obj,
            # and store it in the cache
            cache_pipes[process_ord].send('N') # NO!
            # we must avoid to put outdated versions of INOUT objects so,
            # if we do not have some INOUT object queried as an IN, we must
            # simply tell the worker to read it from disk since it makes no
            # sense to store an outdated object for future uses
            if comments[0] != 'INOUT_IN':
                dumped_obj = open(file_name, 'rb').read()
                manager = SHM(1337, len(dumped_obj))
                manager.write_object(dumped_obj)
                cache.add(suf_file_name, manager, 1)

######################
# Main method
######################


def compss_persistent_worker():

    # Get args
    debug = (sys.argv[1] == 'true')
    tracing = (sys.argv[2] == 'true')
    tasks_x_node = int(sys.argv[3])
    in_pipes = sys.argv[4:4 + tasks_x_node]
    out_pipes = sys.argv[4 + tasks_x_node:]
    storage_conf = None # TODO: NEEDS TO BE PASSED BY THE RUNTIME

    if tracing:
        import pyextrae.multiprocessing as pyextrae
        pyextrae.eventandcounters(SYNC_EVENTS, 1)
        pyextrae.eventandcounters(TASK_EVENTS, WORKER_RUNNING)

    if debug:
        assert tasks_x_node == len(in_pipes)
        assert tasks_x_node == len(out_pipes)

    # Load log level configuration file
    worker_path = os.path.dirname(os.path.realpath(__file__))
    if debug:
        # Debug
        init_logging_worker(worker_path + '/../../log/logging.json.debug')
    else:
        # Default
        init_logging_worker(worker_path + '/../../log/logging.json.off')

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
    logger.debug("[PYTHON WORKER] Storage conf.  : " + str(out_pipes))
    logger.debug("[PYTHON WORKER] -----------------------------")

    # Initialize storage
    initStorageAtWorker(config_file_path=storage_conf)
    if USE_CACHE:
        cache_queue = Queue()
        cache_pipes = [Pipe() for _ in range(tasks_x_node)]
        # Create cache process
        cache_process = Process(target=cache_proc, args=(cache_queue, [p[0] for p in cache_pipes]))
        cache_process.start()

    # Create new threads
    queues = []
    for i in xrange(0, tasks_x_node):
        logger.debug("[PYTHON WORKER] Launching process " + str(i))
        process_name = 'Process-' + str(i)
        queues.append(Queue())
        if USE_CACHE:
            cache_queue_p = cache_queue
            cache_pipe_p = cache_pipes[i][1]
        else:
            cache_queue_p = None
            cache_pipe_p = None
        def create_threads():
            processes.append(Process(target=worker, args=(queues[i],
                                                          process_name,
                                                          in_pipes[i],
                                                          out_pipes[i],
                                                          cache_queue_p,
                                                          cache_pipe_p,
                                                          storage_conf)))
            processes[i].start()
        create_threads()

    # Catch SIGTERM send by bindings_piper to exit all processes
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Wait for all threads
    for i in xrange(0, tasks_x_node):
        processes[i].join()

    if USE_CACHE:
        cache_queue.put("##END##")

        cache_queue.close()
        cache_queue.join_thread()
        for (x, y) in cache_pipes:
            x.close()
            y.close()
        cache_process.join()


    # Check if there is any exception message from the threads
    for i in xrange(0, tasks_x_node):
        if not queues[i].empty:
            print(queues[i].get())

    for q in queues:
        q.close()
        q.join_thread()

    # Finish storage
    finishStorageAtWorker()

    logger.debug("[PYTHON WORKER] Finished")
    if tracing:
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(SYNC_EVENTS, 0)


############################
# Main -> Calls main method
############################

if __name__ == '__main__':
    compss_persistent_worker()
