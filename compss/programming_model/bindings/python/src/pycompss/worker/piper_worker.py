"""
@author: etejedor
@author: fconejer
@author: cramonco

PyCOMPSs Persistent Worker
===========================
    This file contains the worker code.
"""

import logging
import os
import signal
import sys
import traceback
from cPickle import loads, UnpicklingError
from exceptions import ValueError
from multiprocessing import Process
from multiprocessing import Queue

from pycompss.api.parameter import Type, JAVA_MAX_INT, JAVA_MIN_INT
from pycompss.util.logs import init_logging_worker

SYNC_EVENTS = 8000666

# Should be equal to Tracer.java definitions
TASK_EVENTS = 8000010
PROCESS_CREATION = 1
WORKER_INITIALIZATION = 2
PARAMETER_PROCESSING = 3
LOGGING = 4
TASK_EXECUTION = 5
WORKER_END = 6
PROCESS_DESTRUCTION = 7

# Persistent worker global variables
tracing = False
debug = True
processes = []

if sys.version_info >= (2, 7):
    import importlib

try:
    # Import storage libraries if possible
    from storage.api import getByID
    from storage.api import TaskContext
except ImportError:
    # If not present, import dummy functions
    from pycompss.storage.api import getByID
    from pycompss.storage.api import TaskContext


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
def worker(queue, process_name, input_pipe, output_pipe):
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

    # TRACING
    # if tracing:
    #     pyextrae.eventandcounters(TASK_EVENTS, 0)

    alive = True
    stdout = sys.stdout
    stderr = sys.stderr

    print("[PYTHON WORKER] Starting process ", process_name)
    while alive:
        with open(input_pipe, 'r', 0) as in_pipe:
            for line in in_pipe:
                if line != "":
                    line = line.split()
                    if line[0] == EXECUTE_TASK_TAG:
                        # task jobId command
                        job_id = line[1]
                        job_out = line[2]
                        job_err = line[3]
                        print("[PYTHON WORKER] Received task at ", process_name)
                        print("[PYTHON WORKER] - TASK CMD: ", line)
                        try:
                            sys.stdout = open(job_out, 'w')
                            sys.stderr = open(job_err, 'w')
                            exitvalue = execute_task(process_name, line[7:])
                            sys.stdout = stdout
                            sys.stderr = stderr

                            # endTask jobId exitValue
                            message = END_TASK_TAG + " " + str(job_id) \
                                      + " " + str(exitvalue) + "\n"
                            print("[PYTHON WORKER] - Pipe ", output_pipe, " END TASK MESSAGE: ", message)
                            with open(output_pipe, 'w+') as out_pipe:
                                out_pipe.write(message)
                        except Exception, e:
                            print("[PYTHON WORKER] Exception ", e)
                            queue.put("EXCEPTION")
                    elif line[0] == QUIT_TAG:
                        # Received quit message -> Suicide
                        print("[PYTHON WORKER] Received quit at ", process_name)
                        alive = False

    # TRACING
    # if tracing:
    #     pyextrae.eventandcounters(TASK_EVENTS, PROCESS_DESTRUCTION)
    print("[PYTHON WORKER] Exiting process ", process_name)


#####################################
# Execute Task Method - Task handler
#####################################  
def execute_task(process_name, params):
    """
    ExecuteTask main method
    """
    print("[PYTHON WORKER] Begin task execution")
    logger = logging.getLogger('pycompss.worker.worker')
    logger.debug("Starting Worker")

    path = params[0]
    method_name = params[1]
    has_target = params[2]
    num_params = int(params[3])

    args = params[4:]
    pos = 0
    values = []
    types = []
    # if tracing:
    #     pyextrae.event(TASK_EVENTS, 0)
    #     pyextrae.event(TASK_EVENTS, PARAMETER_PROCESSING)
    # Get all parameter values
    for i in range(0, num_params):
        ptype = int(args[pos])
        types.append(ptype)

        if ptype == Type.FILE:
            values.append(args[pos + 1])
        elif (ptype == Type.PERSISTENT):
            po = getByID(args[pos + 1])
            values.append(po)
            pos = pos + 1  # Skip info about direction (R, W)
        elif ptype == Type.STRING:
            num_substrings = int(args[pos + 1])
            aux = ''
            for j in range(2, num_substrings + 2):
                aux += args[pos + j]
                if j < num_substrings + 1:
                    aux += ' '
            #######
            # Check if the string is really an object
            # Required in order to recover objects passed as parameters.
            # - Option object_conversion
            real_value = aux
            try:
                # try to recover the real object
                aux = loads(aux)
            except (UnpicklingError, ValueError, EOFError):
                # was not an object
                aux = real_value
            #######
            values.append(aux)
            pos += num_substrings
        elif ptype == Type.INT:
            values.append(int(args[pos + 1]))
        elif ptype == Type.LONG:
            l = long(args[pos + 1])
            if l > JAVA_MAX_INT or l < JAVA_MIN_INT:
                # A Python int was converted to a Java long to prevent overflow
                # We are sure we will not overflow Python int, otherwise this
                # would have been passed as a serialized object.
                l = int(l)
            values.append(l)
        elif ptype == Type.FLOAT:
            values.append(float(args[pos + 1]))
        elif ptype == Type.BOOLEAN:
            if args[pos + 1] == 'true':
                values.append(True)
            else:
                values.append(False)
        # elif (ptype == Type.OBJECT):
        #    pass
        else:
            logger.fatal("Invalid type (%d) for parameter %d" % (ptype, i))
            print("[PYTHON WORKER] Error: Invalid type for parameter" + str(i))
            print("[PYTHON WORKER] End task execution.")
            return 1

        pos += 2

    # if tracing:
    #     pyextrae.event(TASK_EVENTS, 0)
    #     pyextrae.event(TASK_EVENTS, LOGGING)
    if logger.isEnabledFor(logging.DEBUG):
        values_str = ''
        types_str = ''
        for v in values:
            values_str += "\t\t" + str(v) + "\n"
        for t in types:
            types_str += str(t) + " "
        logger.debug("RUN TASK with arguments\n" +
                     "\t- Path: " + path + "\n" +
                     "\t- Method/function name: " + method_name + "\n" +
                     "\t- Has target: " + has_target + "\n" +
                     "\t- # parameters: " + str(num_params) + "\n" +
                     "\t- Values:\n" + values_str +
                     "\t- COMPSs types: " + types_str)

    try:
        # Try to import the module (for functions)
        if sys.version_info >= (2, 7):
            module = importlib.import_module(path)  # Python 2.7
            logger.debug("Version >= 2.7")
        else:
            module = __import__(path, globals(), locals(), [path], -1)
            logger.debug("Version < 2.7")

        with TaskContext(logger, values):
            # if tracing:
            #     pyextrae.eventandcounters(TASK_EVENTS, 0)
            #     pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)
            getattr(module, method_name)(*values, compss_types=types)
            # if tracing:
            #     pyextrae.eventandcounters(TASK_EVENTS, 0)
            #     pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
    # ==========================================================================
    except AttributeError:
        # Appears with functions that have not been well defined.
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("WORKER EXCEPTION - Attribute Error Exception")
        logger.exception(''.join(line for line in lines))
        logger.exception("Check that all parameters have been defined with " +
                         "an absolute import path (even if in the same file)")
        print("[PYTHON WORKER] Error: Attribute Error Exception")
        print("[PYTHON WORKER] End task execution.")
        return 1
    # ==========================================================================
    except ImportError, e:
        logger.exception("WORKER EXCEPTION ", e)
        logger.exception("Trying to recover!!!")
        from pycompss.util.serializer import deserialize_from_file
        from pycompss.util.serializer import serialize_to_file
        # Not the path of a module, it ends with a class name
        class_name = path.split('.')[-1]
        # module_name = path.replace('.' + class_name, '')  # BUG - does not support same filename as a package
        module_name = '.'.join(path.split('.')[0:-1])  # SOLUTION - all path but the class_name means the module_name
        if '.' in path:
            module_name = '.'.join(
                path.split('.')[0:-1])  # SOLUTION - all path but the class_name means the module_name
        else:
            module_name = path

        module = __import__(module_name, fromlist=[class_name])

        klass = getattr(module, class_name)

        logger.debug("Method in class %s of module %s"
                     % (class_name, module_name))

        if has_target == 'true':
            # Instance method
            file_name = values.pop()
            obj = deserialize_from_file(file_name)
            logger.debug("Processing callee, a hidden object of %s in file %s"
                         % (file_name, type(obj)))
            values.insert(0, obj)
            types.pop()
            types.insert(0, Type.OBJECT)

            with TaskContext(logger, values):
                # if tracing:
                #     pyextrae.eventandcounters(TASK_EVENTS, 0)
                #     pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)
                getattr(klass, method_name)(*values, compss_types=types)
                # if tracing:
                #     pyextrae.eventandcounters(TASK_EVENTS, 0)
                #     pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
            serialize_to_file(obj, file_name, force=True)
        else:
            # Class method - class is not included in values (e.g. values = [7])
            types.insert(0, None)  # class must be first type

            with TaskContext(logger, values):
                # if tracing:
                #     pyextrae.eventandcounters(TASK_EVENTS, 0)
                #     pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)
                getattr(klass, method_name)(*values, compss_types=types)
                # if tracing:
                #     pyextrae.eventandcounters(TASK_EVENTS, 0)
                #     pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
    # ==========================================================================
    except Exception, e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("WORKER EXCEPTION")
        logger.exception(''.join(line for line in lines))

        print("[PYTHON WORKER] Error: Worker Exception", e)
        print("[PYTHON WORKER] End task execution.")
        return 1

    # EVERYTHING OK
    print("[PYTHON WORKER] End task execution. Status: Ok")

    # if tracing:
    #     pyextrae.eventandcounters(TASK_EVENTS, 0)

    return 0


def shutdown_handler(signal, frame):
    for proc in processes:
        if proc.is_alive():
            proc.terminate()


######################
# Main method
######################

def compss_persistent_worker():
    print("[PYTHON WORKER] Piper wake up")

    # Get args  
    debug = (sys.argv[1] == 'true')
    tracing = (sys.argv[2] == 'true')
    tasks_x_node = int(sys.argv[3])
    in_pipes = sys.argv[4:4 + tasks_x_node]
    out_pipes = sys.argv[4 + tasks_x_node:]

    print("-----------")
    print("Parameters:")
    print("-----------")
    print("Debug          : ", debug)
    print("Tracing        : ", tracing)
    print("Tasks per node : ", tasks_x_node)
    print("In Pipes       : ", in_pipes)
    print("Out Pipes      : ", out_pipes)

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

    # Create new threads
    queues = []
    for i in xrange(0, tasks_x_node):
        print("[PYTHON WORKER] Launching process ", i)
        process_name = 'Process-' + str(i)
        queues.append(Queue())
        processes.append(Process(target=worker, args=(queues[i],
                                                      process_name,
                                                      in_pipes[i],
                                                      out_pipes[i])))
        processes[i].start()

    # Catch SIGTERM send by bindings_piper to exit all processes
    signal.signal(signal.SIGTERM, shutdown_handler)

    # Wait for all threads
    for i in xrange(0, tasks_x_node):
        processes[i].join()

    # Check if there is any exception message from the threads
    for i in xrange(0, tasks_x_node):
        if not queues[i].empty:
            print(queues[i].get())

    print("[PYTHON WORKER] Finished")


############################
# Main -> Calls main method
############################

if __name__ == '__main__':
    compss_persistent_worker()
