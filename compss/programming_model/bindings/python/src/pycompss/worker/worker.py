"""
@author: etejedor
@author: fconejer

PyCOMPSs Worker
===============
    This file contains the worker code.
    Args: debug full_path (method_class)
    method_name has_target num_params par_type_1 par_1 ... par_type_n par_n
"""

import os
import sys
import traceback
import logging

from pycompss.util.logs import init_logging_worker
from pycompss.api.parameter import Type, JAVA_MAX_INT, JAVA_MIN_INT

from cPickle import loads, UnpicklingError
from exceptions import ValueError

SYNC_EVENTS = 8000666

# Should be equal to Tracer.java definitions
TASK_EVENTS = 8000010

PROCESS_CREATION = 100
WORKER_INITIALIZATION = 102
PARAMETER_PROCESSING = 103
LOGGING = 104
TASK_EXECUTION = 105
WORKER_END = 106
PROCESS_DESTRUCTION = 107


if sys.version_info >= (2, 7):
    import importlib

try:
    # Import storage libraries if possible
    from storage.api import getByID
    from storage.api import TaskContext
    #from storage.api import start_task, end_task
except ImportError:
    # If not present, import dummy functions
    from dummy.storage import getByID
    from dummy.storage import TaskContext
    #from dummy.storage import start_task
    #from dummy.storage import end_task

# Uncomment the next line if you do not want to reuse pyc files.
# sys.dont_write_bytecode = True


def compss_worker():
    """
    Worker main method (invocated from __main__).
    """
    logger = logging.getLogger('pycompss.worker.worker')

    logger.debug("Starting Worker")

    args = sys.argv[1:]

    # verbose = args[0]
    path = args[1]
    method_name = args[2]
    has_target = args[3]
    num_params = int(args[4])

    args = args[5:]
    pos = 0
    values = []
    types = []
    if tracing:
        pyextrae.event(TASK_EVENTS, 0)
        pyextrae.event(TASK_EVENTS, PARAMETER_PROCESSING)
    # Get all parameter values
    for i in range(0, num_params):
        ptype = int(args[pos])
        types.append(ptype)

        if ptype == Type.FILE:
            values.append(args[pos + 1])
        elif (ptype == Type.PERSISTENT):
            po = getByID(args[pos+1])
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
            exit(1)

        pos += 2
    if tracing:
        pyextrae.event(TASK_EVENTS, 0)
        pyextrae.event(TASK_EVENTS, LOGGING)
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
            if tracing:
                pyextrae.eventandcounters(TASK_EVENTS, 0)
                pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)
            getattr(module, method_name)(*values, compss_types=types)
            if tracing:
                pyextrae.eventandcounters(TASK_EVENTS, 0)
                pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
        '''
        # Old school
        # Storage Prolog
        start_task(values)
        # Task execution
        getattr(module, method_name)(*values, compss_types=types)
        # Storage Epilog
        end_task(values)
        '''
    # ==========================================================================
    except AttributeError:
        # Appears with functions that have not been well defined.
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("WORKER EXCEPTION - Attribute Error Exception")
        logger.exception(''.join(line for line in lines))
        logger.exception("Check that all parameters have been defined with " +
                         "an absolute import path (even if in the same file)")
        exit(1)
    # ==========================================================================
    except ImportError:
        from pycompss.util.serializer import deserialize_from_file
        from pycompss.util.serializer import serialize_to_file
        # Not the path of a module, it ends with a class name
        class_name = path.split('.')[-1]
        # module_name = path.replace('.' + class_name, '')  # BUG - does not support same filename as a package
        module_name = '.'.join(path.split('.')[0:-1])       # SOLUTION - all path but the class_name means the module_name
        if '.' in path:
            module_name = '.'.join(path.split('.')[0:-1])   # SOLUTION - all path but the class_name means the module_name
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
                if tracing:
                    pyextrae.eventandcounters(TASK_EVENTS, 0)
                    pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)
                getattr(klass, method_name)(*values, compss_types=types)
                if tracing:
                    pyextrae.eventandcounters(TASK_EVENTS, 0)
                    pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
            ''' Old school
            # Storage Prolog
            start_task(values)
            # Task execution
            getattr(klass, method_name)(*values, compss_types=types)
            #  Storage Epilog
            end_task(values)
            '''
            serialize_to_file(obj, file_name, force=True)
        else:
            # Class method - class is not included in values (e.g. values = [7])
            types.insert(0, None)    # class must be first type

            with TaskContext(logger, values):
                if tracing:
                    pyextrae.eventandcounters(TASK_EVENTS, 0)
                    pyextrae.eventandcounters(TASK_EVENTS, TASK_EXECUTION)
                getattr(klass, method_name)(*values, compss_types=types)
                if tracing:
                    pyextrae.eventandcounters(TASK_EVENTS, 0)
                    pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
            ''' Old school
            # Storage Prolog
            start_task(values)
            # Task execution
            getattr(klass, method_name)(*values, compss_types=types)
            #  Storage Epilog
            end_task(values)
            '''
    # ==========================================================================
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("WORKER EXCEPTION")
        logger.exception(''.join(line for line in lines))
        exit(1)


if __name__ == "__main__":

    # Emit sync event if tracing is enabled
    tracing = sys.argv[1] == 'true'
    taskId = int(sys.argv[2])

    sys.argv = sys.argv[2:]

    if tracing:
        import pyextrae
        pyextrae.eventandcounters(SYNC_EVENTS, taskId)
        # pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(TASK_EVENTS, WORKER_INITIALIZATION)


    # Load log level configuration file
    log_level = sys.argv[1]
    if log_level == 'true' or log_level == "debug":
        init_logging_worker(os.path.realpath(__file__) +
                            '/../../log/logging.json.debug')
    elif log_level == "info" or log_level == "off":
        init_logging_worker(os.path.realpath(__file__) +
                            '/../../log/logging.json.off')
    else:
        # Default
        init_logging_worker(os.path.realpath(__file__) +
                            '/../../log/logging.json')

    # Init worker
    compss_worker()
    if tracing:
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        # pyextrae.eventandcounters(TASK_EVENTS, PROCESS_DESTRUCTION)
        pyextrae.eventandcounters(SYNC_EVENTS, taskId)
