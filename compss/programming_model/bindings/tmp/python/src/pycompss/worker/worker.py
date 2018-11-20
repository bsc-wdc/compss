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
PyCOMPSs Worker
===============
    This file contains the worker code.
    Args: debug full_path (method_class)
    method_name has_target num_params par_type_1 par_1 ... par_type_n par_n
"""

import logging
import os
import sys
import traceback
import base64

from pycompss.api.parameter import TYPE, JAVA_MAX_INT, JAVA_MIN_INT
from pycompss.runtime.commons import EMPTY_STRING_KEY
from pycompss.runtime.commons import STR_ESCAPE
from pycompss.runtime.commons import IS_PYTHON3
from pycompss.util.serializer import serialize_to_file
from pycompss.util.serializer import deserialize_from_file
from pycompss.util.serializer import deserialize_from_string
from pycompss.util.serializer import SerializerException
from pycompss.util.logs import init_logging_worker
from pycompss.util.persistent_storage import get_by_id

if IS_PYTHON3:
    long = int
else:
    # Exception moved to built-in
    from exceptions import ValueError

    str_escape = 'string_escape'

SYNC_EVENTS = 8000666

# Should be equal to Tracer.java definitions
TASK_EVENTS = 60000100

# Rank 110-119 reserved to events launched from task.py
PROCESS_CREATION = 100
WORKER_INITIALIZATION = 102
PARAMETER_PROCESSING = 103
LOGGING = 104
MODULES_IMPORT = 105
WORKER_END = 106
PROCESS_DESTRUCTION = 107

if sys.version_info >= (2, 7):
    import importlib


# Uncomment the next line if you do not want to reuse pyc files.
# sys.dont_write_bytecode = True


def compss_worker(persistent_storage):
    """
    Worker main method (invocated from __main__).

    :param persistent_storage: Persistent storage boolean
    :return: None
    """

    logger = logging.getLogger('pycompss.worker.worker')

    logger.debug("Starting Worker")

    # Set the binding in worker mode
    import pycompss.util.context as context
    context.set_pycompss_context(context.WORKER)

    args = sys.argv[6:]

    path = args[0]
    method_name = args[1]

    num_slaves = int(args[2])
    slaves = []
    for i in range(2, 2 + num_slaves):
        slaves.append(args[i])
    arg_position = 3 + num_slaves

    args = args[arg_position:]
    cus = args[0]

    args = args[1:]
    has_target = args[0]
    return_type = args[1]
    num_returns = int(args[2])
    num_params = int(args[3])

    args = args[4:]

    # COMPSs keywords for tasks (ie: tracing, process name...)
    compss_kwargs = {
        'compss_tracing': tracing,
        'compss_process_name': "GAT",
        'compss_storage_conf': storage_conf,
        'compss_return_length': num_returns
    }

    values = []
    types = []
    streams = []
    prefixes = []

    if tracing:
        pyextrae.event(TASK_EVENTS, 0)
        pyextrae.event(TASK_EVENTS, PARAMETER_PROCESSING)

    if persistent_storage:
        from pycompss.util.persistent_storage import storage_task_context

    # Get all parameter values
    logger.debug("Processing parameters:")
    pos = 0
    for i in range(0, num_params):
        p_type = int(args[pos])
        p_stream = int(args[pos + 1])
        p_prefix = args[pos + 2]
        p_value = args[pos + 3]

        logger.debug("Parameter : " + str(i))
        logger.debug("\t * Type : " + str(p_type))
        logger.debug("\t * Stream : " + str(p_stream))
        logger.debug("\t * Prefix : " + str(p_prefix))
        logger.debug("\t * Value: " + str(p_value))

        types.append(p_type)
        streams.append(p_stream)
        prefixes.append(p_prefix)

        if p_type == TYPE.FILE:
            values.append(p_value)
        elif p_type == TYPE.EXTERNAL_PSCO:
            po = get_by_id(p_value)
            values.append(po)
            pos += 1  # Skip info about direction (R, W)
        elif p_type == TYPE.STRING:
            num_substrings = int(p_value)
            aux = ''
            first_substring = True
            for j in range(4, num_substrings + 4):
                if not first_substring:
                    aux += ' '
                first_substring = False
                aux += args[pos + j]
            # Decode the string received
            aux = base64.b64decode(aux.encode())
            if aux.decode() == EMPTY_STRING_KEY:
                # Then it is an empty string
                aux = ""
            else:
                #######
                # Check if the string is really an object
                # Required in order to recover objects passed as parameters.
                # - Option object_conversion
                real_value = aux
                try:
                    # try to recover the real object
                    if IS_PYTHON3:
                        # decode removes double backslash, and encode returns as binary
                        name, aux = deserialize_from_string(aux.decode(STR_ESCAPE).encode())
                    else:
                        # decode removes double backslash, and str casts the output
                        name, aux = deserialize_from_string(str(aux.decode(STR_ESCAPE)))
                except (SerializerException, ValueError, EOFError):
                    # was not an object
                    aux = str(real_value.decode())
                    #######
            values.append(aux)
            logger.debug("\t * Final Value: " + str(aux))
            pos += num_substrings
        elif p_type == TYPE.INT:
            values.append(int(p_value))
        elif p_type == TYPE.LONG:
            my_l = long(p_value)
            if my_l > JAVA_MAX_INT or my_l < JAVA_MIN_INT:
                # A Python int was converted to a Java long to prevent overflow
                # We are sure we will not overflow Python int, otherwise this
                # would have been passed as a serialized object.
                my_l = int(my_l)
            values.append(my_l)
        elif p_type == TYPE.DOUBLE:
            values.append(float(p_value))
        elif p_type == TYPE.BOOLEAN:
            if p_value == 'true':
                values.append(True)
            else:
                values.append(False)
        # elif (p_type == TYPE.OBJECT):
        #    pass
        else:
            logger.fatal("Invalid type (%d) for parameter %d" % (p_type, i))
            exit(1)
        pos += 4

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

    if tracing:
        pyextrae.event(TASK_EVENTS, 0)
        pyextrae.event(TASK_EVENTS, MODULES_IMPORT)

    try:
        # Try to import the module (for functions)
        logger.debug("Trying to import the user module.")
        if sys.version_info >= (2, 7):
            module = importlib.import_module(path)  # Python 2.7
            logger.debug("Module successfully loaded (Python version >= 2.7)")
        else:
            module = __import__(path, globals(), locals(), [path], -1)
            logger.debug("Module successfully loaded (Python version < 2.7")

        if persistent_storage:
            with storage_task_context(logger, values, config_file_path=storage_conf):
                getattr(module, method_name)(*values, compss_types=types, **compss_kwargs)
                if tracing:
                    pyextrae.eventandcounters(TASK_EVENTS, 0)
                    pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
        else:
            getattr(module, method_name)(*values, compss_types=types, **compss_kwargs)
            if tracing:
                pyextrae.eventandcounters(TASK_EVENTS, 0)
                pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
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
        logger.debug("Method in class %s of module %s" % (class_name, module_name))

        if has_target == 'true':
            # Instance method
            file_name = values.pop()
            logger.debug("Deserialize self from file.")
            obj = deserialize_from_file(file_name)

            logger.debug("Processing callee, a hidden object of %s in file %s" % (file_name, type(obj)))
            values.insert(0, obj)
            types.pop()
            types.insert(0, TYPE.OBJECT)

            if persistent_storage:
                with storage_task_context(logger, values, config_file_path=storage_conf):
                    getattr(klass, method_name)(*values, compss_types=types, **compss_kwargs)
                    if tracing:
                        pyextrae.eventandcounters(TASK_EVENTS, 0)
                        pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
            else:
                getattr(klass, method_name)(*values, compss_types=types, **compss_kwargs)
                if tracing:
                    pyextrae.eventandcounters(TASK_EVENTS, 0)
                    pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)

            logger.debug("Serializing self to file")
            logger.debug("Obj: " + str(obj))
            serialize_to_file(obj, file_name)
        else:
            # Class method - class is not included in values (e.g. values = [7])
            types.insert(0, None)  # class must be first type

            if persistent_storage:
                with storage_task_context(logger, values, config_file_path=storage_conf):
                    getattr(klass, method_name)(*values, compss_types=types, **compss_kwargs)
                    if tracing:
                        pyextrae.eventandcounters(TASK_EVENTS, 0)
                        pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
            else:
                getattr(klass, method_name)(*values, compss_types=types, **compss_kwargs)
                if tracing:
                    pyextrae.eventandcounters(TASK_EVENTS, 0)
                    pyextrae.eventandcounters(TASK_EVENTS, WORKER_END)
    # ==========================================================================
    except Exception:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.exception("WORKER EXCEPTION")
        logger.exception(''.join(line for line in lines))
        exit(1)


def main():
    # Emit sync event if tracing is enabled
    tracing = sys.argv[1] == 'true'
    taskId = int(sys.argv[2])
    log_level = sys.argv[3]
    storage_conf = sys.argv[4]
    method_type = sys.argv[5]
    # class_name = sys.argv[6]
    # method_name = sys.argv[7]
    # num_slaves = sys.argv[8]
    # i = 8 + num_slaves
    # slaves = sys.argv[9-i]
    # numCus = sys.argv[i+1]
    # has_target = sys.argv[i+2] == 'true'
    # num_params = int(sys.argv[i+3])
    # params = sys.argv[i+4..]

    persistent_storage = False
    if storage_conf != 'null':
        persistent_storage = True
        from storage.api import initWorker as initStorageAtWorker
        from storage.api import finishWorker as finishStorageAtWorker

    if tracing:
        import pyextrae.multiprocessing as pyextrae

        pyextrae.eventandcounters(SYNC_EVENTS, taskId)
        # pyextrae.eventandcounters(TASK_EVENTS, 0)
        pyextrae.eventandcounters(TASK_EVENTS, WORKER_INITIALIZATION)

    # Load log level configuration file
    worker_path = os.path.dirname(os.path.realpath(__file__))
    if log_level == 'true' or log_level == "debug":
        # Debug
        init_logging_worker(worker_path + '/../../log/logging.json.debug')
    elif log_level == "info" or log_level == "off":
        # Info or no debug
        init_logging_worker(worker_path + '/../../log/logging.json.off')
    else:
        # Default
        init_logging_worker(worker_path + '/../../log/logging.json')

    if persistent_storage:
        # Initialize storage
        initStorageAtWorker(config_file_path=storage_conf)

    # Init worker
    compss_worker(persistent_storage)

    if tracing:
        pyextrae.eventandcounters(TASK_EVENTS, 0)
        # pyextrae.eventandcounters(TASK_EVENTS, PROCESS_DESTRUCTION)
        pyextrae.eventandcounters(SYNC_EVENTS, taskId)

    if persistent_storage:
        # Finish storage
        finishStorageAtWorker()


if __name__ == '__main__':
    main()
