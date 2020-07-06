#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Binding - Link
=======================
    This file contains the functions to link with the binding-commons.
    In particular, manages a separate process which handles the compss
    extension, so that the process can be removed when shutting off
    and restarted (interactive usage of PyCOMPSs - ipython and jupyter).
"""

import multiprocessing
import logging

# Global variables
LINK_PROCESS = multiprocessing.Process()
IN_QUEUE = multiprocessing.Queue()
OUT_QUEUE = multiprocessing.Queue()
RELOAD = False

# Queue messages
START = 'START'
SET_DEBUG = 'SET_DEBUG'
STOP = 'STOP'
CANCEL_TASKS = 'CANCEL_TASKS'
ACCESSED_FILE = 'ACCESSED_FILE'
OPEN_FILE = 'OPEN_FILE'
CLOSE_FILE = 'CLOSE_FILE'
DELETE_FILE = 'DELETE_FILE'
GET_FILE = 'GET_FILE'
GET_DIRECTORY = 'GET_DIRECTORY'
BARRIER = 'BARRIER'
BARRIER_GROUP = 'BARRIER_GROUP'
OPEN_TASK_GROUP = 'OPEN_TASK_GROUP'
CLOSE_TASK_GROUP = 'CLOSE_TASK_GROUP'
GET_LOGGING_PATH = 'GET_LOGGING_PATH'
GET_NUMBER_OF_RESOURCES = 'GET_NUMBER_OF_RESOURCES'
REQUEST_RESOURCES = 'REQUEST_RESOURCES'
FREE_RESOURCES = 'FREE_RESOURCES'
REGISTER_CORE_ELEMENT = 'REGISTER_CORE_ELEMENT'
PROCESS_TASK = 'PROCESS_TASK'

# Setup logger
logger = logging.getLogger(__name__)


def shutdown_handler(signal, frame):  # noqa
    """
    Shutdown handler (do not remove the parameters).

    :param signal: shutdown signal
    :param frame: Frame
    :return: None
    """
    if LINK_PROCESS.is_alive():
        LINK_PROCESS.terminate()


def c_extension_link(in_queue, out_queue):
    """
    Main C extension process

    :param in_queue: Queue to receive messages
    :param out_queue: Queue to send messages
    :return: None
    """
    import compss

    alive = True
    while alive:
        message = in_queue.get()
        command = message[0]
        parameters = []
        if len(message) > 0:
            parameters = message[1:]
        if command == START:
            compss.start_runtime()
        elif command == SET_DEBUG:
            compss.set_debug(*parameters)
        elif command == STOP:
            compss.stop_runtime(*parameters)
            alive = False
        elif command == CANCEL_TASKS:
            compss.cancel_application_tasks(*parameters)
        elif command == ACCESSED_FILE:
            accessed = compss.accessed_file(*parameters)
            out_queue.put(accessed)
        elif command == OPEN_FILE:
            compss_name = compss.open_file(*parameters)
            out_queue.put(compss_name)
        elif command == CLOSE_FILE:
            compss.close_file(*parameters)
        elif command == DELETE_FILE:
            result = compss.delete_file(*parameters)
            out_queue.put(result)
        elif command == GET_FILE:
            compss.get_file(*parameters)
        elif command == GET_DIRECTORY:
            compss.get_directory(*parameters)
        elif command == BARRIER:
            compss.barrier(*parameters)
        elif command == BARRIER_GROUP:
            exception_message = compss.barrier_group(*parameters)
            out_queue.put(exception_message)
        elif command == OPEN_TASK_GROUP:
            compss.open_task_group(*parameters)
        elif command == CLOSE_TASK_GROUP:
            compss.close_task_group(*parameters)
        elif command == GET_LOGGING_PATH:
            log_path = compss.get_logging_path()
            out_queue.put(log_path)
        elif command == GET_NUMBER_OF_RESOURCES:
            num_resources = compss.get_number_of_resources(*parameters)
            out_queue.put(num_resources)
        elif command == REQUEST_RESOURCES:
            compss.request_resources(*parameters)
        elif command == FREE_RESOURCES:
            compss.free_resources(*parameters)
        elif command == REGISTER_CORE_ELEMENT:
            compss.register_core_element(*parameters)
        elif command == PROCESS_TASK:
            compss.process_task(*parameters)
        else:
            raise Exception("Unknown link command")


def establish_link():
    """
    Loads the compss C extension within the same process.

    :return: the COMPSs C extension link
    """
    if __debug__:
        logger.debug("Loading compss extension")
    import compss
    if __debug__:
        logger.debug("Loaded compss extension")
    return compss


def establish_interactive_link():
    """
    Starts a new process which will be in charge of communicating with the
    C-extension.

    :return: the COMPSs C extension link
    """
    global LINK_PROCESS
    global IN_QUEUE
    global OUT_QUEUE
    global RELOAD

    if RELOAD:
        IN_QUEUE = multiprocessing.Queue()
        OUT_QUEUE = multiprocessing.Queue()
        RELOAD = False

    if __debug__:
        logger.debug("Starting new process linking with the C-extension")

    LINK_PROCESS = multiprocessing.Process(target=c_extension_link,
                                           args=(IN_QUEUE, OUT_QUEUE))
    LINK_PROCESS.start()

    if __debug__:
        logger.debug("Established link with C-extension")

    compss_link = COMPSs  # object that mimics compss library
    return compss_link


def wait_for_interactive_link():
    """
    Wait for interactive link finalization

    :return: None
    """
    global RELOAD

    # Wait for the link to finish
    IN_QUEUE.close()
    OUT_QUEUE.close()
    IN_QUEUE.join_thread()
    OUT_QUEUE.join_thread()
    LINK_PROCESS.join()

    # Notify that if terminated, the queues must be reopened to start again.
    RELOAD = True


def terminate_interactive_link():
    """
    Terminate the compss C extension process

    :return: None
    """
    LINK_PROCESS.terminate()
    wait_for_interactive_link()


class COMPSs(object):
    """
    Class that mimics the compss extension library.
    Each function puts into the queue a list or set composed by:
         (COMMAND_TAG, parameter1, parameter2, ...)

    IMPORTANT: methods must be exactly the same.
    """

    @staticmethod
    def start_runtime():
        IN_QUEUE.put([START])

    @staticmethod
    def set_debug(mode):
        IN_QUEUE.put((SET_DEBUG, mode))

    @staticmethod
    def stop_runtime(code):
        IN_QUEUE.put([STOP, code])
        wait_for_interactive_link()
        # terminate_interactive_link()

    @staticmethod
    def cancel_application_tasks(value):
        IN_QUEUE.put((CANCEL_TASKS, value))

    @staticmethod
    def accessed_file(file_name):
        IN_QUEUE.put((ACCESSED_FILE, file_name))
        accessed = OUT_QUEUE.get(block=True)
        return accessed

    @staticmethod
    def open_file(file_name, mode):
        IN_QUEUE.put((OPEN_FILE, file_name, mode))
        compss_name = OUT_QUEUE.get(block=True)
        return compss_name

    @staticmethod
    def close_file(file_name, mode):
        IN_QUEUE.put((CLOSE_FILE, file_name, mode))

    @staticmethod
    def delete_file(file_name, mode):
        IN_QUEUE.put((DELETE_FILE, file_name, mode))
        result = OUT_QUEUE.get(block=True)
        return result

    @staticmethod
    def get_file(app_id, file_name):
        IN_QUEUE.put((GET_FILE, app_id, file_name))

    @staticmethod
    def get_directory(app_id, file_name):
        IN_QUEUE.put((GET_DIRECTORY, app_id, file_name))

    @staticmethod
    def barrier(app_id, no_more_tasks):
        IN_QUEUE.put((BARRIER, app_id, no_more_tasks))

    @staticmethod
    def barrier_group(app_id, group_name):
        IN_QUEUE.put((BARRIER_GROUP, app_id, group_name))
        exception_message = OUT_QUEUE.get(block=True)
        return exception_message

    @staticmethod
    def open_task_group(group_name, implicit_barrier, mode):
        IN_QUEUE.put((OPEN_TASK_GROUP, group_name, implicit_barrier, mode))

    @staticmethod
    def close_task_group(group_name, mode):
        IN_QUEUE.put((CLOSE_TASK_GROUP, group_name, mode))

    @staticmethod
    def get_logging_path():
        IN_QUEUE.put([GET_LOGGING_PATH])
        log_path = OUT_QUEUE.get(block=True)
        return log_path

    @staticmethod
    def get_number_of_resources(app_id):
        IN_QUEUE.put((GET_NUMBER_OF_RESOURCES, app_id))
        num_resources = OUT_QUEUE.get(block=True)
        return num_resources

    @staticmethod
    def request_resources(app_id, num_resources, group_name):
        IN_QUEUE.put((REQUEST_RESOURCES, app_id, num_resources, group_name))

    @staticmethod
    def free_resources(app_id, num_resources, group_name):
        IN_QUEUE.put((FREE_RESOURCES, app_id, num_resources, group_name))

    @staticmethod
    def register_core_element(ce_signature,
                              impl_signature,
                              impl_constraints_str,
                              impl_type,
                              impl_io,
                              impl_type_args):
        IN_QUEUE.put((REGISTER_CORE_ELEMENT,
                      ce_signature,
                      impl_signature,
                      impl_constraints_str,
                      impl_type,
                      impl_io,
                      impl_type_args))

    @staticmethod
    def process_task(app_id,
                     signature,
                     on_failure,
                     time_out,
                     has_priority,
                     num_nodes,
                     replicated,
                     distributed,
                     has_target,
                     num_returns,
                     values,
                     names,
                     compss_types,
                     compss_directions,
                     compss_streams,
                     compss_prefixes,
                     content_types,
                     weights,
                     keep_renames):
        IN_QUEUE.put((PROCESS_TASK,
                      app_id,
                      signature,
                      on_failure,
                      time_out,
                      has_priority,
                      num_nodes,
                      replicated,
                      distributed,
                      has_target,
                      num_returns,
                      values,
                      names,
                      compss_types,
                      compss_directions,
                      compss_streams,
                      compss_prefixes,
                      content_types,
                      weights,
                      keep_renames))
