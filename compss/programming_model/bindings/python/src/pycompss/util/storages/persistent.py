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
PyCOMPSs Utils - External Storage
=================================
    This file contains the methods required to manage PSCOs.
    Isolates the API signature calls.
"""

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.tracing.helpers import emit_event
from pycompss.runtime.constants import INIT_STORAGE_EVENT as MASTER_INIT_STORAGE_EVENT  # noqa: E501
from pycompss.runtime.constants import STOP_STORAGE_EVENT as MASTER_STOP_STORAGE_EVENT  # noqa: E501
from pycompss.worker.commons.constants import INIT_STORAGE_EVENT
from pycompss.worker.commons.constants import STOP_STORAGE_EVENT

# Globals
# Contain the actual storage api functions set on initialization
INIT = None
FINISH = None
get_by_id = None
storage_task_context = None


def load_storage_library():  # noqa
    # type: () -> None
    """ Import the propper storage libraries

    :return: None
    """
    global INIT
    global FINISH
    global get_by_id
    global storage_task_context

    try:
        # Try to import the external storage API module methods
        from storage.api import init  # noqa
        from storage.api import finish  # noqa
        from storage.api import getByID as get_by_id_from_storage  # noqa
        from storage.api import TaskContext  # noqa
        print("INFO: Storage API successfully imported.")
    except ImportError as e:
        # print("INFO: No storage API defined.")
        # Defined methods throwing exceptions.
        ERROR_MSG = e

        def init(config_file_path=None):  # noqa
            # type: (str) -> None
            raise PyCOMPSsException('Unexpected call to init from storage. Reason: %s' %
                                    ERROR_MSG)

        def finish():
            # type: () -> None
            raise PyCOMPSsException('Unexpected call to finish from storage. Reason: %s' %
                                    ERROR_MSG)

        def get_by_id_from_storage(id):  # noqa
            # type: (str) -> None
            raise PyCOMPSsException('Unexpected call to getByID. Reason: %s' % ERROR_MSG)

        class TaskContext(object):
            """
            Dummy task context to be used with storage frameworks.
            """

            def __init__(self, logger, values, config_file_path=None):
                self.logger = logger
                err_msg = 'Unexpected call to dummy storage task context. ' \
                          'Reason: %s' % ERROR_MSG
                self.logger.error(err_msg)
                self.values = values
                self.config_file_path = config_file_path
                raise PyCOMPSsException(err_msg)

            def __enter__(self):
                # Ready to start the task
                err_msg = 'Unexpected call to dummy storage task context __enter__'
                self.logger.error(err_msg)
                raise PyCOMPSsException(err_msg)

            def __exit__(self, type, value, traceback):
                # Task finished
                err_msg = 'Unexpected call to dummy storage task context __exit__'
                self.logger.error(err_msg)
                raise PyCOMPSsException(err_msg)

    # Prepare the imports
    INIT = init
    FINISH = finish
    get_by_id = get_by_id_from_storage
    storage_task_context = TaskContext  # Renamed for importing it from the worker


def is_psco(obj):
    # type: (object) -> bool
    """ Checks if obj is a persistent object (external storage).

    :param obj: Object to check.
    :return: True if is persistent object. False otherwise.
    """
    # Check from storage object requires a dummy storage object class
    # from storage.storage_object import storage_object
    # return issubclass(obj.__class__, storage_object) and
    #        get_id(obj) not in [None, 'None']
    return has_id(obj) and get_id(obj) not in [None, 'None']


def has_id(obj):
    # type: (object) -> bool
    """ Checks if the object has a getID method.

    :param obj: Object to check.
    :return: True if is persistent object. False otherwise.
    """
    if 'getID' in dir(obj):
        return True
    else:
        return False


def get_id(psco):
    # type: (...) -> str
    """ Retrieve the persistent object identifier.

    :param psco: Persistent object.
    :return: Persistent object identifier.
    """
    return psco.getID()


def get_by_id(identifier):
    # type: (str) -> object
    """ Retrieve the actual object from a persistent object identifier.

    :param identifier: Persistent object identifier.
    :return: The object that corresponds to the id.
    """
    return get_by_id(identifier)  # noqa


@emit_event(MASTER_INIT_STORAGE_EVENT, master=True)
def master_init_storage(storage_conf, logger):  # noqa
    # type: (str, ...) -> bool
    """ Call to init storage from the master.

    This function emits the event in the master.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    return __init_storage__(storage_conf, logger)


@emit_event(INIT_STORAGE_EVENT)
def init_storage(storage_conf, logger):  # noqa
    # type: (str, ...) -> bool
    """ Call to init storage.

    This function emits the event in the worker.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    return __init_storage__(storage_conf, logger)


def __init_storage__(storage_conf, logger):  # noqa
    # type: (str, ...) -> bool
    """ Call to init storage.

    Initializes the persistent storage with the given storage_conf file.
    The storage will be initialized if storage_conf is not None nor 'null'.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    global INIT
    if storage_conf is not None and not storage_conf == 'null':
        if __debug__:
            logger.debug("Starting storage")
            logger.debug("Storage configuration file: %s" % storage_conf)
        load_storage_library()
        # Call to storage init
        INIT(config_file_path=storage_conf)  # noqa
        return True
    else:
        return False


@emit_event(MASTER_STOP_STORAGE_EVENT, master=True)
def master_stop_storage(logger):
    # type: (...) -> None
    """ Stops the persistent storage.

    This function emits the event in the master.

    :param logger: Logger where to log the messages.
    :return: None
    """
    __stop_storage__(logger)


@emit_event(STOP_STORAGE_EVENT)
def stop_storage(logger):
    # type: (...) -> None
    """ Stops the persistent storage.

    This function emits the event in the worker.

    :param logger: Logger where to log the messages.
    :return: None
    """
    __stop_storage__(logger)


def __stop_storage__(logger):  # noqa
    # type: (...) -> None
    """ Stops the persistent storage.

    :param logger: Logger where to log the messages.
    :return: None
    """
    global FINISH
    if __debug__:
        logger.debug("Stopping storage")
    FINISH()  # noqa
