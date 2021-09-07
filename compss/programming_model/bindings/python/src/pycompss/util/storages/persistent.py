#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
from pycompss.worker.commons.constants import GET_BY_ID_EVENT
from pycompss.worker.commons.constants import GETID_EVENT
from pycompss.worker.commons.constants import MAKE_PERSISTENT_EVENT
from pycompss.worker.commons.constants import DELETE_PERSISTENT_EVENT
from pycompss.runtime.constants import INIT_STORAGE_EVENT as MASTER_INIT_STORAGE_EVENT  # noqa: E501
from pycompss.runtime.constants import STOP_STORAGE_EVENT as MASTER_STOP_STORAGE_EVENT  # noqa: E501
from pycompss.worker.commons.constants import INIT_STORAGE_EVENT
from pycompss.worker.commons.constants import STOP_STORAGE_EVENT

# Globals
# Contain the actual storage api functions set on initialization
INIT = None
FINISH = None
GET_BY_ID = None
TaskContext = None
DUMMY_STORAGE = False


class dummy_task_context(object):
    """
    Dummy task context to be used with storage frameworks.
    """

    def __init__(self, logger, values, config_file_path=None):
        self.logger = logger
        err_msg = "Unexpected call to dummy storage task context."
        self.logger.error(err_msg)
        self.values = values
        self.config_file_path = config_file_path
        raise PyCOMPSsException(err_msg)

    def __enter__(self):
        # Ready to start the task
        err_msg = "Unexpected call to dummy storage task context __enter__"
        self.logger.error(err_msg)
        raise PyCOMPSsException(err_msg)

    def __exit__(self, type, value, traceback):
        # Task finished
        err_msg = "Unexpected call to dummy storage task context __exit__"
        self.logger.error(err_msg)
        raise PyCOMPSsException(err_msg)


def load_storage_library():  # noqa
    """ Import the proper storage libraries

    :return: None
    """
    global INIT
    global FINISH
    global GET_BY_ID
    global TaskContext
    global DUMMY_STORAGE
    error_msg = "UNDEFINED"

    def dummy_init(config_file_path=None):  # noqa
        raise PyCOMPSsException("Unexpected call to init from storage. Reason: %s" %
                                error_msg)

    def dummy_finish():
        raise PyCOMPSsException("Unexpected call to finish from storage. Reason: %s" %
                                error_msg)

    def dummy_get_by_id(id):  # noqa
        raise PyCOMPSsException("Unexpected call to getByID. Reason: %s" % error_msg)

    try:
        # Try to import the external storage API module methods
        from storage.api import init as real_init                 # noqa
        from storage.api import finish as real_finish             # noqa
        from storage.api import getByID as real_get_by_id         # noqa
        from storage.api import TaskContext as real_task_context  # noqa
        DUMMY_STORAGE = False
        print("INFO: Storage API successfully imported.")
    except ImportError as e:
        # print("INFO: No storage API defined.")
        # Defined methods throwing exceptions.
        error_msg = str(e)
        DUMMY_STORAGE = True

    # Prepare the imports
    if DUMMY_STORAGE:
        INIT = dummy_init
        FINISH = dummy_finish
        GET_BY_ID = dummy_get_by_id
        TaskContext = dummy_task_context
    else:
        INIT = real_init
        FINISH = real_finish
        GET_BY_ID = real_get_by_id
        TaskContext = real_task_context


def is_psco(obj):
    """ Checks if obj is a persistent object (external storage).

    :param obj: Object to check.
    :return: True if is persistent object. False otherwise.
    """
    # Check from storage object requires a dummy storage object class
    # from storage.storage_object import storage_object
    # return issubclass(obj.__class__, storage_object) and
    #        get_id(obj) not in [None, "None"]
    return has_id(obj) and get_id(obj) not in [None, "None"]


def has_id(obj):
    """ Checks if the object has a getID method.

    :param obj: Object to check.
    :return: True if is persistent object. False otherwise.
    """
    if "getID" in dir(obj):
        return True
    else:
        return False


@emit_event(GETID_EVENT, master=False, inside=True)
def get_id(psco):
    """ Retrieve the persistent object identifier.

    :param psco: Persistent object.
    :return: Persistent object identifier.
    """
    return psco.getID()


@emit_event(GET_BY_ID_EVENT, master=False, inside=True)
def get_by_id(identifier):
    """ Retrieve the actual object from a persistent object identifier.

    :param identifier: Persistent object identifier.
    :return: The object that corresponds to the id.
    """
    return GET_BY_ID(identifier)  # noqa


@emit_event(MASTER_INIT_STORAGE_EVENT, master=True)
def master_init_storage(storage_conf, logger):  # noqa
    """ Call to init storage from the master.

    This function emits the event in the master.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    return __init_storage__(storage_conf, logger)


def use_storage(storage_conf):
    """ Evaluates if the storage_conf is defined.
    The storage will be used if storage_conf is not None nor "null".

    :param storage_conf: Storage configuration file.
    :return: True if defined. False on the contrary.
    """
    return storage_conf != "" and not storage_conf == "null"


@emit_event(INIT_STORAGE_EVENT)
def init_storage(storage_conf, logger):  # noqa
    """ Call to init storage.

    This function emits the event in the worker.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    return __init_storage__(storage_conf, logger)


def __init_storage__(storage_conf, logger):  # noqa
    """ Call to init storage.

    Initializes the persistent storage with the given storage_conf file.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    global INIT
    if use_storage(storage_conf):
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
    """ Stops the persistent storage.

    This function emits the event in the master.

    :param logger: Logger where to log the messages.
    :return: None
    """
    __stop_storage__(logger)


@emit_event(STOP_STORAGE_EVENT)
def stop_storage(logger):
    """ Stops the persistent storage.

    This function emits the event in the worker.

    :param logger: Logger where to log the messages.
    :return: None
    """
    __stop_storage__(logger)


def __stop_storage__(logger):  # noqa
    """ Stops the persistent storage.

    :param logger: Logger where to log the messages.
    :return: None
    """
    global FINISH
    if __debug__:
        logger.debug("Stopping storage")
    FINISH()  # noqa
