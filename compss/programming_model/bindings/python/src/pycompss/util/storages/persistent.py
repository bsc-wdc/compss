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

from pycompss.util.typing_helper import typing

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.tracing.helpers import event_master
from pycompss.util.tracing.helpers import event_worker
from pycompss.util.tracing.helpers import event_inside_worker
from pycompss.worker.commons.constants import GETID_EVENT
from pycompss.worker.commons.constants import GET_BY_ID_EVENT
from pycompss.worker.commons.constants import (
    MAKE_PERSISTENT_EVENT,
)  # noqa Expose event id
from pycompss.worker.commons.constants import (
    DELETE_PERSISTENT_EVENT,
)  # noqa Expose event id
from pycompss.runtime.constants import INIT_STORAGE_EVENT as MASTER_INIT_STORAGE_EVENT
from pycompss.runtime.constants import STOP_STORAGE_EVENT as MASTER_STOP_STORAGE_EVENT
from pycompss.worker.commons.constants import INIT_STORAGE_EVENT
from pycompss.worker.commons.constants import STOP_STORAGE_EVENT


# Globals
# Contain the actual storage api functions set on initialization
INIT = None  # type: typing.Any
FINISH = None  # type: typing.Any
GET_BY_ID = None  # type: typing.Any
TaskContext = None  # type: typing.Any
DUMMY_STORAGE = False  # type: bool


class dummy_task_context(object):
    """
    Dummy task context to be used with storage frameworks.
    """

    def __init__(self, logger, values, config_file_path=None):
        # type: (typing.Any, typing.Any, str) -> None
        self.logger = logger
        err_msg = "Unexpected call to dummy storage task context."
        self.logger.error(err_msg)
        self.values = values
        self.config_file_path = config_file_path
        raise PyCOMPSsException(err_msg)

    def __enter__(self):
        # type: () -> None
        # Ready to start the task
        err_msg = "Unexpected call to dummy storage task context __enter__"
        self.logger.error(err_msg)
        raise PyCOMPSsException(err_msg)

    def __exit__(self, type, value, traceback):
        # type: (typing.Any, typing.Any, typing.Any) -> None
        # Task finished
        err_msg = "Unexpected call to dummy storage task context __exit__"
        self.logger.error(err_msg)
        raise PyCOMPSsException(err_msg)


def load_storage_library():  # noqa
    # type: () -> None
    """Import the proper storage libraries

    :return: None
    """
    global INIT
    global FINISH
    global GET_BY_ID
    global TaskContext
    global DUMMY_STORAGE
    error_msg = "UNDEFINED"

    def dummy_init(config_file_path=None):  # noqa
        # type: (str) -> None
        raise PyCOMPSsException(
            "Unexpected call to init from storage. Reason: %s" % error_msg
        )

    def dummy_finish():
        # type: () -> None
        raise PyCOMPSsException(
            "Unexpected call to finish from storage. Reason: %s" % error_msg
        )

    def dummy_get_by_id(id):  # noqa
        # type: (str) -> None
        raise PyCOMPSsException("Unexpected call to getByID. Reason: %s" % error_msg)

    try:
        # Try to import the external storage API module methods
        from storage.api import init as real_init  # noqa
        from storage.api import finish as real_finish  # noqa
        from storage.api import getByID as real_get_by_id  # noqa
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
    # type: (typing.Any) -> bool
    """Checks if obj is a persistent object (external storage).

    :param obj: Object to check.
    :return: True if is persistent object. False otherwise.
    """
    # Check from storage object requires a dummy storage object class
    # from storage.storage_object import storage_object
    # return issubclass(obj.__class__, storage_object) and
    #        get_id(obj) not in [None, "None"]
    return has_id(obj) and get_id(obj) not in [None, "None"]


def has_id(obj):
    # type: (typing.Any) -> bool
    """Checks if the object has a getID method.

    :param obj: Object to check.
    :return: True if is persistent object. False otherwise.
    """
    if "getID" in dir(obj):
        return True
    else:
        return False


def get_id(psco):
    # type: (typing.Any) -> typing.Union[str, None]
    """Retrieve the persistent object identifier.

    :param psco: Persistent object.
    :return: Persistent object identifier.
    """
    with event_inside_worker(GETID_EVENT):
        return psco.getID()


def get_by_id(id):
    # type: (str) -> typing.Any
    """Retrieve the object from the given identifier.

    :param id: Persistent object identifier.
    :return: object associated to the persistent object identifier.
    """
    with event_inside_worker(GET_BY_ID_EVENT):
        return GET_BY_ID(id)


def master_init_storage(storage_conf, logger):  # noqa
    # type: (str, typing.Any) -> bool
    """Call to init storage from the master.

    This function emits the event in the master.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    with event_master(MASTER_INIT_STORAGE_EVENT):
        return __init_storage__(storage_conf, logger)


def use_storage(storage_conf):
    # type: (str) -> bool
    """Evaluates if the storage_conf is defined.
    The storage will be used if storage_conf is not None nor "null".

    :param storage_conf: Storage configuration file.
    :return: True if defined. False on the contrary.
    """
    return storage_conf != "" and not storage_conf == "null"


def init_storage(storage_conf, logger):  # noqa
    # type: (str, typing.Any) -> bool
    """Call to init storage.

    This function emits the event in the worker.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    with event_worker(INIT_STORAGE_EVENT):
        return __init_storage__(storage_conf, logger)


def __init_storage__(storage_conf, logger):  # noqa
    # type: (str, typing.Any) -> bool
    """Call to init storage.

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


def master_stop_storage(logger):
    # type: (typing.Any) -> None
    """Stops the persistent storage.

    This function emits the event in the master.

    :param logger: Logger where to log the messages.
    :return: None
    """
    with event_master(MASTER_STOP_STORAGE_EVENT):
        __stop_storage__(logger)


def stop_storage(logger):
    # type: (typing.Any) -> None
    """Stops the persistent storage.

    This function emits the event in the worker.

    :param logger: Logger where to log the messages.
    :return: None
    """
    with event_worker(STOP_STORAGE_EVENT):
        __stop_storage__(logger)


def __stop_storage__(logger):  # noqa
    # type: (typing.Any) -> None
    """Stops the persistent storage.

    :param logger: Logger where to log the messages.
    :return: None
    """
    global FINISH
    if __debug__:
        logger.debug("Stopping storage")
    FINISH()  # noqa
