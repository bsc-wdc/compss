#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Utils - Storage - Persistent.

This file contains the methods required to manage PSCOs.
Isolates the API signature calls.
"""

import logging

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.tracing.helpers import EventInsideWorker
from pycompss.util.tracing.helpers import EventMaster
from pycompss.util.tracing.helpers import EventWorker
from pycompss.util.tracing.types_events_master import TRACING_MASTER
from pycompss.util.tracing.types_events_worker import TRACING_WORKER
from pycompss.util.typing_helper import typing


# Definition helpers
def __dummy_function() -> None:
    """Do nothing function.

    To be used as definition of INIT, FINISH and GET_BY_ID globals.

    :return: None
    :raises PyCOMPSsException: Always raises this function if invoked.
    """
    raise PyCOMPSsException(
        "Invoking unexpected dummy function from persistent storage API."
    )


# Globals
# Contain the actual storage api functions set on initialization
INIT = __dummy_function  # type: typing.Callable
FINISH = __dummy_function  # type: typing.Callable
GET_BY_ID = __dummy_function  # type: typing.Callable
TaskContext = None  # type: typing.Any
DUMMY_STORAGE = False  # type: bool


class DummyTaskContext(object):
    """Dummy task context to be used with storage frameworks."""

    def __init__(
        self,
        logger: logging.Logger,
        values: typing.Any,
        config_file_path: typing.Optional[str] = None,
    ) -> None:
        """Create a new instance of DummyTaskContext.

        :param logger: Logger facility.
        :param values: Values.
        :param config_file_path: Configuration file path.
        :returns: None
        """
        self.logger = logger
        err_msg = "Unexpected call to dummy storage task context."
        self.logger.error(err_msg)
        self.values = values
        self.config_file_path = config_file_path
        raise PyCOMPSsException(err_msg)

    def __enter__(self) -> None:
        """Execute before starting the task.

        :returns: None.
        :raises: PyCOMPSsException: If dummy task context is used.
        """
        # Ready to start the task
        err_msg = "Unexpected call to dummy storage task context __enter__"
        self.logger.error(err_msg)
        raise PyCOMPSsException(err_msg)

    def __exit__(
        self, type: typing.Any, value: typing.Any, traceback: typing.Any
    ) -> None:
        """Execute when the task has finished.

        Signature from context.

        :param type: Type.
        :param value: Value.
        :param traceback: Traceback.
        :returns: None.
        :raises: PyCOMPSsException: If dummy task context is used.
        """
        # Task finished
        err_msg = "Unexpected call to dummy storage task context __exit__"
        self.logger.error(err_msg)
        raise PyCOMPSsException(err_msg)


def load_storage_library() -> None:
    """Import the proper storage libraries.

    :return: None.
    """
    global INIT
    global FINISH
    global GET_BY_ID
    global TaskContext
    global DUMMY_STORAGE
    error_msg = "UNDEFINED"

    def dummy_init(config_file_path: typing.Optional[str] = None) -> None:
        """Initialize the storage library.

        :returns: None.
        :raises: PyCOMPSsException: If dummy task context is used.
        """
        raise PyCOMPSsException(
            f"Unexpected call to init from storage. Reason: {error_msg}"
        )

    def dummy_finish() -> None:
        """Finish the storage library.

        :returns: None.
        :raises: PyCOMPSsException: If dummy task context is used.
        """
        raise PyCOMPSsException(
            f"Unexpected call to finish from storage. Reason: {error_msg}"
        )

    def dummy_get_by_id(identifier: str) -> None:
        """Get object by id from the storage library.

        :param identifier: Object identifier.
        :returns: None.
        :raises: PyCOMPSsException: If dummy task context is used.
        """
        raise PyCOMPSsException(
            f"Unexpected call to getByID. Reason: {error_msg}"
        )

    try:
        # Try to import the external storage API module methods
        from storage.api import init as real_init  # noqa
        from storage.api import finish as real_finish  # noqa
        from storage.api import getByID as real_get_by_id  # noqa
        from storage.api import TaskContext as RealTaskContext  # noqa

        DUMMY_STORAGE = False
        print("INFO: Storage API successfully imported.")
    except ImportError as import_error:
        # print("INFO: No storage API defined.")
        # Defined methods throwing exceptions.
        error_msg = str(import_error)
        DUMMY_STORAGE = True

    # Prepare the imports
    if DUMMY_STORAGE:
        INIT = dummy_init
        FINISH = dummy_finish
        GET_BY_ID = dummy_get_by_id
        TaskContext = DummyTaskContext
    else:
        INIT = real_init
        FINISH = real_finish
        GET_BY_ID = real_get_by_id
        TaskContext = RealTaskContext


def is_psco(obj: typing.Any) -> bool:
    """Check if obj is a persistent object (external storage).

    :param obj: Object to check.
    :return: True if is persistent object. False otherwise.
    """
    # Check from storage object requires a dummy storage object class
    # from storage.storage_object import storage_object
    # return issubclass(obj.__class__, storage_object) and
    #        get_id(obj) not in [None, "None"]
    return has_id(obj) and get_id(obj) not in [None, "None"]


def has_id(obj: typing.Any) -> bool:
    """Check if the object has a getID method.

    :param obj: Object to check.
    :return: True if is persistent object. False otherwise.
    """
    if "getID" in dir(obj):
        return True
    return False


def get_id(psco: typing.Any) -> typing.Union[str, None]:
    """Retrieve the persistent object identifier.

    :param psco: Persistent object.
    :return: Persistent object identifier.
    """
    with EventInsideWorker(TRACING_WORKER.getid_event):
        return psco.getID()


def get_by_id(identifier: str) -> typing.Any:
    """Retrieve the object from the given identifier.

    :param identifier: Persistent object identifier.
    :return: object associated to the persistent object identifier.
    """
    with EventInsideWorker(TRACING_WORKER.get_by_id_event):
        return GET_BY_ID(identifier)


def master_init_storage(storage_conf: str, logger: logging.Logger) -> bool:
    """Call to init storage from the master.

    This function emits the event in the master.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    with EventMaster(TRACING_MASTER.init_storage_event):
        return __init_storage(storage_conf, logger)


def use_storage(storage_conf: str) -> bool:
    """Evaluate if the storage_conf is defined.

    The storage will be used if storage_conf is not None nor "null".

    :param storage_conf: Storage configuration file.
    :return: True if defined. False on the contrary.
    """
    return storage_conf != "" and not storage_conf == "null"


def init_storage(storage_conf: str, logger: logging.Logger) -> bool:
    """Call to init storage.

    This function emits the event in the worker.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    with EventWorker(TRACING_WORKER.init_storage_event):
        return __init_storage(storage_conf, logger)


def __init_storage(storage_conf: str, logger: logging.Logger) -> bool:
    """Call to init storage.

    Initializes the persistent storage with the given storage_conf file.

    :param storage_conf: Storage configuration file.
    :param logger: Logger where to log the messages.
    :return: True if initialized. False on the contrary.
    """
    if use_storage(storage_conf):
        if __debug__:
            logger.debug("Starting storage")
            logger.debug("Storage configuration file: %s", storage_conf)
        load_storage_library()
        # Call to storage init
        INIT(config_file_path=storage_conf)  # noqa
        return True
    return False


def master_stop_storage(logger: logging.Logger) -> None:
    """Stop the persistent storage.

    This function emits the event in the master.

    :param logger: Logger where to log the messages.
    :return: None
    """
    with EventMaster(TRACING_MASTER.stop_storage_event):
        __stop_storage(logger)


def stop_storage(logger: logging.Logger) -> None:
    """Stop the persistent storage.

    This function emits the event in the worker.

    :param logger: Logger where to log the messages.
    :return: None
    """
    with EventWorker(TRACING_WORKER.stop_storage_event):
        __stop_storage(logger)


def __stop_storage(logger: logging.Logger) -> None:
    """Stop the persistent storage.

    :param logger: Logger where to log the messages.
    :return: None.
    """
    if __debug__:
        logger.debug("Stopping storage")
    FINISH()
