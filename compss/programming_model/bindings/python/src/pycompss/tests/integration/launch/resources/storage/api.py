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
Storage dummy connector.

This file contains the functions that any storage that wants to be used
with PyCOMPSs must implement.

storage.api code example.
"""

import os
import socket
import uuid

from pycompss.tests.outlog import get_logger
from pycompss.util.serialization.serializer import deserialize_from_file
from pycompss.util.serialization.serializer import serialize_to_file
from pycompss.util.typing_helper import typing

STORAGE_PATH = "/tmp/PSCO/" + str(socket.gethostname()) + "/"  # NOSONAR
LOGGER = get_logger()


def init(config_file_path=None, **kwargs):  # noqa
    """Initialize dummy storage.

    :param config_file_path: Storage conf.
    :param kwargs: Other arguments.
    :return: None.
    """
    # print("-----------------------------------------------------")
    # print("| WARNING!!! - YOU ARE USING THE DUMMY STORAGE API. |")
    # print("| Call to: init function.                           |")
    # print("| Parameters: config_file_path = None")
    # for key in kwargs:
    #     print("| Kwargs: Key %s - Value %s" % (key, kwargs[key]))
    # print("-----------------------------------------------------")
    pass


def finish(**kwargs: dict) -> None:
    """Finalize dummy storage.

    :param kwargs: Other arguments.
    :return: None.
    """
    # print("-----------------------------------------------------")
    # print("| WARNING!!! - YOU ARE USING THE DUMMY STORAGE API. |")
    # print("| Call to: finish function.                         |")
    # for key in kwargs:
    #     print("| Kwargs: Key %s - Value %s" % (key, kwargs[key]))
    # print("-----------------------------------------------------")
    pass


def init_worker(config_file_path: str = None, **kwargs: dict) -> None:
    """Initialize dummy storage at worker.

    :param config_file_path: Storage conf.
    :param kwargs: Other arguments.
    :return: None.
    """
    # print("-----------------------------------------------------")
    # print("| WARNING!!! - YOU ARE USING THE DUMMY STORAGE API. |")
    # print("| Call to: init Worker function.                    |")
    # print("| Parameters: config_file_path = None")
    # for key in kwargs:
    #     print("| Kwargs: Key %s - Value %s" % (key, kwargs[key]))
    # print("-----------------------------------------------------")
    pass


def finish_worker(**kwargs: dict) -> None:
    """Finalize dummy storage at worker.

    :param kwargs: Other arguments.
    :return: None.
    """
    # print("-----------------------------------------------------")
    # print("| WARNING!!! - YOU ARE USING THE DUMMY STORAGE API. |")
    # print("| Call to: finish Worker function.                  |")
    # for key in kwargs:
    #     print("| Kwargs: Key %s - Value %s" % (key, kwargs[key]))
    # print("-----------------------------------------------------")
    pass


def get_by_id(id: str) -> typing.Any:
    """Retrieve an object from an external storage.

    This dummy returns the same object as submitted by the parameter obj.

    :param id: Key of the object to be retrieved.
    :return: The real object.
    """
    # # Warning message:
    # print "-----------------------------------------------------"
    # print "| WARNING!!! - YOU ARE USING THE DUMMY STORAGE API. |"
    # print "| Call to: get_by_id                                  |"
    # print "|   *********************************************   |"
    # print "|   *** Check that you really want to use the ***   |"
    # print "|   ************* dummy storage api *************   |"
    # print "|   *********************************************   |"
    # print "-----------------------------------------------------"
    if id is not None:
        try:
            file_name = id + ".PSCO"
            file_path = STORAGE_PATH + file_name
            obj = deserialize_from_file(file_path, logger=LOGGER)
            obj.setID(id)  # noqa
            return obj
        except ValueError:
            # The id does not complain uuid4 --> raise an exception
            print(
                "Error: the ID for get_by_id does not complain the "
                "uuid4 format."
            )
            raise ValueError(
                "Using the dummy storage API get_by_id with wrong id."
            )
    else:
        # Using a None id --> raise an exception
        print("Error: the ID for get_by_id is None.")
        raise ValueError("Using the dummy storage API get_by_id with None id.")


def make_persistent(obj: typing.Any, *args: dict) -> None:
    """Persist the given object.

    :param obj: object to persist.
    :param args: Extra arguments.
    :return: None.
    """
    if obj.id is None:
        if len(args) == 0:
            # The user has not indicated the id
            uid = uuid.uuid4()
        elif len(args) == 1:
            # The user has indicated the id
            uid = args[0]
        else:
            raise ValueError("Too many arguments when calling makePersistent.")
        obj.id = str(uid)
        # Write ID file
        file_name = str(uid) + ".ID"
        file_path = STORAGE_PATH + file_name
        print("MAKE PERSISTENT: Creating ID file " + file_path)
        with open(file_path, "w") as f:
            f.write(obj.id)

        # Write PSCO file
        file_name = str(uid) + ".PSCO"
        file_path = STORAGE_PATH + file_name
        print("MAKE PERSISTENT: Serializing object to file " + file_path)
        serialize_to_file(obj, file_path, logger=LOGGER)
    else:
        # The obj is already persistent
        pass


def update_persistent(obj: typing.Any, *args: dict) -> None:
    """Update the given object.

    :param obj: object to update.
    :param args: Extra arguments.
    :return: None.
    """
    if obj.id is not None:
        # The psco is already persistent
        # Update PSCO file
        file_name = str(obj.id) + ".PSCO"
        file_path = STORAGE_PATH + file_name
        # Remove old file
        os.remove(file_path)
        # Create a new one
        serialize_to_file(obj, file_path, logger=LOGGER)
    else:
        # The obj is not persistent
        pass


def remove_by_id(obj: str) -> None:
    """Remove the given object.

    :param obj: Object to remove.
    :return: None.
    """
    if obj.id is not None:
        # Remove ID file from /tmp/PSCO
        file_name = str(obj.id) + ".ID"
        file_path = STORAGE_PATH + file_name
        try:
            print("Removing ID file: " + file_path)
            os.remove(file_path)
        except OSError:
            print("PSCO: " + file_path + " Does not exist!")

        # Remove PSCO file from /tmp/PSCO
        file_name = str(obj.id) + ".PSCO"
        file_path = STORAGE_PATH + file_name
        try:
            print("Removing Object file: " + file_path)
            os.remove(file_path)
            obj.id = None
        except OSError:
            print("PSCO: " + file_path + " Does not exist!")
    else:
        # The obj is not persistent yet
        pass


class TaskContext(object):
    """Dummy task context."""

    def __init__(self, logger, values, config_file_path=None):
        """Do nothing init.

        :param logger: Logger.
        :param values: Values.
        :param config_file_path: Configuration file path.
        """
        self.logger = logger
        self.values = values
        self.config_file_path = config_file_path

    def __enter__(self):
        """Do nothing at enter.

        :returns: None.
        """
        # Do something prolog
        # Ready to start the task
        self.logger.info("Prolog finished")

    def __exit__(self, type, value, traceback):
        """Do nothing at exit.

        :returns: None.
        """
        # Do something epilog
        # Finished
        self.logger.info("Epilog finished")


# Renaming
initWorker = init_worker  # noqa: N816
finishWorker = finish_worker  # noqa: N816
getByID = get_by_id  # noqa: N816
makePersistent = make_persistent  # noqa: N816
updatePersistent = update_persistent  # noqa: N816
removeById = remove_by_id  # noqa: N816
