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
PyCOMPSs Utils - External Storage
=================================
    This file contains the methods required to manage PSCOs.
    Isolates the API signature calls.
"""

try:
    # Try to import the external storage API module methods
    from storage.api import getByID
    from storage.api import TaskContext

    print("INFO: Storage API successfully imported.")
except ImportError:
    # print("INFO: No storage API defined.")
    # Defined methods throwing exceptions.

    def getByID(id):
        raise Exception('Unexpected call to getByID.')

    class TaskContext(object):
        def __init__(self, logger, values, config_file_path=None):
            self.logger = logger
            self.logger.error('Unexpected call to dummy storage task context.')
            raise Exception('Unexpected call to dummy storage task context.')

        def __enter__(self):
            # Ready to start the task
            self.logger.error('Unexpected call to dummy storage task context __enter__.')
            raise Exception('Unexpected call to dummy storage task context __enter__.')

        def __exit__(self, type, value, traceback):
            # Task finished
            self.logger.error('Unexpected call to dummy storage task context __exit__.')
            raise Exception('Unexpected call to dummy storage task context __exit__.')

storage_task_context = TaskContext  # Renamed for importing it from the worker


def is_psco(obj):
    """
    Checks if obj is a persistent object (external storage).

    :param obj: Object to check
    :return: <Boolean>
    """

    # Check from storage object requires a dummy storage object class
    # from storage.storage_object import storage_object
    # return issubclass(obj.__class__, storage_object) and get_id(obj) not in [None, 'None']
    return has_id(obj) and get_id(obj) not in [None, 'None']


def has_id(obj):
    """
    Checks if the object has a getID method.

    :param obj: Object to check
    :return: <Boolean>
    """

    if 'getID' in dir(obj):
        return True
    else:
        return False


def get_id(psco):
    """
    Retrieve the persistent object identifier.

    :param psco: Persistent object
    :return: <String> Id
    """

    return psco.getID()


def get_by_id(id):
    """
    Retrieve the actual object from a persistent object identifier.

    :param id: Persistent object identifier
    :return: The object that corresponds to the id
    """

    return getByID(id)
