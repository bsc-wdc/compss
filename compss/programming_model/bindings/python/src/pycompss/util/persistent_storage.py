#!/usr/bin/python

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
except:
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


def is_PSCO(obj):
    '''
    Checks if obj is a persistent object (external storage).
    :param obj: Object to check
    :return: Boolean
    '''
    # Check from storage object requires a dummy storage object class
    #from storage.storage_object import storage_object
    #return issubclass(obj.__class__, storage_object) and get_ID(obj) not in [None, 'None']
    return has_ID(obj) and get_ID(obj) not in [None, 'None']


def has_ID(obj):
    '''
    Checks if the object has a getID method.
    :param obj: Object to check
    :return: Boolean
    '''
    if 'getID' in dir(obj):
        return True
    else:
        return False


def get_ID(psco):
    '''
    Retrieve the persistent object identificator.
    :param psco: Persistent object
    :return: Id
    '''
    return psco.getID()


def get_by_ID(id):
    '''
    Retrieve the actual object from a persistent object identificator.
    :param id: Persistent object identificator
    :return: The object that corresponds to the id
    '''
    return getByID(id)
