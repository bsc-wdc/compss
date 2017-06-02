#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
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
"""
@author: fconejer

Storage dummy connector
=======================
    This file contains the functions that any storage that wants to be used
    with PyCOMPSs must implement

    storage.api code example.
"""
import os
import uuid
from uuid import UUID
from pycompss.util.serializer import serialize_to_file
from pycompss.util.serializer import deserialize_from_file

storage_path = '/tmp/PSCO/linux-BSC/'


def init(config_file_path=None, **kwargs):
    '''
    print "-----------------------------------------------------"
    print "| WARNING!!! - YOU ARE USING THE DUMMY STORAGE API. |"
    print "| Call to: init function.                           |"
    print "| Parameters: config_file_path = None"
    for key in kwargs:
        print "| Kwargs: Key %s - Value %s" % (key, kwargs[key])
    print "-----------------------------------------------------"
    '''
    pass


def finish(**kwargs):
    '''
    print "-----------------------------------------------------"
    print "| WARNING!!! - YOU ARE USING THE DUMMY STORAGE API. |"
    print "| Call to: finish function.                         |"
    for key in kwargs:
        print "| Kwargs: Key %s - Value %s" % (key, kwargs[key])
    print "-----------------------------------------------------"
    '''
    pass


def initWorker(config_file_path=None, **kwargs):
    '''
    print "-----------------------------------------------------"
    print "| WARNING!!! - YOU ARE USING THE DUMMY STORAGE API. |"
    print "| Call to: init Worker function.                    |"
    print "| Parameters: config_file_path = None"
    for key in kwargs:
        print "| Kwargs: Key %s - Value %s" % (key, kwargs[key])
    print "-----------------------------------------------------"
    '''
    pass


def finishWorker(**kwargs):
    '''
    print "-----------------------------------------------------"
    print "| WARNING!!! - YOU ARE USING THE DUMMY STORAGE API. |"
    print "| Call to: finish Worker function.                  |"
    for key in kwargs:
        print "| Kwargs: Key %s - Value %s" % (key, kwargs[key])
    print "-----------------------------------------------------"
    '''
    pass


def getByID(id):
    """
    This functions retrieves an object from an external storage technology
    from the obj object.
    This dummy returns the same object as submited by the parameter obj.
    @param obj: key/object of the object to be retrieved.
    @return: the real object.
    """
    print "-----------------------------------------------------"
    print "| WARNING!!! - YOU ARE USING THE DUMMY STORAGE API. |"
    print "| Call to: getByID                                  |"
    print "|   *********************************************   |"
    print "|   *** Check that you really want to use the ***   |"
    print "|   ************* dummy storage api *************   |"
    print "|   *********************************************   |"
    print "-----------------------------------------------------"
    if id is not None:
        try:
            # Validate that the uuid is uuid4
            val = UUID(id, version=4)
            file_name = id + '.PSCO'
            file_path = storage_path + file_name
            obj = deserialize_from_file(file_path)
            return obj
        except ValueError:
            # The id does not complain uuid4 --> raise an exception
            print "Error: the ID for getByID does not complain the uuid4 format."
            raise ValueError('Using the dummy storage API getByID with wrong id.')
    else:
        # Using a None id --> raise an exception
        print "Error: the ID for getByID is None."
        raise ValueError('Using the dummy storage API getByID with None id.')


def makePersistent(obj, *args):
    print "Make persistent"

    if obj.id is None:
        if len(args) == 0:
            # The user has not indicated the id
            uid = uuid.uuid4()
        elif len(args) == 1:
            # The user has indicated the id
            uid = args[0]
        else:
            raise ValueError('Too many arguments when calling makePersistent.')
        obj.id = str(uid)
        # Write ID file
        file_name = str(uid) + '.ID'
        file_path = storage_path + file_name
        with open (file_path, 'w') as f:
            f.write(obj.id)

        # Write PSCO file
        file_name = str(uid) + '.PSCO'
        file_path = storage_path + file_name
        serialize_to_file(obj, file_path)
    else:
        # The obj is already persistent
        pass


def removeById(obj):
    if obj.id is not None:
        # Remove file from /tmp
        file_name = str(obj.id) + '.PSCO'
        file_path = storage_path + file_name
        try:
            os.remove(file_path)
            obj.id = None
        except:
            print "PSCO: " + file_path + " Does not exist!"
    else:
        # The obj is not persistent yet
        pass

class TaskContext(object):

    def __init__(self, logger, values, config_file_path=None):
        self.logger = logger
        self.values = values
        self.config_file_path = config_file_path

    def __enter__(self):
        # Do something prolog

        # Ready to start the task
        self.logger.info("Prolog finished")
        pass

    def __exit__(self, type, value, traceback):
        # Do something epilog

        # Finished
        self.logger.info("Epilog finished")
        pass
