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
"""
Redis Storage Object implementation for the PyCOMPSs Python Binding
"""
import uuid
import storage.api

__name__ = "redispycompss"


class storage_object(object):
    """
    Storage Object
    """

    def __init__(self):
        """
        Constructor method
        """
        # Id will be None until persisted
        self.pycompss_psco_identifier = None
        self.pycompss_mark_as_unmodified()

    def makePersistent(self, identifier=None):
        """
        Stores the object in the Redis database
        """
        storage.api.makePersistent(self, identifier)

    def make_persistent(self, identifier=None):
        """
        Support for underscore notation
        """
        self.makePersistent(identifier)

    def deletePersistent(self):
        """
        Deletes the object from the Redis database
        """
        storage.api.deletePersistent(self)

    def delete_persistent(self):
        """
        Support for underscore notation
        """
        self.deletePersistent()

    def getID(self):
        """
        Gets the ID of the object
        """
        return self.pycompss_psco_identifier

    def pycompss_is_modified(self):
        """
        Check if the object was modified
        """
        return self._pycompss_modified

    def pycompss_set_mod_flag(self, val):
        """
        Set modification flag to given value
        """
        super(storage_object, self).__setattr__('_pycompss_modified', val)

    def pycompss_mark_as_unmodified(self):
        """
        Mark the object as unmodified
        """
        self.pycompss_set_mod_flag(False)

    def pycompss_mark_as_modified(self):
        """
        Mark the object as modified
        """
        self.pycompss_set_mod_flag(True)

    def __setattr__(self, name, value):
        """
        Custom setattr which marks the object as modified and calls the
        actual setattr
        """
        self.pycompss_set_mod_flag(True)
        super(storage_object, self).__setattr__(name, value)


"""
Support for camelCase
"""
StorageObject = storage_object
