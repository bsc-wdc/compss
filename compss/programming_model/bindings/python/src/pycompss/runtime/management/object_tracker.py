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
PyCOMPSs Binding - Management - Object tracker
==============================================
    This file contains the object tracking functionality.
"""

import os
import uuid
from pycompss.runtime.commons import get_temporary_directory

if __debug__:
    import logging
    logger = logging.getLogger(__name__)


class ObjectTracker(object):
    """
    Object tracker class
    --------------------

    This class has all needed data structures and functionalities
    to keep track of the objects within the python binding.
    """

    __slots__ = ['file_names', 'pending_to_synchronize',
                 'written_objects', 'current_id', 'runtime_id',
                 'id2obj2addr']

    def __init__(self):
        # Dictionary to contain the conversion from object id to the
        # filename where it is stored (mapping).
        # The filename will be used for requesting an object to
        # the runtime (its corresponding version).
        self.file_names = {}
        # Set that contains the object identifiers of the objects to pending
        # to be synchronized.
        self.pending_to_synchronize = set()
        # Set of identifiers of the objects that have been accessed by the
        # main program
        self.written_objects = set()
        # Identifier handling
        self.current_id = 1
        # Object identifiers will be of the form _runtime_id-_current_id
        # This way we avoid to have two objects from different applications
        # having the same identifier
        self.runtime_id = str(uuid.uuid1())
        # Dictionary to contain the conversion from object identifier to
        # the memory address of the object.
        # Given these positions, all objects are then reverse-mapped.
        self.id2obj2addr = {}

    def track(self, obj, collection=False):
        # type: (object, bool) -> str
        """ Start tracking an object.

        Collections are not stored into a file. Consequently, we just register
        it to keep track of the identifier, but no file is stored. However,
        the collection elements are stored into files.

        :param obj: Object to track.
        :param collection: If the object to be tracked is a collection.
        :return: Object identifier.
        """
        if collection:
            return self._register_object(obj, True)
        else:
            obj_id = self._register_object(obj, True)
            file_name = os.path.join(get_temporary_directory(), str(obj_id))
            self._set_file_name(obj_id, file_name)
            self.set_pending_to_synchronize(obj_id)
            if __debug__:
                logger.debug("Mapping object %s to file %s" % (obj_id,
                                                               file_name))
            return obj_id

    def stop_tracking(self, obj, collection=False):
        # type: (object, bool) -> None
        """ Stop tracking the given object.

        :param obj: Object to stop tracking.
        :param collection: If the object to stop tracking is a collection.
        :return: None
        """
        obj_id = self.is_tracked(obj)
        if obj_id is not None:
            if collection:
                self._pop_object_id(obj_id)
            else:
                self._delete_file_name(obj_id)
                self._remove_from_pending_to_synchronize(obj_id)
                self._pop_object_id(obj_id)

    def get_object_id(self, obj):
        # type: (object) -> str or None
        """ Returns the object identifier.

        This function is a wrapper of is_tracked.

        :param obj: Object to check.
        :return: Object identifier if under tracking. None otherwise.
        """
        return self.is_tracked(obj)

    def is_tracked(self, obj):
        # type: (object) -> str or None
        """ Checks if the given object is being tracked.

        Due to the length that the id2obj2addr dictionary can reach, if
        is tracked we return the identifier in order to avoid to search again
        into the dictionary.

        :param obj: Object to check.
        :return: Object identifier if under tracking. None otherwise.
        """
        if self.id2obj2addr:
            for identifier in self.id2obj2addr:
                obj_address = self._get_object_address(obj)
                if self.id2obj2addr[identifier] == obj_address:
                    # Found
                    return identifier
            # Not Found
            return None
        else:
            # The dictionary is empty. So it is not tracked.
            return None

    def get_all_file_names(self):
        # type: () -> tuple
        """ Returns all files used.

        Useful for cleanup.

        :return: List of file name that are currently available.
        """
        return tuple(self.file_names.values())

    def get_file_name(self, obj_id):
        # type: (str) -> str
        """ Get the file name associated to the given object identifier.

        :param obj_id: Object identifier.
        :return: File name.
        """
        return self.file_names.get(obj_id)

    def is_obj_pending_to_synchronize(self, obj):
        # type: (object) -> bool
        """ Checks if the given object is pending to be synchronized.

        :param obj: Object to check.
        :return: True if pending. False otherwise.
        """
        obj_id = self.is_tracked(obj)
        if obj_id is None:
            return False
        else:
            return self.is_pending_to_synchronize(obj_id)

    def is_pending_to_synchronize(self, obj_id):
        # type: (str) -> bool
        """ Checks if the given object identifier is in pending to be
        synchronized dictionary.

        :param obj_id: Object identifier.
        :return: True if pending. False otherwise.
        """
        return obj_id in self.pending_to_synchronize

    def set_pending_to_synchronize(self, obj_id):
        # type: (str) -> None
        """ Set the given filename with object identifier as pending to
        synchronize.

        :param obj_id: Object identifier.
        :return: None
        """
        self.pending_to_synchronize.add(obj_id)

    def has_been_written(self, obj_id):
        # type: (str) -> bool
        """ Checks if the given object identifier has been written by the
        main program.

        :param obj_id: Object identifier.
        :return: True if written. False otherwise.
        """
        return obj_id in self.written_objects

    def pop_written_obj(self, obj_id):
        # type: (str) -> str
        """ Pop a written filename with the given object identifier from
        written objects.

        :param obj_id: Object identifier.
        :return: The file name.
        """
        self.written_objects.remove(obj_id)
        return self.get_file_name(obj_id)

    def update_mapping(self, obj_id, obj):
        # type: (str, object) -> None
        """ Updates the object into the object tracker.

        :param obj_id: Object identifier.
        :param obj: New object to track.
        :return: None
        """
        # The main program won't work with the old object anymore, update
        # mapping
        new_obj_id = self._register_object(obj, True, True)
        old_file_name = self.get_file_name(obj_id)
        new_file_name = old_file_name.replace(obj_id, new_obj_id)
        self._set_file_name(new_obj_id, new_file_name, written=True)

    def clean_object_tracker(self):
        # type: () -> None
        """ Clears all object tracker internal structures.

        :return: None
        """
        self.pending_to_synchronize.clear()
        self.file_names.clear()
        self.written_objects.clear()
        self.id2obj2addr.clear()

    #############################################
    #            PRIVATE FUNCTIONS              #
    #############################################

    def _register_object(self, obj, assign_new_key=False,
                         force_insertion=False):
        # type: (object, bool, bool) -> str or None
        """ Registers an object into the object tracker.

        If not found or we are forced to, we create a new identifier for this
        object, deleting the old one if necessary. We can also query for some
        object without adding it in case of failure.

        :param obj: Object to analyse.
        :param assign_new_key: Assign new key.
        :param force_insertion: force insertion.
        :return: Object id.
        """
        # Force_insertion implies assign_new_key
        assert not force_insertion or assign_new_key

        identifier = self.is_tracked(obj)
        if identifier is not None:
            if force_insertion:
                self.id2obj2addr.pop(identifier)
            else:
                return identifier

        if assign_new_key:
            # This object was not in our object database or we were forced to
            # remove it, lets assign it an identifier and store it.
            new_id = self._calculate_new_identifier()
            obj_address = self._get_object_address(obj)
            self.id2obj2addr[new_id] = obj_address
            return new_id

    def _calculate_new_identifier(self):
        # type: () -> str
        """ Generates a new identifier.

        Identifiers are of the form _runtime_id-_current_id in order to avoid
        having two objects from different applications with the same identifier
        (and thus file name).
        This function updates the internal self.current_id to guarantee
        that each time returns a new identifier.

        :return: New identifier.
        """
        new_id = '%s-%d' % (self.runtime_id, self.current_id)
        self.current_id += 1
        return new_id

    def _set_file_name(self, obj_id, filename, written=False):
        # type: (str, str, bool) -> None
        """ Set a filename for the given object identifier.

        :param obj_id: Object identifier.
        :param filename: File name.
        :param written: If the file has been written by main program
        :return: None
        """
        self.file_names[obj_id] = filename
        if written:
            self.written_objects.add(obj_id)

    def _delete_file_name(self, obj_id):
        # type: (str) -> None
        """ Remove the file name of the given object identifier.

        :param obj_id: Object identifier.
        :return: None
        """
        del self.file_names[obj_id]

    def _remove_from_pending_to_synchronize(self, obj_id):
        # type: (str) -> None
        """ Pop the filename of the given object identifier from pending to
        synchronize.

        :param obj_id: Object identifier.
        :return: None
        """
        self.pending_to_synchronize.remove(obj_id)

    def _pop_object_id(self, obj_id):
        # type: (object) -> object or None
        """ Pop an object from the dictionary.

        :param obj_id: Object identifier to pop.
        :return: Popped object.
        """
        return self.id2obj2addr.pop(obj_id)

    @staticmethod
    def _get_object_address(obj):
        # type: (object) -> int
        """ Retrieves the object memory address.

        :param obj: Object to get the memory address.
        :return: Object identifier.
        """
        return id(obj)
        # # If we want to detect automatically IN objects modification we need
        # # to ensure uniqueness of the identifier. At this point, obj is a
        # # reference to the object that we want to compute its identifier.
        # # This means that we do not have the previous object to compare
        # # directly.
        # # So the only way would be to ensure the uniqueness by calculating
        # # an id which depends on the object.
        # # BUT THIS IS REALLY EXPENSIVE. So: Use the id and unregister the
        # #                                   object (IN) to be modified
        # #                                   explicitly.
        # immutable_types = [bool, int, float, complex, str,
        #                    tuple, frozenset, bytes]
        # obj_type = type(obj)
        # if obj_type in immutable_types:
        #     obj_address = id(obj)  # Only guarantees uniqueness with
        #                            # immutable objects
        # else:
        #     # For all the rest, use hash of:
        #     #  - The object id
        #     #  - The size of the object (object increase/decrease)
        #     #  - The object representation (object size is the same but has
        #     #                               been modified(e.g. list element))
        #     # WARNING: Caveat:
        #     #  - IN User defined object with parameter change without
        #     #    __repr__
        #     # INOUT parameters to be modified require a synchronization, so
        #     # they are not affected.
        #     import hashlib
        #     hash_id = hashlib.md5()
        #     hash_id.update(str(id(obj)).encode())            # Consider the memory pointer        # noqa: E501
        #     hash_id.update(str(total_sizeof(obj)).encode())  # Include the object size            # noqa: E501
        #     hash_id.update(repr(obj).encode())               # Include the object representation  # noqa: E501
        #     obj_address = str(hash_id.hexdigest())
        # return obj_address


# Instantiation of the Object tracker class to be shared among
# management modules
OT = ObjectTracker()
