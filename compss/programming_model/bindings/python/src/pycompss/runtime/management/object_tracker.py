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

import uuid


class ObjectTracker(object):
    """
    Object tracker class
    --------------------

    This class has all needed data structures and functionalities
    to keep track of the objects within the python binding.
    """

    def __init__(self):
        # Dictionary to contain the conversion from object id to the
        # filename where it is stored (mapping).
        # The filename will be used for requesting an object to
        # the runtime (its corresponding version).
        self.obj_id_to_filename = {}
        # Dictionary that contains the objects used within tasks.
        self.pending_to_synchronize = {}
        # Objects that have been accessed by the main program
        self.objs_written_by_mp = {}  # obj_id -> compss_file_name
        # Identifier handling
        self.current_id = 1
        # Object identifiers will be of the form _runtime_id-_current_id
        # This way we avoid to have two objects from different applications
        # having the same identifier
        self.runtime_id = str(uuid.uuid1())
        # All objects are segregated according to their position in memory
        # Given these positions, all objects are then reverse-mapped according
        # to their filenames
        self.addr2id2obj = {}

    def get_filenames(self):
        """
        Returns all files used.
        Useful for cleanup

        :return: List of file name that are currently available
        """
        return self.obj_id_to_filename.values()

    def get_filename(self, obj_id):
        """
        Get the file name associated to the given object identifier.

        :param obj_id: Object identifier
        :return: File name
        """
        return self.obj_id_to_filename.get(obj_id)

    def set_filename(self, obj_id, filename):
        """
        Set a filename for the given object identifier.

        :param obj_id: Object identifier
        :param filename: File name
        :return: None
        """
        self.obj_id_to_filename[obj_id] = filename

    def pop_filename(self, obj_id):
        """
        Pop the file name of the given object identifier.

        :param obj_id: Object identifier
        :return: The file name
        """
        return self.obj_id_to_filename.pop(obj_id)

    def get_pending_to_synchronize_objids(self):
        """
        Get all pending to synchronize object identifiers.

        :return: List of pending to synchronize object identifiers.
        """
        return self.pending_to_synchronize.keys()

    def set_pending_to_synchronize(self, obj_id, filename):
        """
        Set the given filename with object identifier as pending to
        synchronize.

        :param obj_id: Object identifier
        :param filename: File name
        :return: None
        """
        self.pending_to_synchronize[obj_id] = filename

    def pop_pending_to_synchronize(self, obj_id):
        """
        Pop the filename of the given object identifier from pending to
        synchronize.

        :param obj_id: Object identifier
        :return: The file name
        """
        return self.pending_to_synchronize.pop(obj_id)

    def get_all_written_objids(self):
        """
        Get all written object identifiers.

        :return: List of object identifiers
        """
        return self.objs_written_by_mp.keys()

    def set_written_obj(self, obj_id, filename):
        """
        Set file name with object identifier as written object.

        :param obj_id: Object identifier
        :param filename: File name
        :return: None
        """
        self.objs_written_by_mp[obj_id] = filename

    def pop_written_obj(self, obj_id):
        """
        Pop a written filename with the given object identifier from written
        objects.

        :param obj_id: Object identifier
        :return: The file name
        """
        return self.objs_written_by_mp.pop(obj_id)

    def get_object_id(self, obj, assign_new_key=False, force_insertion=False):
        """
        Gets the identifier of an object. If not found or we are forced to,
        we create a new identifier for this object, deleting the old one if
        necessary. We can also query for some object without adding it in case of
        failure.

        :param obj: Object to analyse.
        :param assign_new_key: <Boolean> Assign new key.
        :param force_insertion: <Boolean> Force insertion.
        :return: Object id
        """
        # Force_insertion implies assign_new_key
        assert not force_insertion or assign_new_key

        obj_address = self.calculate_identifier(obj)

        # Assign an empty dictionary (in case there is nothing there)
        _id2obj = self.addr2id2obj.setdefault(obj_address, {})

        for identifier in _id2obj:
            if _id2obj[identifier] is obj:
                if force_insertion:
                    _id2obj.pop(identifier)
                    break
                else:
                    return identifier
        if assign_new_key:
            # This object was not in our object database or we were forced to
            # remove it, lets assign it an identifier and store it.
            # As mentioned before, identifiers are of the form
            # _runtime_id-_current_id in order to avoid having two objects from
            # different applications with the same identifier (and thus file
            # name)
            new_id = '%s-%d' % (self.runtime_id, self.current_id)
            _id2obj[new_id] = obj
            self.current_id += 1
            return new_id
        if len(_id2obj) == 0:
            self.addr2id2obj.pop(obj_address)
        return None

    def pop_object_id(self, obj):
        """
        Pop an object from the nested identifier hashmap
        :param obj: Object to pop
        :return: Popped object, None if obj was not in _addr2id2obj
        """
        obj_address = self.calculate_identifier(obj)
        _id2obj = self.addr2id2obj.setdefault(obj_address, {})
        for (k, v) in list(_id2obj.items()):
            _id2obj.pop(k)
        self.addr2id2obj.pop(obj_address)

    @staticmethod
    def calculate_identifier(obj):
        """
        Calculates the identifier for the given object.

        :param obj: Object to get the identifier
        :return: String (md5 string)
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
        #                         # immutable objects
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

    def clean_object_tracker(self):
        """
        Clears all object tracker internal structures.

        :return: None
        """
        self.pending_to_synchronize.clear()
        self.addr2id2obj.clear()
        self.obj_id_to_filename.clear()
        self.objs_written_by_mp.clear()


# Instantiation of the Object tracker class to be shared among
# management modules
OT = ObjectTracker()
