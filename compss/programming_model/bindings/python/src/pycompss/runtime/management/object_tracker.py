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

# Dictionary to contain the conversion from object id to the
# filename where it is stored (mapping).
# The filename will be used for requesting an object to
# the runtime (its corresponding version).
objid_to_filename = {}

# Dictionary that contains the objects used within tasks.
pending_to_synchronize = {}

# Objects that have been accessed by the main program
_objs_written_by_mp = {}  # obj_id -> compss_file_name

# Identifier handling
_current_id = 1
# Object identifiers will be of the form _runtime_id-_current_id
# This way we avoid to have two objects from different applications having
# the same identifier
_runtime_id = str(uuid.uuid1())
# All objects are segregated according to their position in memory
# Given these positions, all objects are then reverse-mapped according
# to their filenames
_addr2id2obj = {}


def get_objid_to_filename_values():
    return objid_to_filename.values()


def get_objid_to_filename(obj_id):
    return objid_to_filename.get(obj_id)


def set_objid_to_filename(obj_id, filename):
    objid_to_filename[obj_id] = filename


def pop_objid_to_filename(obj_id):
    return objid_to_filename.pop(obj_id)


def get_all_pending_to_synchronize():
    return pending_to_synchronize


def set_pending_to_synchronize(obj_id, filename):
    pending_to_synchronize[obj_id] = filename


def pop_pending_to_synchronize(obj_id):
    return pending_to_synchronize.pop(obj_id)


def get_all_objs_written_by_mp():
    return _objs_written_by_mp


def set_objs_written_by_mp(obj_id, filename):
    _objs_written_by_mp[obj_id] = filename


def pop_objs_written_by_mp(obj_id):
    return _objs_written_by_mp.pop(obj_id)


def get_object_id(obj, assign_new_key=False, force_insertion=False):
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
    global _current_id
    global _runtime_id
    # Force_insertion implies assign_new_key
    assert not force_insertion or assign_new_key

    obj_address = calculate_identifier(obj)

    # Assign an empty dictionary (in case there is nothing there)
    _id2obj = _addr2id2obj.setdefault(obj_address, {})

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
        # different applications with the same identifier (and thus file name)
        new_id = '%s-%d' % (_runtime_id, _current_id)
        _id2obj[new_id] = obj
        _current_id += 1
        return new_id
    if len(_id2obj) == 0:
        _addr2id2obj.pop(obj_address)
    return None


def pop_object_id(obj):
    """
    Pop an object from the nested identifier hashmap
    :param obj: Object to pop
    :return: Popped object, None if obj was not in _addr2id2obj
    """
    global _addr2id2obj
    obj_address = calculate_identifier(obj)
    _id2obj = _addr2id2obj.setdefault(obj_address, {})
    for (k, v) in list(_id2obj.items()):
        _id2obj.pop(k)
    _addr2id2obj.pop(obj_address)


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
    # # This means that we do not have the previous object to compare directly.
    # # So the only way would be to ensure the uniqueness by calculating
    # # an id which depends on the object.
    # # BUT THIS IS REALLY EXPENSIVE. So: Use the id and unregister the object
    # #                                   (IN) to be modified explicitly.
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
    #     #  - The object representation (object size is the same but has been
    #     #                               modified (e.g. list element))
    #     # WARNING: Caveat:
    #     #  - IN User defined object with parameter change without __repr__
    #     # INOUT parameters to be modified require a synchronization, so they
    #     # are not affected.
    #     import hashlib
    #     hash_id = hashlib.md5()
    #     hash_id.update(str(id(obj)).encode())            # Consider the memory pointer        # noqa: E501
    #     hash_id.update(str(total_sizeof(obj)).encode())  # Include the object size            # noqa: E501
    #     hash_id.update(repr(obj).encode())               # Include the object representation  # noqa: E501
    #     obj_address = str(hash_id.hexdigest())
    # return obj_address


def clean_object_tracker():
    pending_to_synchronize.clear()
    _addr2id2obj.clear()
    objid_to_filename.clear()
    _objs_written_by_mp.clear()
