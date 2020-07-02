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
PyCOMPSs Binding - Management - Object Synchronization
======================================================
    This file contains the object synchronization core methods.
"""

import logging

from pycompss.runtime.management.direction import get_compss_direction
from pycompss.runtime.management.classes import Future
from pycompss.runtime.management.object_tracker import OT
from pycompss.util.storages.persistent import is_psco
from pycompss.util.storages.persistent import get_by_id
from pycompss.util.storages.persistent import get_id
from pycompss.util.serialization.serializer import *
from pycompss.runtime.commons import IS_PYTHON3


if IS_PYTHON3:
    listType = list
    dictType = dict
else:
    import types
    listType = types.ListType
    dictType = types.DictType

# Setup logger
logger = logging.getLogger(__name__)


def wait_on_object(obj, mode):
    """
    Waits on an object.

    :param obj: Object to wait on.
    :param mode: Read or write mode
    :return: An object of 'file' type.
    """
    compss_mode = get_compss_direction(mode)
    if isinstance(obj, Future) or not (isinstance(obj, listType) or
                                       isinstance(obj, dictType)):
        return _synchronize(obj, compss_mode)
    else:
        if len(obj) == 0:  # FUTURE OBJECT
            return _synchronize(obj, compss_mode)
        else:
            # Will be a iterable object
            res = _wait_on_iterable(obj, compss_mode)
            return res


def _synchronize(obj, mode):
    """
    Synchronization function.
    This method retrieves the value of a future object.
    Calls the runtime in order to wait for the value and returns it when
    received.

    :param obj: Object to synchronize.
    :param mode: Direction of the object to synchronize.
    :return: The value of the object requested.
    """
    from pycompss.runtime.management.compss import COMPSs

    # TODO: Add a boolean to differentiate between files and object on the
    # COMPSs.open_file call. This change pretends to obtain better traces.
    # Must be implemented first in the Runtime, then in the bindings common
    # C API and finally add the boolean here
    app_id = 0
    if is_psco(obj):
        obj_id = get_id(obj)
        if obj_id not in OT.get_pending_to_synchronize_objids():
            return obj
        else:
            # file_path is of the form storage://pscoId or
            # file://sys_path_to_file
            file_path = COMPSs.open_file(app_id, "storage://" + str(obj_id), mode)
            # TODO: Add switch on protocol
            protocol, file_name = file_path.split('://')
            new_obj = get_by_id(file_name)
            return new_obj

    obj_id = OT.get_object_id(obj)
    if obj_id not in OT.get_pending_to_synchronize_objids():
        return obj

    if __debug__:
        logger.debug("Synchronizing object %s with mode %s" % (obj_id, mode))

    file_name = OT.get_filename(obj_id)
    compss_file = COMPSs.open_file(app_id, file_name, mode)

    # Runtime can return a path or a PSCOId
    if compss_file.startswith('/'):
        # If the real filename is null, then return None. The task that
        # produces the output file may have been ignored or cancelled, so its
        # result does not exist.
        real_file_name = compss_file.split('/')[-1]
        if real_file_name == 'null':
            print("WARNING: Could not retrieve the object " + str(file_name) +
                  " since the task that produces it may have been IGNORED or CANCELLED. Please, check the logs. Returning None.")  # noqa: E501
            return None
        new_obj = deserialize_from_file(compss_file)
        COMPSs.close_file(app_id, file_name, mode)
    else:
        new_obj = get_by_id(compss_file)

    if mode == 'r':
        new_obj_id = OT.get_object_id(new_obj, True, True)
        # The main program won't work with the old object anymore, update
        # mapping
        OT.set_filename(new_obj_id, OT.get_filename(obj_id).replace(obj_id, new_obj_id))
        OT.set_written_obj(new_obj_id, OT.get_filename(new_obj_id))

    if mode != 'r':
        COMPSs.delete_file(app_id, OT.get_filename(obj_id), False)
        OT.pop_filename(obj_id)
        OT.pop_pending_to_synchronize(obj_id)
        OT.pop_object_id(obj)

    return new_obj


def _wait_on_iterable(iter_obj, compss_mode):
    """
    Wait on an iterable object.
    Currently supports lists and dictionaries (syncs the values).

    :param iter_obj: iterable object
    :return: synchronized object
    """
    # check if the object is in our pending_to_synchronize dictionary
    obj_id = OT.get_object_id(iter_obj)
    if obj_id in OT.get_pending_to_synchronize_objids():
        return _synchronize(iter_obj, compss_mode)
    else:
        if type(iter_obj) == list:
            return [_wait_on_iterable(x, compss_mode)
                    for x in iter_obj]
        elif type(iter_obj) == dict:
            return {k: _wait_on_iterable(v, compss_mode)
                    for k, v in iter_obj.items()}
        else:
            return _synchronize(iter_obj, compss_mode)
