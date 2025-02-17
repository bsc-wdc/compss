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
PyCOMPSs Binding - Management - Object Synchronization.

This file contains the object synchronization core methods.
"""

import logging
from pycompss.runtime.management.COMPSs import COMPSs
from pycompss.util.context import CONTEXT
from pycompss.runtime.management.classes import Future
from pycompss.runtime.management.direction import get_compss_direction
from pycompss.runtime.management.object_tracker import OT
from pycompss.runtime.task.shared_args import SHARED_ARGUMENTS
from pycompss.util.serialization.serializer import deserialize_from_file
from pycompss.util.storages.persistent import get_by_id
from pycompss.util.storages.persistent import get_id
from pycompss.util.storages.persistent import is_psco
from pycompss.util.typing_helper import typing


LOGGER = logging.getLogger(__name__)


def wait_on_object(obj: typing.Any, mode: str) -> typing.Any:
    """Wait on an object (synchronize).

    :param obj: Object to wait on.
    :param mode: Read or write mode
    :return: An object of "file" type.
    """
    compss_mode = get_compss_direction(mode)
    if isinstance(obj, Future) or not isinstance(obj, (list, dict)):
        return _synchronize(obj, compss_mode)
    if len(obj) == 0:  # FUTURE OBJECT
        return _synchronize(obj, compss_mode)
    # Otherwise, will be an iterable object

    return _wait_on_iterable(obj, compss_mode)


def _synchronize(obj: typing.Any, mode: int) -> typing.Any:
    """Synchronize the given object.

    This method retrieves the value of a future object.
    Calls the runtime in order to wait for the value and returns it when
    received.

    :param obj: Object to synchronize.
    :param mode: Direction of the object to synchronize.
    :return: The value of the object requested.
    """
    # TODO: Add a boolean to differentiate between files and object on the
    # COMPSs.open_file call. This change pretends to obtain better traces.
    # Must be implemented first in the Runtime, then in the bindings common
    # C API and finally add the boolean here
    app_id = 0
    obj_id = ""  # noqa

    if is_psco(obj):
        obj_id = str(get_id(obj))
        if not OT.is_pending_to_synchronize(obj_id):
            return obj
        # file_path is of the form storage://pscoId or
        # file://sys_path_to_file
        file_path = COMPSs.open_file(
            app_id, "".join(("storage://", str(obj_id))), mode
        )
        # TODO: Add switch on protocol
        #       (first parameter returned currently ignored)
        _, file_name = file_path.split("://")
        new_obj = get_by_id(file_name)
        OT.stop_tracking(obj)
        return new_obj

    obj_id = OT.is_tracked(obj)
    obj_name = OT.get_obj_name(obj_id)
    if obj_id is None:  # Not being tracked
        return obj
    if not OT.is_pending_to_synchronize(obj_id):
        return obj

    if __debug__:
        LOGGER.debug("Synchronizing object %s with mode %s", obj_id, mode)
    file_name = OT.get_file_name(obj_id)
    compss_file = COMPSs.open_file(app_id, file_name, mode)
    # Runtime can return a path or a PSCOId
    if compss_file.startswith("/"):
        # If the real filename is null, then return None. The task that
        # produces the output file may have been ignored or cancelled, so its
        # result does not exist.
        real_file_name = compss_file.split("/")[-1]
        if real_file_name == "null":
            print(
                "WARNING: Could not retrieve the object "
                + str(file_name)
                + " since the task that produces it may have been IGNORED or"
                + " CANCELLED. Please, check the logs. Returning None."
            )
            return None
        new_obj = deserialize_from_file(compss_file, LOGGER)
        COMPSs.close_file(app_id, file_name, mode)
    else:
        new_obj = get_by_id(compss_file)

    if CONTEXT.is_nesting_enabled() and CONTEXT.in_worker():
        # If nesting and in worker, the user wants to synchronize an object.
        # So it is necessary to update the parameter with the new object.
        SHARED_ARGUMENTS.update_worker_argument_parameter_content(
            obj_name, new_obj
        )

    if mode == "r":
        OT.update_mapping(obj_id, new_obj)

    if mode != "r":
        COMPSs.delete_file(app_id, OT.get_file_name(obj_id), False)
        OT.stop_tracking(obj)

    return new_obj


def _wait_on_iterable(iter_obj: typing.Any, compss_mode: int) -> typing.Any:
    """Wait on an iterable object (Recursive).

    Currently, supports lists and dictionaries (syncs the values).

    :param iter_obj: Iterable object.
    :return: Synchronized object.
    """
    # check if the object is in our pending_to_synchronize dictionary
    obj_id = OT.is_tracked(iter_obj)
    if OT.is_pending_to_synchronize(obj_id):
        return _synchronize(iter_obj, compss_mode)
    if isinstance(iter_obj, list):
        return [_wait_on_iterable(x, compss_mode) for x in iter_obj]
    if isinstance(iter_obj, dict):
        return {
            _wait_on_iterable(k, compss_mode): _wait_on_iterable(
                v, compss_mode
            )
            for k, v in iter_obj.items()
        }
    return _synchronize(iter_obj, compss_mode)
