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
PyCOMPSs Binding - Management - Object tracker.

This file contains the object tracking functionality.
"""

import os
import time
import uuid

from pycompss.runtime.commons import GLOBALS
from pycompss.runtime.management.COMPSs import COMPSs
from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing

if __debug__:
    import logging

    LOGGER = logging.getLogger(__name__)


class ObjectTracker:  # pylint: disable=too-many-instance-attributes
    """Object tracker class.

    This class has all needed data structures and functionalities
    to keep track of the objects within the python binding.
    """

    __slots__ = [
        "file_names",
        "obj_names",
        "pending_to_synchronize",
        "written_objects",
        "current_id",
        "runtime_id",
        "obj_id_to_obj",
        "address_to_obj_id",
        "reporting",
        "reporting_info",
        "initial_time",
    ]

    def __init__(self) -> None:
        """Object tracker constructor.

        :returns: None.
        """
        # Dictionary to contain the conversion from object id to the
        # filename where it is stored (mapping).
        # The filename will be used for requesting an object to
        # the runtime (its corresponding version).
        self.file_names = {}  # type: typing.Dict[str, str]
        # Dictionary to contain the conversion from object id to the
        # parameter name (object name mapping).
        # The object name will be used to map the tracked objects with the
        # updated within a task (e.g. synchronize within task).
        self.obj_names = {}  # type: typing.Dict[str, typing.Optional[str]]
        # Set that contains the object identifiers of the objects to pending
        # to be synchronized.
        self.pending_to_synchronize = set()  # type: typing.Set[str]
        # Set of identifiers of the objects that have been accessed by the
        # main program
        self.written_objects = set()  # type: typing.Set[str]
        # Identifier handling
        self.current_id = 1
        # Object identifiers will be of the form _runtime_id-_current_id
        # This way we avoid having two objects from different applications
        # having the same identifier
        self.runtime_id = str(uuid.uuid1())
        # Dictionary to contain the conversion from object identifier to
        # the object (address pointer).
        # NOTE: it can not be done in the other way since the memory addresses
        #       can be reused, not guaranteeing their uniqueness, and causing
        #       weird behaviour.
        self.obj_id_to_obj = {}  # type: typing.Dict[str, typing.Any]
        # Dictionary to contain the object address (currently the id(obj)) to
        # the identifier provided by the binding.
        self.address_to_obj_id = {}  # type: typing.Dict[typing.Any, str]

        # Boolean to store tracking information
        # CAUTION: Enabling reporting increases the memory usage since
        #          it requires storing internally the object tracker status
        #          when a new object is tracked or stopped tracking.
        self.reporting = False
        # Report info: Contains tuples composed by the values to be reported.
        self.reporting_info = []  # type: typing.List[tuple]
        # Store the initial time as reference for the reporting.
        self.initial_time = 0.0

    def track(
        self,
        obj: typing.Any,
        obj_name: typing.Optional[str] = None,
        collection: bool = False,
    ) -> typing.Tuple[str, str]:
        """Start tracking an object.

        Collections are not stored into a file. Consequently, we just register
        it to keep track of the identifier, but no file is stored. However,
        the collection elements are stored into files.

        :param obj: Object to track.
        :param obj_name: Object name (variable placeholder name).
        :param collection: If the object to be tracked is a collection.
        :return: Object identifier and its corresponding file name.
        """
        if collection:
            obj_id = self._register_object(obj, True)
            file_name = "None"
            if __debug__:
                LOGGER.debug("Tracking collection %s", obj_id)
        else:
            obj_id = self._register_object(obj, True)
            file_name = f"{GLOBALS.get_temporary_directory()}/{str(obj_id)}"
            self._set_file_name(obj_id, file_name)
            self._set_obj_name(obj_id, obj_name)
            self.set_pending_to_synchronize(obj_id)
            if __debug__:
                LOGGER.debug(
                    "Tracking object %s to file %s", obj_id, file_name
                )
        address = self._get_object_address(obj)
        self.address_to_obj_id[address] = obj_id
        if self.reporting:
            self.report_now()
        return obj_id, file_name

    def not_track(self, collection: bool = False) -> typing.Tuple[str, str]:
        """Retrieve a not tracked identifier and file_name.

        :param collection: If the object is a collection.
        :returns: Object identifier and file name.
        """
        obj_id = f"{self.runtime_id}-{self.current_id}"
        if collection:
            file_name = "None"
        else:
            file_name = f"{GLOBALS.get_temporary_directory()}/{str(obj_id)}"
        self.current_id += 1
        return obj_id, file_name

    def stop_tracking(self, obj: typing.Any, collection: bool = False) -> None:
        """Stop tracking the given object.

        :param obj: Object to stop tracking.
        :param collection: If the object to stop tracking is a collection.
        :return: None.
        """
        obj_id = self.is_tracked(obj)
        if obj_id != "":
            if collection:
                if __debug__:
                    LOGGER.debug("Stop tracking collection %s", obj_id)
                self._pop_object_id(obj_id)
            else:
                if __debug__:
                    LOGGER.debug("Stop tracking object %s", obj_id)
                self._delete_file_name(obj_id)
                self._delete_obj_name(obj_id)
                self._remove_from_pending_to_synchronize(obj_id)
                self._pop_object_id(obj_id)
        self.report_now()

    def get_object_id(self, obj: typing.Any) -> str:
        """Return the object identifier of the given object.

        This function is a wrapper of is_tracked.

        :param obj: Object to check.
        :return: Object identifier if under tracking. Empty string otherwise.
        """
        return self.is_tracked(obj)

    def is_tracked(self, obj: typing.Any) -> str:
        """Check if the given object is being tracked.

        Due to the length that the obj_id_to_address dictionary can reach, if
        is tracked we return the identifier in order to avoid to search again
        into the dictionary.

        :param obj: Object to check.
        :return: Object identifier if under tracking. Empty string otherwise.
        """
        address = self._get_object_address(obj)
        if address in self.address_to_obj_id:
            return self.address_to_obj_id[address]
        return ""

    def get_all_file_names(self) -> tuple:
        """Return all used files names.

        Useful for cleanup.

        :return: List of file name that are currently available.
        """
        return tuple(self.file_names.values())

    def get_file_name(self, obj_id: str) -> str:
        """Get the file name associated to the given object identifier.

        :param obj_id: Object identifier.
        :return: File name.
        """
        return self.file_names[obj_id]

    def get_obj_name(self, obj_id: str) -> typing.Optional[str]:
        """Get the object name associated to the given object identifier.

        :param obj_id: Object identifier.
        :return: Object name.
        """
        if obj_id:
            return self.obj_names[obj_id]
        return None

    def is_obj_pending_to_synchronize(self, obj: typing.Any) -> bool:
        """Check if the given object is pending to be synchronized.

        :param obj: Object to check.
        :return: True if pending. False otherwise.
        """
        obj_id = self.is_tracked(obj)
        if obj_id == "":
            return False
        return self.is_pending_to_synchronize(obj_id)

    def is_pending_to_synchronize(self, obj_id: str) -> bool:
        """Check if the given object id is in pending to be synchronized dict.

        :param obj_id: Object identifier.
        :return: True if pending. False otherwise.
        """
        return obj_id in self.pending_to_synchronize

    def set_pending_to_synchronize(self, obj_id: str) -> None:
        """Set the given filename with object id as pending to synchronize.

        :param obj_id: Object identifier.
        :return: None
        """
        self.pending_to_synchronize.add(obj_id)

    def has_been_written(self, obj_id: str) -> bool:
        """Check if the given object id has been written by the main program.

        :param obj_id: Object identifier.
        :return: True if written. False otherwise.
        """
        return obj_id in self.written_objects

    def pop_written_obj(self, obj_id: str) -> str:
        """Pop a written filename with the given obj_id from written objects.

        :param obj_id: Object identifier.
        :return: The file name.
        """
        self.written_objects.remove(obj_id)
        return self.get_file_name(obj_id)

    def update_mapping(self, obj_id: str, obj: typing.Any) -> None:
        """Update the object into the object tracker.

        :param obj_id: Object identifier.
        :param obj: New object to track.
        :return: None.
        """
        # The main program won't work with the old object anymore, update
        # mapping
        new_obj_id = self._register_object(obj, True, True)
        old_file_name = self.get_file_name(obj_id)
        new_file_name = old_file_name.replace(obj_id, str(new_obj_id))
        self._set_file_name(new_obj_id, new_file_name, written=True)

    def clean_object_tracker(self, hard_stop: bool = False) -> None:
        """Clear all object tracker internal structures.

        :param hard_stop: avoid call to delete_file when the runtime has died.
        :return: None
        """
        app_id = 0
        if not hard_stop:
            for filename in OT.get_all_file_names():
                COMPSs.delete_file(app_id, filename, False)

        self.file_names.clear()
        self.obj_names.clear()
        self.pending_to_synchronize.clear()
        self.written_objects.clear()
        self.current_id = 1
        self.runtime_id = str(uuid.uuid1())
        self.obj_id_to_obj.clear()
        self.address_to_obj_id.clear()
        self.report_now()

    def clean_report(self) -> None:
        """Clear the reporting data.

        :return: None.
        """
        del self.reporting_info[:]

    #############################################
    #            PRIVATE FUNCTIONS              #
    #############################################

    def _register_object(
        self,
        obj: typing.Any,
        assign_new_key: bool = False,
        force_insertion: bool = False,
    ) -> str:
        """Register an object into the object tracker.

        If not found or we are forced to, we create a new identifier for this
        object, deleting the old one if necessary. We can also query for some
        object without adding it in case of failure.

        Identifiers are of the form _runtime_id-_current_id in order to avoid
        having two objects from different applications with the same identifier
        (and thus file name).
        This function updates the internal self.current_id to guarantee
        that each time returns a new identifier.

        :param obj: Object to analyse.
        :param assign_new_key: Assign new key.
        :param force_insertion: force insertion.
        :return: Object id.
        """
        # Force_insertion implies assign_new_key
        if not (not force_insertion or assign_new_key):
            raise PyCOMPSsException("Force insertion implies assign new key")

        identifier = self.is_tracked(obj)
        if identifier != "":
            if force_insertion:
                self.obj_id_to_obj.pop(identifier)
                address = self._get_object_address(obj)
                self.address_to_obj_id.pop(address)
            else:
                return identifier

        if assign_new_key:
            # This object was not in our object database or we were forced to
            # remove it, lets assign it an identifier and store it.
            # Generate a new identifier
            new_id = f"{self.runtime_id}-{self.current_id}"
            self.current_id += 1
            self.obj_id_to_obj[new_id] = obj
            address = self._get_object_address(obj)
            self.address_to_obj_id[address] = new_id
            return new_id

        raise PyCOMPSsException("Reached unexpected object registry case.")

    def _set_file_name(
        self, obj_id: str, filename: str, written: bool = False
    ) -> None:
        """Set a filename for the given object identifier.

        :param obj_id: Object identifier.
        :param filename: File name.
        :param written: If the file has been written by main program
        :return: None
        """
        self.file_names[obj_id] = filename
        if written:
            self.written_objects.add(obj_id)

    def _delete_file_name(self, obj_id: str) -> None:
        """Remove the file name of the given object identifier.

        :param obj_id: Object identifier.
        :return: None
        """
        del self.file_names[obj_id]

    def _set_obj_name(
        self, obj_id: str, obj_name: typing.Optional[str]
    ) -> None:
        """Set the object name for the given object identifier.

        :param obj_id: Object identifier.
        :param obj_name: Object name.
        :return: None
        """
        self.obj_names[obj_id] = obj_name

    def _delete_obj_name(self, obj_id: str) -> None:
        """Remove the object name of the given object identifier.

        :param obj_id: Object identifier.
        :return: None
        """
        del self.obj_names[obj_id]

    def _remove_from_pending_to_synchronize(self, obj_id: str) -> None:
        """Pop the filename of the given object id from pending to synchronize.

        :param obj_id: Object identifier.
        :return: None
        """
        self.pending_to_synchronize.remove(obj_id)

    def _pop_object_id(self, obj_id: str) -> typing.Any:
        """Pop an object from the dictionary.

        :param obj_id: Object identifier to pop.
        :return: Popped object.
        """
        obj = self.obj_id_to_obj.pop(obj_id)
        address = self._get_object_address(obj)
        return self.address_to_obj_id.pop(address)

    @staticmethod
    def _get_object_address(obj: typing.Any) -> int:
        """Retrieve the object memory address.

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
        #     # Consider the memory pointer:
        #     hash_id.update(str(id(obj)).encode())
        #     # Include the object size:
        #     hash_id.update(str(total_sizeof(obj)).encode())
        #     # Include the object representation:
        #     hash_id.update(repr(obj).encode())
        #     obj_address = str(hash_id.hexdigest())
        # return obj_address

    #############################################
    #           REPORTING FUNCTIONS             #
    #############################################

    def enable_report(self) -> None:
        """Enable reporting.

        Enables to keep the status in internal infrastructure so that
        the report can be generated afterwards.

        :return: None
        """
        self.reporting = True
        # Get initial reporting status
        self.report_now(first=True)

    def is_report_enabled(self) -> bool:
        """Retrieve if the reporting is enabled.

        :return: If the object tracker is keeping track of the status.
        """
        return self.reporting

    def report_now(self, first: bool = False) -> None:
        """Update the report with the current Object Tracker status.

        WARNING: This function only works if log_level=debug.

        :param first: If it is the first time reporting the status.
        :return: None
        """
        if __debug__ and self.reporting:
            # Log the object tracker status
            self.__log_object_tracker_status__()
            self.__update_report__(first)

    def __log_object_tracker_status__(self) -> None:
        """Log the object tracker status.

        :return: None
        """
        LOGGER.debug(
            "Object tracker status:\n"
            " - File_names=%s\n"
            " - Pending_to_synchronize=%s\n"
            " - Written_objs=%s\n"
            " - Obj_id_to_obj=%s\n"
            " - Address_to_obj_id=%s\n"
            " - Current_id=%s",
            str(len(self.file_names)),
            str(len(self.pending_to_synchronize)),
            str(len(self.written_objects)),
            str(len(self.obj_id_to_obj)),
            str(len(self.address_to_obj_id)),
            str(self.current_id),
        )

    def __update_report__(self, first: bool = False) -> None:
        """Update the reporting.

        Update the internal self.report_info variable with the
        current object tracker status.

        :param first: If it is the first time reporting the status.
        :return: None
        """
        if first:
            self.initial_time = time.time()
        current_status = (
            time.time() - self.initial_time,
            (
                len(self.file_names),
                len(self.pending_to_synchronize),
                len(self.written_objects),
                len(self.obj_id_to_obj),
                len(self.address_to_obj_id),
            ),
        )
        self.reporting_info.append(current_status)

    def generate_report(self, target_path: str) -> None:
        """Generate a plot reporting the behaviour of the object tracker.

        Uses the self.report_info internal variable contents.

        :param target_path: Path where to store the report.
        :return: None
        """
        try:
            import matplotlib  # pylint: disable=import-outside-toplevel

            # Agg avoid issues in MN
            matplotlib.use("Agg")
            import matplotlib.pyplot as plt  # pylint: disable=C0415

            # disable=import-outside-toplevel
        except ImportError:
            print("WARNING: Could not generate the Object Tracker report")
            print("REASON : matplotlib not available.")
            return None
        if __debug__:
            LOGGER.debug("Generating object tracker report...")
        x_axis = [status[0] for status in self.reporting_info]
        y_axis = [status[1] for status in self.reporting_info]
        plt.xlabel("Time (seconds)")
        plt.ylabel("# Elements")
        plt.title("Object tracker behaviour")
        labels = [
            "File names",
            "Pending to synchronize",
            "Updated mappings",
            "IDs",
            "Addresses",
        ]
        for i in range(len(y_axis[0])):
            plt.plot(x_axis, [pt[i] for pt in y_axis], label=f"{labels[i]}")
        plt.legend()
        target = os.path.join(target_path, "object_tracker.png")
        plt.savefig(target)
        if __debug__:
            LOGGER.debug("Object tracker report stored in %s", target)
        return None


# Instantiation of the Object tracker class to be shared among
# management modules
OT = ObjectTracker()
