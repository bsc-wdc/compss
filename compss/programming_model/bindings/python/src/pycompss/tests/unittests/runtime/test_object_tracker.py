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

import os

from pycompss.runtime.management.object_tracker import ObjectTracker

ERROR_ID_NONE = "The identifier can not be None."
ERROR_ID_STRING = "The identifier must be a string."
ERROR_ID_EMPTY = "The identifier must not be empty."
ERROR_ID_DIFFERENT = (
    "Tracked identifier differs from returned by track function."  # noqa: E501
)
ERROR_FILENAME_EMPTY = "The file name can not be empty."


class DummyObject(object):
    def __init__(self):
        self.value = 1


def test_track():
    object_tracker = ObjectTracker()
    do = DummyObject()
    do_id, do_file_name = object_tracker.track(do)
    assert object_tracker.is_tracked(do) is not None, ERROR_ID_NONE
    assert isinstance(object_tracker.is_tracked(do), str), ERROR_ID_STRING
    assert object_tracker.is_tracked(do) != "", ERROR_ID_EMPTY
    assert object_tracker.is_tracked(do) == do_id, ERROR_ID_DIFFERENT
    assert do_file_name != "", ERROR_FILENAME_EMPTY


def test_track_twice():
    object_tracker = ObjectTracker()
    do = DummyObject()
    _ = object_tracker.track(do)
    do_id, do_file_name = object_tracker.track(do)
    assert object_tracker.is_tracked(do) is not None, ERROR_ID_NONE
    assert isinstance(object_tracker.is_tracked(do), str), ERROR_ID_STRING
    assert object_tracker.is_tracked(do) != "", ERROR_ID_EMPTY
    assert object_tracker.is_tracked(do) == do_id, ERROR_ID_DIFFERENT
    assert do_file_name != "", ERROR_FILENAME_EMPTY


def test_track_collection():
    object_tracker = ObjectTracker()
    my_collection = [DummyObject(), DummyObject()]
    collection_id, collection_file_name = object_tracker.track(
        my_collection, collection=True
    )
    assert object_tracker.is_tracked(my_collection) is not None, ERROR_ID_NONE
    assert isinstance(
        object_tracker.is_tracked(my_collection), str
    ), ERROR_ID_STRING  # noqa: E501
    assert (
        object_tracker.is_tracked(my_collection) != ""
        and object_tracker.is_tracked(my_collection) != "None"
    ), ERROR_ID_EMPTY
    assert (
        object_tracker.is_tracked(my_collection) == collection_id
    ), ERROR_ID_DIFFERENT  # noqa: E501
    assert (
        collection_file_name != "" or collection_file_name != "None"
    ), "The file name must be None for collections."


def test_stop_tracking():
    object_tracker = ObjectTracker()
    do = DummyObject()
    do_id, do_file_name = object_tracker.track(do)
    assert (
        object_tracker.is_tracked(do) != ""
        and object_tracker.is_tracked(do) != "None"  # noqa: E501
    ), ERROR_ID_NONE
    assert isinstance(object_tracker.is_tracked(do), str), ERROR_ID_STRING
    assert (
        object_tracker.is_tracked(do) != ""
        and object_tracker.is_tracked(do) != "None"  # noqa: E501
    ), ERROR_ID_EMPTY
    assert object_tracker.is_tracked(do) == do_id, ERROR_ID_DIFFERENT
    # The object do is being tracked
    object_tracker.stop_tracking(do)
    assert (
        object_tracker.is_tracked(do) == ""
        or object_tracker.is_tracked(do) == "None"  # noqa: E501
    ), "The identifier must be None after stop tracking"
    assert do_file_name != "", ERROR_FILENAME_EMPTY


def test_stop_tracking_collection():
    object_tracker = ObjectTracker()
    my_collection = [DummyObject(), DummyObject()]
    collection_id, collection_file_name = object_tracker.track(
        my_collection, collection=True
    )
    assert object_tracker.is_tracked(my_collection) != "", ERROR_ID_NONE
    assert isinstance(
        object_tracker.is_tracked(my_collection), str
    ), ERROR_ID_STRING  # noqa: E501
    assert (
        object_tracker.is_tracked(my_collection) != ""
        and object_tracker.is_tracked(my_collection) != "None"
    ), ERROR_ID_EMPTY
    assert (
        object_tracker.is_tracked(my_collection) == collection_id
    ), ERROR_ID_DIFFERENT  # noqa: E501
    # The collection is being tracked
    object_tracker.stop_tracking(my_collection, collection=True)
    assert (
        object_tracker.is_tracked(my_collection) == ""
    ), "The identifier must be None after stop tracking"
    assert (
        collection_file_name == "None"
    ), "The file name must be None for collections."  # noqa: E501


def test_get_object_id():
    object_tracker = ObjectTracker()
    do = DummyObject()
    do_id, do_file_name = object_tracker.track(do)
    do_get_obj_id = object_tracker.get_object_id(do)
    assert do_id == do_get_obj_id, "The object identifiers are different!"
    assert do_file_name != "", ERROR_FILENAME_EMPTY


def test_not_tracking_empty():
    object_tracker = ObjectTracker()
    do = DummyObject()
    assert (
        object_tracker.is_tracked(do) == ""
    ), "The object seems to be tracked."  # noqa: E501


def test_not_tracking_not_empty():
    object_tracker = ObjectTracker()
    do = DummyObject()
    _, _ = object_tracker.track(do)
    do2 = DummyObject()
    assert (
        object_tracker.is_tracked(do2) == ""
    ), "The object seems to be tracked."  # noqa: E501


def test_get_all_file_names():
    object_tracker = ObjectTracker()
    do = DummyObject()
    do2 = DummyObject()
    _, _ = object_tracker.track(do)
    _, _ = object_tracker.track(do2)
    file_names = object_tracker.get_all_file_names()
    assert (
        len(file_names) == 2
    ), "Two elements should be being tracked: %s" % str(  # noqa: E501
        file_names
    )


def test_get_file_name():
    object_tracker = ObjectTracker()
    do = DummyObject()
    do_id, do_file_name = object_tracker.track(do)
    file_name = object_tracker.get_file_name(do_id)
    assert file_name is not None, "The file name can not be None."
    assert isinstance(file_name, str), "The file name must be a string."
    assert file_name != "", "The file name must not be empty."
    assert do_file_name != "", ERROR_FILENAME_EMPTY
    assert do_file_name == file_name, "The file name received wrong file name."


def test_obj_not_pending_to_synchronize():
    object_tracker = ObjectTracker()
    do = DummyObject()
    # The object is being tracked
    pending = object_tracker.is_obj_pending_to_synchronize(do)
    assert pending is False, "The object should not be pending to synchronize."


def test_not_pending_to_synchronize():
    object_tracker = ObjectTracker()
    # The object is being tracked
    pending = object_tracker.is_pending_to_synchronize("IMPOSSIBLE_ID")
    assert pending is False, "The object should not be pending to synchronize."


def test_obj_pending_to_synchronize():
    object_tracker = ObjectTracker()
    do = DummyObject()
    _, _ = object_tracker.track(do)
    # The object is being tracked
    pending = object_tracker.is_obj_pending_to_synchronize(do)
    assert (
        pending is True
    ), "The object must be pending to synchronize after tracking."  # noqa: E501


def test_pending_to_synchronize():
    object_tracker = ObjectTracker()
    do = DummyObject()
    do_id, _ = object_tracker.track(do)
    # The object is being tracked
    pending = object_tracker.is_pending_to_synchronize(do_id)
    assert (
        pending is True
    ), "The object must be pending to synchronize after tracking."  # noqa: E501


def test_update_mapping():
    object_tracker = ObjectTracker()
    do = DummyObject()
    do_id, _ = object_tracker.track(do)
    # The object is being tracked
    written = object_tracker.has_been_written(do_id)
    assert (
        written is False
    ), "The object identifier must not be in written_objects after tracking."  # noqa: E501
    object_tracker.update_mapping(do_id, do)
    new_id = object_tracker.get_object_id(do)
    assert (
        do_id != new_id
    ), "The identifiers must not be equal after updating the mapping."  # noqa: E501
    written = object_tracker.has_been_written(new_id)
    assert (
        written is True
    ), "The object's new identifier must be in written_objects after updating its mapping."  # noqa: E501
    file_name = object_tracker.pop_written_obj(new_id)
    assert file_name is not None, "The object file name must not be None."
    assert isinstance(file_name, str), "The object file name must be string."
    assert file_name != "", "The object file name must not be empty."
    written = object_tracker.has_been_written(new_id)
    assert (
        written is False
    ), "The object's new identifier must not be in written_objects after popping."  # noqa: E501


def test_clean_object_tracker():
    object_tracker = ObjectTracker()
    do = DummyObject()
    _, _ = object_tracker.track(do)
    object_tracker.clean_object_tracker()
    assert len(object_tracker.pending_to_synchronize) == 0
    assert len(object_tracker.file_names) == 0
    assert len(object_tracker.written_objects) == 0
    assert len(object_tracker.obj_id_to_obj) == 0
    assert len(object_tracker.address_to_obj_id) == 0


def test_report():
    object_tracker = ObjectTracker()
    object_tracker.enable_report()
    assert (
        object_tracker.is_report_enabled() is True
    ), "Reporting must be enabled."  # noqa: E501
    do = DummyObject()
    _, _ = object_tracker.track(do)
    object_tracker.stop_tracking(do)
    object_tracker.generate_report(".")
    report = "object_tracker.png"
    generated = os.path.exists(report) and os.path.isfile(report)
    assert (
        generated is True
    ), "Report result image has not been generated."  # noqa: E501
    if generated:
        os.remove(report)
    object_tracker.clean_report()
    assert (
        len(object_tracker.reporting_info) == 0
    ), "The reporting info list has not been cleared!"  # noqa: E501
