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
import sys

import dill
import numpy
import numpy as np

from pycompss.tests.outlog import create_logger

if sys.version_info >= (3, 0):
    import pickle as pickle  # Uses _pickle if available
else:
    import cPickle as pickle  # noqa

LOGGER = create_logger()


def test_get_serializer_priority():
    from pycompss.util.serialization.serializer import get_serializer_priority

    priority = get_serializer_priority([1, 2, 3], logger=LOGGER)
    assert priority == [pickle, dill], "ERROR: Received wrong priority."

    priority = get_serializer_priority(np.random.rand(2, 2), LOGGER)
    assert priority == [
        numpy,
        pickle,
        dill,
    ], "ERROR: Received wrong priority with numpy object."


def test_get_serializers():
    from pycompss.util.serialization.serializer import get_serializer_priority

    serializers = get_serializer_priority(LOGGER, logger=LOGGER)
    assert serializers == [
        pickle,
        dill,
    ], "ERROR: Received wrong serializers. " + str(serializers)


def test_serialize_deserialize_obj_to_file():
    # Uses serialize to handler underneath.
    from pycompss.util.serialization.serializer import serialize_to_file
    from pycompss.util.serialization.serializer import deserialize_from_file

    target_file = "target.pkl"
    obj = [1, 3, 2, "hello", "world"]
    serialize_to_file(obj, target_file, logger=LOGGER)
    result = deserialize_from_file(target_file, logger=LOGGER)
    os.remove(target_file)
    assert (
        obj == result
    ), "ERROR: Object serialization and deserialization retrieved wrong object."  # noqa: E501


def test_serialize_deserialize_np_to_file():
    # Uses serialize to handler underneath.
    from pycompss.util.serialization.serializer import serialize_to_file
    from pycompss.util.serialization.serializer import deserialize_from_file

    target_file_np = "target_np.pkl"
    obj_np = np.random.rand(4, 4)
    serialize_to_file(obj_np, target_file_np, logger=LOGGER)
    result_np = deserialize_from_file(target_file_np, logger=LOGGER)
    os.remove(target_file_np)
    assert np.array_equal(
        obj_np, result_np
    ), "ERROR: Numpy object serialization and deserialization retrieved wrong object."  # noqa: E501


def test_serialize_deserialize_obj_to_file_no_gc():
    # Uses serialize to handler underneath.
    import pycompss.util.serialization.serializer as serializer
    from pycompss.util.serialization.serializer import serialize_to_file
    from pycompss.util.serialization.serializer import deserialize_from_file

    serializer.DISABLE_GC = True
    target_file = "target.pkl"
    obj = [1, 3, 2, "hello", "world"]
    serialize_to_file(obj, target_file, logger=LOGGER)
    result = deserialize_from_file(target_file, logger=LOGGER)
    os.remove(target_file)
    assert (
        obj == result
    ), "ERROR: Object serialization and deserialization (without garbage collector) retrieved wrong object."  # noqa: E501


def test_serialize_deserialize_np_to_file_no_gc():
    # Uses serialize to handler underneath.
    import pycompss.util.serialization.serializer as serializer
    from pycompss.util.serialization.serializer import serialize_to_file
    from pycompss.util.serialization.serializer import deserialize_from_file

    serializer.DISABLE_GC = True
    target_file_np = "target_np.pkl"
    obj_np = np.random.rand(4, 4)
    serialize_to_file(obj_np, target_file_np, logger=LOGGER)
    result_np = deserialize_from_file(target_file_np, logger=LOGGER)
    os.remove(target_file_np)
    assert np.array_equal(
        obj_np, result_np
    ), "ERROR: Numpy object serialization and deserialization (without garbage collector) retrieved wrong object."  # noqa: E501


# def test_serialize_deserialize_string():
#     obj = ["hello", 1, "world", 2, [5, 4, 3, 2, 1], None]
#     from pycompss.util.serialization.serializer import serialize_to_bytes
#     from pycompss.util.serialization.serializer import deserialize_from_bytes
#
#     serialized = serialize_to_bytes(obj)
#     result = deserialize_from_bytes(serialized)
#     assert (
#         result == obj
#     ), "ERROR: Serialization and deserialization to/from string retrieved wrong object."  # noqa: E501


def test_serialize_objects():
    from pycompss.util.serialization.serializer import serialize_objects
    from pycompss.util.serialization.serializer import deserialize_from_file

    obj1 = ([1, 2, 3, 4], "obj1.pkl")
    obj2 = ({"hello": "mars", "goodbye": "world"}, "obj2.pkl")
    obj3 = (np.random.rand(3, 3), "obj3.pkl")
    objects = [obj1, obj2, obj3]
    serialize_objects(objects, logger=LOGGER)
    result = []
    for obj in objects:
        result.append(deserialize_from_file(obj[1], logger=LOGGER))
    os.remove(obj1[1])
    os.remove(obj2[1])
    os.remove(obj3[1])
    assert len(result) == len(
        objects
    ), "ERROR: Wrong number of objects retrieved."
    assert result[0] == obj1[0], "ERROR: Wrong first object."
    assert result[1] == obj2[0], "ERROR: Wrong second object."
    assert np.array_equal(result[2], obj3[0]), "ERROR: Wrong third object."
