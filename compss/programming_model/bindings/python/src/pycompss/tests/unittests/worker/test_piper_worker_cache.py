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

import sys

from pycompss.worker.piper.cache.setup import is_cache_enabled

NOT_PYTHON_3_8 = (
    "WARNING: Could not perform cache test since python "
    "version is lower than 3.8"
)


def test_is_cache_enabled():
    if sys.version_info >= (3, 8):
        case1 = is_cache_enabled("true")
        assert case1, "Unexpected return. Expected: <bool> True"
        case2 = is_cache_enabled("True")
        assert case2, "Unexpected return. Expected: <bool> True"
        case3 = is_cache_enabled("true:1000")
        assert case3, "Unexpected return. Expected: <bool> True"
        case4 = is_cache_enabled("True:1000")
        assert case4, "Unexpected return. Expected: <bool> True"
    else:
        print(NOT_PYTHON_3_8)


# def test_piper_worker_cache():
#     if sys.version_info >= (3, 8):
#         # Initiate cache
#         smm, cache_process, cache_queue, cache_ids = start_cache(logging,
#                                                                  "Default",
#                                                                  False,
#                                                                  "")
#         load_shared_memory_manager()
#         # Supported types:
#         np_obj = np.random.rand(4)
#         np_obj_name = "np_obj"
#         list_obj = [1, 2, 3, 4, 5, "hi"]
#         list_obj_name = "list_obj_name"
#         tuple_obj = ("1", 2, 3, "4", "hi")
#         tuple_obj_name = "tuple_obj_name"
#         # Check insertions
#         insert_object_into_cache_wrapper(logging, cache_queue, np_obj, np_obj_name, np_obj_name, None)  # noqa: E501
#         insert_object_into_cache_wrapper(logging, cache_queue, list_obj, list_obj_name, list_obj_name, None)  # noqa: E501
#         insert_object_into_cache_wrapper(logging, cache_queue, tuple_obj, tuple_obj_name, tuple_obj_name, None)  # noqa: E501
#         # Check retrieves
#         np_obj_new, np_obj_shm = retrieve_object_from_cache(logging, cache_ids, cache_queue, np_obj_name, np_obj_name, None, False)  # noqa: E501
#         list_obj_new, list_obj_shm = retrieve_object_from_cache(logging, cache_ids, cache_queue, list_obj_name, list_obj_name, None, False)  # noqa: E501
#         tuple_obj_new, tuple_obj_shm = retrieve_object_from_cache(logging, cache_ids, cache_queue, tuple_obj_name, tuple_obj_name, None, False)  # noqa: E501
#         assert (
#             set(np_obj_new).intersection(np_obj)
#         ), "ERROR: Numpy object retrieved from cache differs from inserted"
#         assert (
#             set(list_obj_new).intersection(list_obj)
#         ), "ERROR: List retrieved from cache differs from inserted"
#         assert (
#             set(tuple_obj_new).intersection(tuple_obj)
#         ), "ERROR: Tuple retrieved from cache differs from inserted"
#         # Check replace
#         new_list_obj = ["hello", "world", 6]
#         replace_object_into_cache(logging, cache_queue, new_list_obj, list_obj_name, list_obj_name, False)   # noqa: E501
#         time.sleep(0.5)
#         list_obj_new2, list_obj_shm2 = retrieve_object_from_cache(logging, cache_ids, cache_queue, list_obj_name, list_obj_name, None, False)  # noqa: E501
#         assert (
#             set(list_obj_new2).intersection(new_list_obj)
#         ), "ERROR: List retrieved from cache differs from inserted"
#         # Remove object
#         remove_object_from_cache(logging, cache_queue, list_obj_name)
#         time.sleep(0.5)
#         is_ok = False
#         try:
#             _, _ = retrieve_object_from_cache(logging, cache_ids, cache_queue, list_obj_name, list_obj_name, None, False)  # noqa: E501
#         except Exception:  # NOSONAR
#             is_ok = True
#         assert (
#             is_ok
#         ), "ERROR: List has not been removed."
#         # Check if in cache
#         assert (
#             in_cache(np_obj_name, cache_ids)
#         ), "ERROR: numpy object not in cache. And it should be."
#         assert (
#             not in_cache(list_obj_name, cache_ids)
#         ), "ERROR: list object should not be in cache."
#         assert (
#             not in_cache(list_obj_name, {})
#         ), "ERROR: in cache should return False if dict is empty."
#         # Stop cache
#         stop_cache(smm, cache_queue, False, cache_process)
#     else:
#         print(NOT_PYTHON_3_8)
#
#
# def test_piper_worker_cache_stress():
#     if sys.version_info >= (3, 8):
#         # Initiate cache
#         smm, cache_process, cache_queue, cache_ids = start_cache(logging,
#                                                                  "true:100",
#                                                                  False,
#                                                                  "")
#         load_shared_memory_manager()
#         # Create multiple objects:
#         amount = 40
#         np_objs = [np.random.rand(4) for _ in range(amount)]
#         np_objs_names = ["name" + str(i) for i in range(amount)]
#         # Check insertions
#         for i in range(amount):
#             insert_object_into_cache_wrapper(logging, cache_queue,
#                                              np_objs[i], np_objs_names[i],
#                                              np_objs_names[i], None)
#         # Stop cache
#         stop_cache(smm, cache_queue, False, cache_process)
#     else:
#         print(NOT_PYTHON_3_8)
#
#
# def test_piper_worker_cache_reuse():
#     if sys.version_info >= (3, 8):
#         # Initiate cache
#         smm, cache_process, cache_queue, cache_ids = start_cache(logging,
#                                                                  "true:100000",
#                                                                  False,
#                                                                  "")
#         load_shared_memory_manager()
#         # Create multiple objects and store with the same name:
#         amount = 10
#         np_objs = [np.random.rand(4) for _ in range(amount)]
#         obj_name = "name"
#         np_objs_names = [obj_name for _ in range(amount)]
#         # Check insertions
#         for i in range(amount):
#             insert_object_into_cache_wrapper(logging, cache_queue,
#                                              np_objs[i], np_objs_names[i],
#                                              np_objs_names[i], None)
#         if obj_name not in cache_ids:
#             raise Exception("Object " + obj_name +
#                             " not found in cache_ids.")
#         else:
#             if cache_ids[obj_name][4] != 9:
#                 raise Exception("Wrong number of hits!!!")
#         # Stop cache
#         stop_cache(smm, cache_queue, False, cache_process)
#     else:
#         print(NOT_PYTHON_3_8)
