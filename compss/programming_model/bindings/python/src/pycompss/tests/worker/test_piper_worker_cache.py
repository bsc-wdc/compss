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

import sys
import time
import logging
import numpy as np

from pycompss.worker.piper.cache.setup import is_cache_enabled
from pycompss.worker.piper.cache.setup import start_cache
from pycompss.worker.piper.cache.setup import stop_cache
from pycompss.worker.piper.cache.tracker import load_shared_memory_manager
from pycompss.worker.piper.cache.tracker import insert_object_into_cache_wrapper
from pycompss.worker.piper.cache.tracker import retrieve_object_from_cache
from pycompss.worker.piper.cache.tracker import remove_object_from_cache
from pycompss.worker.piper.cache.tracker import replace_object_into_cache


def test_is_cache_enabled():
    if sys.version_info >= (3, 8):
        case1 = is_cache_enabled("true")
        assert case1, "Unexpected return. Expected: <bool> True"
        case2 = is_cache_enabled("True")
        assert not case2, "Unexpected return. Expected: <bool> False"
        case3 = is_cache_enabled("true:1000")
        assert case3, "Unexpected return. Expected: <bool> True"
        case4 = is_cache_enabled("True:1000")
        assert not case4, "Unexpected return. Expected: <bool> False"
    else:
        print("WARNING: Could not perform cache test since python version is lower than 3.8")  # noqa: E501


def test_piper_worker_cache():
    if sys.version_info >= (3, 8):
        smm, cache_process, cache_queue, cache_ids = start_cache(logging,
                                                                 "Default")
        load_shared_memory_manager()
        # Supported types:
        np_obj = np.random.rand(4)
        np_obj_name = "np_obj"
        list_obj = [1, 2, 3, 4, 5, "hi"]
        list_obj_name = "list_obj_name"
        tuple_obj = ("1", 2, 3, "4", "hi")
        tuple_obj_name = "tuple_obj_name"
        # Check insertions
        insert_object_into_cache_wrapper(logging, cache_queue, np_obj, np_obj_name)  # noqa: E501
        insert_object_into_cache_wrapper(logging, cache_queue, list_obj, list_obj_name)  # noqa: E501
        insert_object_into_cache_wrapper(logging, cache_queue, tuple_obj, tuple_obj_name)  # noqa: E501
        # Check retrieves
        np_obj_new, np_obj_shm = retrieve_object_from_cache(logging, cache_ids, np_obj_name)  # noqa: E501
        list_obj_new, list_obj_shm = retrieve_object_from_cache(logging, cache_ids, list_obj_name)  # noqa: E501
        tuple_obj_new, tuple_obj_shm = retrieve_object_from_cache(logging, cache_ids, tuple_obj_name)  # noqa: E501
        assert (
            set(np_obj_new).intersection(np_obj)
        ), "ERROR: Numpy object retrieved from cache differs from inserted"
        assert (
            set(list_obj_new).intersection(list_obj)
        ), "ERROR: List retrieved from cache differs from inserted"
        assert (
            set(tuple_obj_new).intersection(tuple_obj)
        ), "ERROR: Tuple retrieved from cache differs from inserted"
        # Check replace
        new_list_obj = ["hello", "world", 6]
        replace_object_into_cache(logging, cache_queue, new_list_obj, list_obj_name)   # noqa: E501
        time.sleep(0.5)
        list_obj_new2, list_obj_shm2 = retrieve_object_from_cache(logging, cache_ids, list_obj_name)  # noqa: E501
        assert (
            set(list_obj_new2).intersection(new_list_obj)
        ), "ERROR: List retrieved from cache differs from inserted"
        # Remove object
        remove_object_from_cache(logging, cache_queue, list_obj_name)
        is_ok = False
        try:
            _, _ = retrieve_object_from_cache(logging, cache_ids, list_obj_name)  # noqa: E501
        except Exception:  # NOSONAR
            is_ok = True
        assert (
            is_ok
        ), "ERROR: List has not been removed."
        stop_cache(smm, cache_queue, cache_process)
    else:
        print("WARNING: Could not perform cache test since python version is lower than 3.8")  # noqa: E501
