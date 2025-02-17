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

import os

from pycompss.dds.dds import DDS

GENERIC_ERROR = "ERROR: Unexpected result from DDS."

current_path = os.path.dirname(os.path.abspath(__file__))
dataset_dir = os.path.join(current_path, "dataset")
wc_dir = os.path.join(dataset_dir, "wordcount")
fayl = os.path.join(wc_dir, "1.txt")


def test_data_loaders():
    dds = DDS()
    assert not dds.partitions, GENERIC_ERROR

    data = list(range(10))
    assert len(dds.load(data).collect()) == 10, GENERIC_ERROR

    assert DDS().load_file(fayl).collect(), GENERIC_ERROR
    assert DDS().load_file(fayl, worker_read=True).collect(), GENERIC_ERROR
    assert DDS().load_text_file(fayl).collect(), GENERIC_ERROR
    assert DDS().load_files_from_dir(wc_dir).collect(), GENERIC_ERROR

    pickles_dir = os.path.join(dataset_dir, "pickles")
    DDS().load_text_file(fayl).save_as_pickle(pickles_dir)
    assert DDS().load_pickle_files(pickles_dir).collect()

    tmp_dir = os.path.join(dataset_dir, "tmp")
    DDS().load_text_file(fayl).save_as_text_file(tmp_dir)
    assert DDS().load_files_from_dir(tmp_dir).collect()


def test_methods():
    data = list(range(10))

    dds = DDS().load(data).map(lambda x: x * 2).collect()
    assert 18 in dds

    unified = DDS().load(data).union(DDS().load(data)).collect()
    assert len(unified) == 20

    dds = DDS().load(data).flat_map(lambda x: [x, x * 2]).collect()
    assert 18 in dds

    dds = DDS().load(data).filter(lambda x: x > 5).collect()
    assert len(dds) == 4

    dds = DDS().load(data).reduce(lambda x, y: x + y)
    assert dds == 45

    dds = DDS().load(data).flat_map(lambda x: list(range(x)))
    assert dds.count_by_value().get(0, 0) == 9

    dds = (
        DDS()
        .load([("a", 1), ("b", 3)])
        .join(DDS().load([("a", 2), ("b", 4)]))
        .collect()
    )
    assert ("a", (1, 2)) in dds

    dds = DDS().load(data).take(4)
    assert len(dds) == 4


def test_k_v_operations():
    data = list(range(10))
    dds = (
        DDS()
        .load(data)
        .map(lambda x: (x, x * 2))
        .map_values(lambda x: x / 2)
        .collect()
    )
    for i in dds:
        assert i[0] == i[1]

    dds = (
        DDS()
        .load(data, num_of_parts=2)
        .map(lambda x: (x, x))
        .partition_by(lambda x: x % 2)
        .collect(keep_partitions=True)
    )
    for i in range(5):
        assert abs(dds[0][i][0] - dds[1][i][0]) == 1

    dds = (
        DDS()
        .load([("a", [1, 2]), ("b", [1])])
        .flatten_by_key(lambda x: x)
        .collect()
    )
    assert len(dds) == 3

    dds = (
        DDS()
        .load([("z", 1), ("b", 3), ("a", 1), ("c", 3)])
        .sort_by_key()
        .collect()
    )
    assert dds[0][0] == "a"
