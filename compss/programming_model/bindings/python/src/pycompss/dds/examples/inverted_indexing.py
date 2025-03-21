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
PyCOMPSs DDS - Examples - inverted indexing.

This file contains the DDS inverted indexing example.
"""

import os
import random
import shutil
import time
from collections import defaultdict

from pycompss.dds import DDS


def create_dataset() -> (str, str):
    """Create a dummy dataset.

    :return: Path to a folder containing a set of dummy files and the expected.
    """
    random.seed(1)
    vocabulary = [
        "PyCOMPSs",
        "Hello",
        "World",
        "Lorem",
        "Ipsum",
        "Barcelona",
        "Supercomputing",
        "Center",
    ]
    current_directory = os.getcwd()
    dataset_path = os.path.join(current_directory, "inverted_indexing_dataset")
    if not os.path.exists(dataset_path):
        os.makedirs(dataset_path)
    files = []
    for i in range(4):
        files.append(os.path.join(dataset_path, f"file_{i}.txt"))
    pairs = defaultdict(list)

    for word in vocabulary:
        _files = random.sample(files, 2)
        for _file in _files:
            with open(_file, "a") as tmp_f:
                tmp_f.write(word + " ")
            pairs[word].append(_file)

    return dataset_path, pairs


def clean_dataset(dataset_path):
    """Remove the given dataset.

    :param dataset_path: Folder to be removed.
    :return: None.
    """
    shutil.rmtree(dataset_path)


def _invert_files(pair):
    """Invert files.

    :param pair: Pair.
    :results: List of items.
    """
    res = {}
    for word in pair[1].split():
        res[word] = [pair[0]]
    return list(res.items())


def inverted_indexing():
    """Inverted indexing.

    :results: Inverted indexing result.
    """
    print("--- INVERTED INDEXING ---")

    # By default, create a dummy dataset and perform wordcount over it.
    # It could be changed to:
    #   path_file = sys.argv[1]
    # if you desire to perform the word count over a given dataset
    # (remember to comment the check_results call in this case).
    dataset_path, pairs = create_dataset()
    start_time = time.time()

    results = (
        DDS()
        .load_files_from_dir(dataset_path)
        .flat_map(_invert_files)
        .reduce_by_key(lambda a, b: a + b)
        .collect()
    )

    print(f"- Results: {results[-1:]}")
    print(f"- Elapsed Time: {time.time() - start_time} (s)")
    print("-------------------------")

    clean_dataset(dataset_path)

    check = []
    for word, files in results:
        check.append(set(pairs[word]).issubset(set(files)))
    return all(test for test in check)
