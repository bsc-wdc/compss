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
PyCOMPSs DDS - Examples - word count.

This file contains the DDS word count example.
"""

import os
import shutil
import time

from pycompss.dds import DDS


def create_dataset() -> str:
    """Create a dummy dataset.

    :return: Path to a folder containing a set of dummy files.
    """
    current_directory = os.getcwd()
    dataset_path = os.path.join(current_directory, "wordcount_dataset")
    if not os.path.exists(dataset_path):
        os.makedirs(dataset_path)
    for i in range(4):
        file_path = os.path.join(dataset_path, f"file_{i}.txt")
        with open(file_path, "w", encoding="utf-8") as file_path_fd:
            for j in range(10):
                file_path_fd.write(
                    f"Dummy dataset content number {i} in line {j}\n"
                )
    return dataset_path


def check_results(results) -> bool:
    """Check if the given results match the expected result.

    CAUTION: Only works for the dummy dataset.

    :param results: Dictionary containing the words and their appearance.
    :return: If the result is the expected or not.
    """
    expected = {
        "Dummy": 40,
        "dataset": 40,
        "content": 40,
        "number": 40,
        "0": 14,
        "in": 40,
        "line": 40,
        "1": 14,
        "2": 14,
        "3": 14,
        "4": 4,
        "5": 4,
        "6": 4,
        "7": 4,
        "8": 4,
        "9": 4,
    }
    return results == expected


def clean_dataset(dataset_path):
    """Remove the given dataset.

    :param dataset_path: Folder to be removed.
    :return: None.
    """
    shutil.rmtree(dataset_path)


def word_count():
    """Word count.

    Perform word counting from a dummy dataset.

    :results: If the result matches the expected.
    """
    print("--- WORD COUNT ---")

    # By default, create a dummy dataset and perform wordcount over it.
    # It could be changed to:
    #   path_file = sys.argv[1]
    # if you desire to perform the word count over a given dataset
    # (remember to comment the check_results call in this case).
    dataset_path = create_dataset()
    start_time = time.time()

    results = (
        DDS()
        .load_files_from_dir(dataset_path)
        .flat_map(lambda x: x[1].split())
        .map(lambda x: "".join(e for e in x if e.isalnum()))
        .count_by_value(as_dict=True)
    )

    print(f"- Results: {results}")
    print(f"- Elapsed Time: {time.time() - start_time} (s)")
    print("------------------")

    clean_dataset(dataset_path)

    return check_results(results)
