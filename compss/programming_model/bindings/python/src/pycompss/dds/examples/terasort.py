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
PyCOMPSs DDS - Examples - terasort.

This file contains the DDS terasort example.
"""

import os
import shutil
import random
import time

from pycompss.dds import DDS


def create_dataset() -> (str, str):
    """Create a dummy dataset.

    :return: Path to a folder containing a set of dummy files and result path.
    """
    random.seed(1)
    current_directory = os.getcwd()
    dataset_path = os.path.join(current_directory, "terasort_dataset")
    dataset_dest_path = os.path.join(
        current_directory, "terasort_dataset_result"
    )
    if not os.path.exists(dataset_path):
        os.makedirs(dataset_path)
    for i in range(4):
        file_path = os.path.join(dataset_path, f"file_{i}.txt")
        with open(file_path, "w", encoding="utf-8") as file_path_fd:
            for j in range(10):
                file_path_fd.write(f"{random.randint(0, 1000)},{i*j}\n")
    # if not os.path.exists(dataset_dest_path):
    #     os.makedirs(dataset_dest_path)
    return dataset_path, dataset_dest_path


def check_results(results) -> bool:
    """Check if the given results match the expected result.

    CAUTION: Only works for the dummy dataset.

    :param results: Dictionary containing the words and their appearance.
    :return: If the result is the expected or not.
    """
    expected = [
        ("104", "15"),
        ("120", "0"),
        ("137", "0"),
        ("2", "12"),
        ("214", "5"),
        ("22", "27"),
        ("234", "6"),
        ("261", "0"),
        ("272", "18"),
        ("29", "8"),
        ("31", "24"),
        ("325", "21"),
        ("388", "3"),
        ("399", "2"),
        ("443", "4"),
        ("456", "16"),
        ("460", "0"),
        ("483", "1"),
        ("499", "7"),
        ("507", "0"),
        ("582", "0"),
        ("605", "9"),
        ("622", "6"),
        ("64", "0"),
        ("667", "2"),
        ("712", "14"),
        ("738", "0"),
        ("779", "0"),
        ("780", "8"),
        ("782", "0"),
        ("785", "10"),
        ("807", "4"),
        ("821", "0"),
        ("821", "3"),
        ("855", "0"),
        ("867", "0"),
        ("914", "9"),
        ("923", "18"),
        ("96", "6"),
        ("967", "12"),
    ]

    return results == expected


def clean_dataset(dataset_path):
    """Remove the given dataset.

    :param dataset_path: Folder to be removed.
    :return: None.
    """
    shutil.rmtree(dataset_path)


def files_to_pairs(element):
    """Pair files.

    :param element: String of elements.
    :return: List of pairs.
    """
    tuples = []
    lines = element[1].split("\n")
    for _l in lines:
        if not _l:
            continue
        k_v = _l.split(",")
        tuples.append(tuple(k_v))

    return tuples


def terasort():
    """Apply terasort over a dummy dataset.

    :return: Sorting result.
    """
    print("--- TERASORT ---")

    # By default, create a dummy dataset and perform wordcount over it.
    # It could be changed to:
    #   dir_path = sys.argv[1]
    #   dest_path = sys.argv[2]
    # if you desire to perform the word count over a given dataset
    # (remember to comment the check_results call in this case).
    dir_path, _ = create_dataset()

    start_time = time.time()

    results = (
        DDS()
        .load_files_from_dir(dir_path)
        .flat_map(files_to_pairs)
        .sort_by_key()
        # .save_as_text_file(dest_path)
        .collect()
    )

    print(f"- Results: {results}")
    print(f"- Elapsed Time: {time.time() - start_time} (s)")
    print("----------------")

    clean_dataset(dir_path)

    return check_results(results)
