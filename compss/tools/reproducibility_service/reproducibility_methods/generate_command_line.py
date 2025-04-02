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
"""
Command Line Generator Module

This module provides functions to generate and modify command line arguments,
particularly handling paths for datasets and application sources.

Problems:
Anything with r'/' ot r'\' is considered a path, but it could be a flag or a value
Plus if the command is " runcompss main.py " it will not convert the path for main.py
so it must be declared as " runcompss /main.py "
"""

import os
import shlex
import re
import sys

from rocrate.rocrate import ROCrate

from utils import print_colored, TextColor
from .address_mapper import address_converter, addr_extractor
from .utilsr import get_file_names, get_results_dict, check_slurm_cluster


def generate_command_line(self, sub_directory_path: str) -> list[str]:
    """
    Generates a modified command line based on the contents of a compss_submission_command_line.txt file, if it exists.
    In alternative, the command line is parsed from the ro-crate-metadata.json, in the description of CreateAction.
    The final command line includes mappings of dataset and application sources found in the crate directory.

    Args:
    crate_directory (str): Path to the crate directory containing dataset and application sources.

    Returns:
    list[str]: Modified command line arguments.
    """
    # print('\nParsing metadata from: ', self.crate_directory)
    path = self.crate_directory
    dataset_flags = (self.remote_dataset_flag, self.new_dataset_flag)

    remote_dataset_hashmap = {}
    if self.remote_dataset_flag:
        remote_dataset_hashmap = addr_extractor(os.path.join(path, "remote_dataset"))

    if not self.new_dataset_flag:
        dataset_hashmap = addr_extractor(os.path.join(path, "dataset"))
    else:
        dataset_hashmap = addr_extractor(os.path.join(path, "new_dataset"))

    application_sources_hashmap = addr_extractor(
        os.path.join(path, "application_sources")
    )

    compss_submission_command = ""
    compss_submission_command_path = os.path.join(
        path, "compss_submission_command_line.txt"
    )
    if os.path.isfile(compss_submission_command_path):
        with open(compss_submission_command_path, "r", encoding="utf-8") as file:
            compss_submission_command = next(file).strip()
    else:
        crate = ROCrate(path)
        for e in crate.get_entities():
            if "#COMPSs_Workflow_Run_Crate_" in e.id:
                compss_submission_command = e["description"]
                if not compss_submission_command.startswith(
                    ("runcompss", "enqueue_compss")
                ):
                    print_colored(
                        f"The command found is this: \n{compss_submission_command}",
                        TextColor.YELLOW,
                    )
                    print_colored(
                        "Error: the command found is not valid.", TextColor.RED
                    )
                    sys.exit(0)
                break

    # compss_submission_command = clean_command(link_or_path, new_command.split(" "))

    new_command = ""
    crate = ROCrate(path)
    for e in crate.get_entities():
        if "#COMPSs_Workflow_Run_Crate_" in e.id:
            new_command = e["description"]
            break

    new_command = ""
    crate = ROCrate(path)
    for e in crate.get_entities():
        if "#COMPSs_Workflow_Run_Crate_" in e.id:
            new_command = e["description"]
            break

    # compss_submission_command_path = os.path.join(path, "compss_submission_command_line.txt")

    # with open(compss_submission_command_path, 'r', encoding='utf-8') as file:
    #     compss_submission_command = next(file).strip()

    new_command = command_line_generator(
        compss_submission_command,
        path,
        dataset_hashmap,
        application_sources_hashmap,
        remote_dataset_hashmap,
        dataset_flags,
        self.remote_dataset_flag,
        sub_directory_path,
    )

    return new_command


def commonsuffix(path1: str, path2: str):
    paths = [path1, path2]
    # Reverse the strings in the list to use os.path.commonprefix on the reversed strings
    reversed_paths = [
        os.path.dirname(path)[::-1] for path in paths
    ]  # This removes the ending '/' from the directories
    # Find the common prefix of the reversed strings
    reversed_common_prefix = os.path.commonprefix(reversed_paths)
    # Reverse the common prefix back to get the common suffix
    path1 = path1.split("/")
    is_possible = reversed_common_prefix[::-1] in path1  # check if the commaon dir
    if is_possible:
        return os.path.basename(reversed_common_prefix[::-1])
    else:
        return None


def is_result(filepath: str, results_dict: dict, sub_directory_path: str) -> str:

    result_path = os.path.join(sub_directory_path, "Result")
    if not os.path.exists(result_path):
        os.mkdir(result_path)
    if os.path.basename(filepath):  # if the path is a file
        if os.path.basename(filepath) in results_dict:  # if it is a result
            return os.path.join(result_path, os.path.basename(filepath))
    else:  # if path is a directory
        for _, id in results_dict.items():
            is_possible_result = commonsuffix(filepath, id)
            if is_possible_result:
                # print("Result is possible for ", filepath, id)
                # print(f"Commonsuffix: {is_possible_result}")
                is_possible_result += "/"
                if not os.path.exists(os.path.join(result_path, is_possible_result)):
                    os.mkdir(os.path.join(result_path, is_possible_result))
                return os.path.join(result_path, is_possible_result)

    return None  # if it is not a result


def command_line_generator(
    command: str,
    path: str,
    dataset_hashmap: dict,
    application_sources_hashmap: dict,
    remote_dataset_hashmap: dict,
    dataset_flags: tuple[bool, bool],
    remote_dataset_flag: bool,
    sub_directory_path: str,
) -> list[str]:
    """
    Generates a modified command line by replacing paths in the command with their
    mapped counterparts based on the dataset and application sources mappings.
    Acts as the main logic behind generate_command_line.

    Args:
        command (str): Original command line string.
        path (str): Path to the RO_Crate directory.
        dataset_hashmap (dict): Hashmap generated from addr_extractor.
        application_sources_hashmap (dict): Hashmap generated from addr_extractor.

    Returns:
        list[str]: Modified command line arguments with mapped paths.
    """
    command = shlex.split(command)
    flags = []
    paths = []
    values = []
    # print(1)
    # print(path)
    files_a = get_file_names(os.path.join(path, "application_sources"))
    files_d = get_file_names(os.path.join(path, "dataset"))
    files_r = get_file_names(os.path.join(path, "remote_dataset"))

    crate = ROCrate(path)
    results_dict = get_results_dict(crate)

    for i, cmd in enumerate(command):
        if cmd.startswith("--") or cmd.startswith("-"):
            if not (cmd.startswith("--provenance") or cmd.startswith("-p")):
                flags.append((cmd, i))
        elif re.compile(r"[/\\]").search(cmd):  # Pattern for detecting paths
            paths.append((cmd, i))
        else:
            if results_dict and cmd in results_dict:
                result_path = os.path.join(sub_directory_path, "Result")
                if not os.path.exists(result_path):
                    os.mkdir(result_path)
                values.append((os.path.join(result_path, cmd), i))
            elif remote_dataset_flag and cmd in files_r:
                values.append((files_r[cmd], i))
            elif cmd in files_a:
                values.append((files_a[cmd], i))
            elif cmd in files_d:
                values.append((files_d[cmd], i))
            else:
                values.append((cmd, i))

    new_paths = []

    for filepath in paths:
        pathr = is_result(filepath[0], results_dict, sub_directory_path)

        if (
            pathr
        ):  # if it is a result then it is specially mapped inside Result/ in the sub-dir
            new_paths.append((pathr, filepath[1]))

        else:  # it is treated as a normal path inside application_sources or dataset
            new_filepath = address_converter(
                path,
                filepath[0],
                dataset_hashmap,
                application_sources_hashmap,
                remote_dataset_hashmap,
                dataset_flags,
            )
            new_paths.append((new_filepath, filepath[1]))

    paths = new_paths

    p1 = 0
    p2 = 0

    new_command = []
    # for this to work paths and value must be sorted by index
    values = sorted(
        values, key=lambda x: x[1]
    )  # needed to add this because of appending result paths at the end
    while p1 < len(paths) and p2 < len(values):
        if paths[p1][1] < values[p2][1]:
            new_command.append(paths[p1][0])
            p1 += 1
        else:
            new_command.append(values[p2][0])
            p2 += 1

    while p1 < len(paths):
        new_command.append(paths[p1][0])
        p1 += 1

    while p2 < len(values):
        new_command.append(values[p2][0])
        p2 += 1

    if check_slurm_cluster()[0]:
        new_command[0] = "enqueue_compss"
    else:
        new_command[0] = "runcompss"

    return new_command
