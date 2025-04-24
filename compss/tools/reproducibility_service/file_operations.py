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
RS File Operations Module

This module provides functions for handling file operations such as removing temporary files,
moving newly created files to a 'Result' folder, and copying files from specific directories
to the current working directory.

"""

import os
import shutil
import datetime


def move_results_created(initial_files, execution_path: str):
    """
    Removes temporary files and moves newly created files to a 'Result' folder.

    Args:
    initial_files (set): Set of initial files before operation.
    temp (list): List of temporary files to be removed.

    """
    result_folder_path = os.path.join(execution_path, "Result")
    # Get the current list of files in the CWD and remove the clean-up files, that were copied for the execution
    cwd = os.getcwd()
    current_files = set(os.listdir(cwd))
    # Determine the new files by comparing current files with initial files
    new_files = current_files - initial_files

    if new_files:
        if not os.path.exists(result_folder_path):
            os.makedirs(result_folder_path)
        # Move the new files to the Result folder
        for new_file in new_files:
            if new_file.startswith(
                "reproducibility_service_"
            ):  # cannot move the execution directory into itself
                continue
            if new_file == "__pycache__":  # Do not consider the cache files
                continue
            src_path = os.path.join(cwd, new_file)
            dest_path = os.path.join(result_folder_path, new_file)
            shutil.move(src_path, dest_path)


# def remote_dataset_mover(directory: str) -> set[str]:
#     """
#     Copies all files from the 'remote_dataset' folder in the current working directory
#     to the current working directory.

#     Args:
#     directory (str): Path to the 'remote_dataset' directory.

#     Returns:
#     set: A set of names of the files and directories copied to the current working directory.
#     """
#     remote_dataset_folder = os.path.join(directory, "remote_dataset")
#     return  copy_all_to_cwd(remote_dataset_folder)

# def dataset_mover_and_application_mover(crate_directory) -> set[str]:
#     """
#     Copies all files from 'application_sources' and 'dataset' folders in the
#     crate directory to the current working directory. Can tackle some cases of
#     hard-coded paths in the application.

#     Args:
#     crate_directory (str): Path to the RO-Crate directory.

#     Returns:
#     set: A set of names of the files copied to the current working directory.
#     """
#     application_folder = os.path.join(crate_directory, "application_sources")
#     dataset_folder = os.path.join(crate_directory, "dataset")
#     input_files_copied = set()
#     input1 = copy_all_files_to_cwd(application_folder)
#     input2 = copy_all_files_to_cwd(dataset_folder)
#     input_files_copied = input1.union(input2)

#     return input_files_copied


def copy_all_files_to_cwd(src_path) -> set[str]:
    """
    Copies all files from the specified source path to the current working directory.

    Parameters:
    src_path (str): The path to the source directory containing files to be copied.

    Returns:
    Set[str]: A set of names of the files copied to the current working directory.
    """
    if not os.path.isdir(src_path):
        print(f"The provided path '{src_path}' is not a valid directory.")
        return set()

    cwd = os.getcwd()
    copied_files = set()

    for item in os.listdir(src_path):
        src_item_path = os.path.join(src_path, item)
        if os.path.isfile(src_item_path):
            dst_item_path = os.path.join(cwd, item)
            shutil.copy2(src_item_path, dst_item_path)
            copied_files.add(os.path.relpath(dst_item_path, cwd))

    return copied_files


def copy_all_to_cwd(src_path) -> set[str]:
    """
    Copies all files and folders from the specified source path to the current working directory.

    Parameters:
    src_path (str): The path to the source directory containing files and folders to be copied.

    Returns:
    Set[str]: A set of names of the files and directories copied to the current working directory.
    """
    if not os.path.isdir(src_path):
        print(f"The provided path '{src_path}' is not a valid directory.")
        return set()

    cwd = os.getcwd()
    copied_items = set()

    def copy_item(src_item_path, dst_item_path):
        if os.path.isdir(src_item_path):
            if not os.path.exists(dst_item_path):
                os.makedirs(dst_item_path)
            items = os.listdir(src_item_path)
            for item in items:
                copy_item(
                    os.path.join(src_item_path, item), os.path.join(dst_item_path, item)
                )
        else:
            shutil.copy2(src_item_path, dst_item_path)
        copied_items.add(os.path.relpath(dst_item_path, cwd))

    for item in os.listdir(src_path):
        src_item_path = os.path.join(src_path, item)
        dst_item_path = os.path.join(cwd, item)
        copy_item(src_item_path, dst_item_path)

    return copied_items


# def cleanup(temp: set):
#     cwd = os.getcwd()
#     for filename in temp:
#         file_path = os.path.join(cwd, filename)
#         if os.path.isfile(file_path):
#             os.unlink(file_path)
#         elif os.path.isdir(file_path):
#             shutil.rmtree(file_path)


def create_new_execution_directory(SERVICE_PATH: str):
    # Create a unique sub-directory name based on the current timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    new_execution_dir = os.path.join(
        os.getcwd(), f"reproducibility_service_{timestamp}"
    )
    # Create the new sub-directory
    os.makedirs(new_execution_dir)
    # required directories for the service
    os.makedirs(os.path.join(new_execution_dir, "log"))
    os.makedirs(os.path.join(new_execution_dir, "Workflow"))
    return new_execution_dir
