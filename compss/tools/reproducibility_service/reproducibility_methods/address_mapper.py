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
Address Mapper Module

This module provides functionality to map and convert addresses of datasets
within a given directory structure. It includes the following functions:

# Problems:
# IF THE DIRECTORY IS TAKEN IN THE FORM OF DATA/INPUT INSTED OF DATA/INPUT/
# THEN IT WILL NOT BE ABLE TO FIND THE DIRECTORY
# DO NOT INCLUDE DOUBLE SLASHES IN THE PATHS LIKE INPUT//TEXT.TXT IT WILL NOT FIND IT
"""

import os


# TO-DO:
# change address converter backend such that if a file/directory matches with
# any result object then mak a new folder with same name if directory inside the Results/ and map it there
# else it is assumed to be a application source or a dataset file/dir
def address_converter_backend(path: str, addr: str, dataset_hashmap: dict) -> str:
    """
    Converts the given address to a mapped address inside the RO_Crate
    based on the dataset hashmap.

    Args:
        path (str): The base path of the dataset.
        addr (str): The address to be converted.
        dataset_hashmap (dict): A dictionary containing the mapping of dataset addresses.

    Returns:
        str: The mapped address corresponding to the given address.

    Raises:
        FileNotFoundError: If the mapped address for the given address is not found.
    """
    filename = None
    mapped_addr = None

    if addr.startswith("./"):
        addr = addr[1:]

    if not addr.startswith("/"):
        addr = "/" + addr

    # Check if the address is a file or a directory and split it accordingly
    if addr.endswith("/"):
        addr_list = addr.split("/")
    else:
        addr_list = addr.split("/")
        filename = addr_list.pop()

    for i in range(1, len(addr_list)):
        if addr_list[i] in dataset_hashmap:
            temp_addr = os.path.join(path, addr_list[i])
            for j in range(i + 1, len(addr_list)):
                temp_addr = os.path.join(temp_addr, addr_list[j])

            if os.path.exists(temp_addr):
                mapped_addr = temp_addr
                break

    # If the address is a file, append the filename and check if exists
    if filename:
        if not mapped_addr:
            mapped_addr = path
        mapped_addr = os.path.join(mapped_addr, filename)
        if not os.path.exists(mapped_addr):
            raise FileNotFoundError(f"Could not find the mapped address for: {addr}")

    # Could not find such directory or file
    if not mapped_addr:
        raise FileNotFoundError(f"Could not find the mapped address for: {addr}")

    return mapped_addr


def address_converter(
    path: str,
    addr: str,
    dataset_hashmap: dict,
    application_sources_hashmap: dict,
    remote_dataset_hashmap: dict,
    dataset_flags: tuple[bool, bool],
) -> str:
    """
    Attempts to convert the given address first using the dataset hashmap and
    then using the application sources hashmap. Raises a `FileNotFoundError` if
    the address  cannot be mapped in either case.

    Args:
        path (str): Path to the RO_Crate directory.
        addr (str): Address to be converted.
        dataset_hashmap (dict): Hashmap generated from addr_extractor.
        application_sources_hashmap (dict): Hashmap generated from addr_extractor.
        remote_dataset_hashmap (dict): Hashmap generated from addr_extractor.
        dataset_flags (tuple[bool, bool]): (remote_dataset_flag, new_dataset_flag)

    Raises:
        FileNotFoundError: Cannot find the address inside the RO_Crate.

    Returns:
        str: The mapped address.
    """
    errors = []

    if dataset_flags[1]:
        dataset_path = os.path.join(path, "new_dataset")
    else:
        dataset_path = os.path.join(path, "dataset")
    application_sources_path = os.path.join(path, "application_sources")

    # Define the paths to try for address conversion
    paths_to_try = [
        (dataset_path, dataset_hashmap, "Dataset Error"),
        (
            application_sources_path,
            application_sources_hashmap,
            "Application Sources Error",
        ),
    ]

    if dataset_flags[0]:
        paths_to_try.insert(
            0,
            (
                os.path.join(path, "remote_dataset"),
                remote_dataset_hashmap,
                "Remote Dataset Error",
            ),
        )

    for (
        path,
        hashmap,
        error_context,
    ) in paths_to_try:  # try all the paths one by one and return where
        try:  # file is found,if not found append the error to the list
            return address_converter_backend(path, addr, hashmap)
        except FileNotFoundError as e:
            errors.append((error_context, e))

    handle_address_conversion_failure(addr, errors)


def addr_extractor(path: str) -> dict:
    """
    Extracts the addresses of datasets in the given path. For this particular case,
    it is used to extract the mapping of filenames in the crate/dataset and
    crate/application_sources directories.

    Args:
        path (str): The path to the directory containing the datasets.

    Returns:
        dict: A dictionary mapping dataset filenames to a value of 1.
    """
    if not os.path.exists(path):
        os.makedirs(path)
    hash_map = {}
    for filename in os.listdir(path):
        hash_map[filename] = 1

    return hash_map


def handle_address_conversion_failure(addr: str, errors: list):
    """
    Used for raising a `FileNotFoundError` when the address conversion fails.
    """
    error_messages = "\n".join(f"{context}: {err}" for context, err in errors)
    raise FileNotFoundError(
        f"Could not find the mapped address for: {addr}\n{error_messages}"
    ) from errors[-1][1]
