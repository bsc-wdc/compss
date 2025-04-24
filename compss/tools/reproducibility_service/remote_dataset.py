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
"""_
Remote Dataset Module

This module is used to download the remote datasets mentioned in the metadata file
and verify their size.
"""
import os
import sys

from rocrate.rocrate import ROCrate
from utils import download_file, get_Create_Action, print_colored, TextColor, get_by_id


def remote_dataset(crate: ROCrate, crate_directory: str) -> bool:
    """
    Download the remote datasets mentioned in the metadata file and verify their size.
    Args:
        crate (ROCrate): ROCrate object.
        crate_directory (str): the path to the root directory of the RO-Crate.

    Returns:
        bool: True if the remote datasets exist, False otherwise.
    """

    create_action = get_Create_Action(crate)
    remote_datasets = {}
    if "object" in create_action:
        temp = create_action["object"]
    else:
        return (False, {})

    for input in temp:  # get the remote_datasets mapped with their names
        if (input.id).startswith("http"):
            remote_datasets[input["name"]] = input.id

    for (
        key,
        val,
    ) in (
        remote_datasets.items()
    ):  # download the remote datasets with the specified names
        try:
            print_colored(
                f"Please wait while remote file {key} is being downloaded from {val} ...",
                TextColor.YELLOW,
            )
            download_file(val, os.path.join(crate_directory, "remote_dataset"), key)

            if "contentSize" in get_by_id(crate, val):
                if get_by_id(crate, val)["contentSize"] == os.path.getsize(
                    os.path.join(crate_directory, "remote_dataset", key)
                ):
                    print_colored(
                        f"Remote file {key} has been successfully downloaded with size verified.",
                        TextColor.GREEN,
                    )
            else:
                print_colored(
                    f"Remote file {key} has been successfully downloaded.Could not verify size as it is not mentioned in the metadata",
                    TextColor.GREEN,
                )

        except ValueError:
            print(f"Remote dataset {key} could not be downloaded.")
            sys.exit(1)

    if len(remote_datasets) == 0:
        return (False, {})

    return (True, remote_datasets)
