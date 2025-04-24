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
RS File Verification Module

This module verifies files referenced in an RO-Crate against their metadata,
ensuring their existence and optionally their size and modification date.

"""

import os

from rocrate.rocrate import ROCrate
from utils import get_by_id, print_colored, TextColor, generate_file_status_table


def files_verifier(
    crate_path: str, instrument: str, objects: dict, remote_dataset_dict: dict
):
    """
    Verify files within an RO-Crate against their metadata.

    Args:
        crate_path (str): Path to the root directory of the RO-Crate.
        instrument (str): Identifier of the instrument file within the RO-Crate.
        objects (list[str]): List of identifiers for objects/inputs within the RO-Crate.

    Raises:
        FileNotFoundError: If any referenced file in the RO-Crate does not exist in the directory.
        ValueError:If the content size of any file does not match the recorded size in the RO-Crate.

    Notes:
        This function verifies the existence and size of files referenced in the RO-Crate
        against the actual files in the specified directory. Optionally, it can also verify
        modification dates, although this feature is currently commented out.
    """
    print_colored("Verifying the files in the crate", TextColor.YELLOW)
    file_verifier = []  # tuple of (file_name, file_path, content_size, actual_size)
    verified = True
    size_verifier = True
    # date_verifier = True
    temp_size = []
    temp_path = []
    # temp_date = []
    crate = ROCrate(crate_path)
    instrument_path = os.path.join(crate_path, instrument)
    instrument_tuple = (instrument, instrument_path, 1, 1)
    # Verify the instrument file
    if not os.path.exists(instrument_path):
        verified = False
        temp_path.append(instrument_path)
        instrument_tuple = (instrument_tuple[0], instrument_tuple[1], 0, 0)

    if (
        not os.path.getsize(instrument_path)
        == get_by_id(crate, instrument)["contentSize"]
    ):
        size_verifier = False
        temp_size.append(instrument_path)
        instrument_tuple = (
            instrument_tuple[0],
            instrument_tuple[1],
            instrument_tuple[2],
            1,
        )

    file_verifier.append(instrument_tuple)
    # Verify the objects/inputs
    crate = ROCrate(crate_path)

    if not objects:
        print_colored(
            "No objects found in the crate, so nothing to verify", TextColor.GREEN
        )
        return

    for name, input in objects.items():
        if (
            not remote_dataset_dict and name[0] in remote_dataset_dict
        ):  # Do not verifiy the local objects if remote dataset exists
            continue
        # Skip the remote objects
        if input.startswith("http"):
            continue
        file_path = os.path.join(crate_path, input)
        file_tuple = (name[0], file_path, 1, 2)
        if not os.path.exists(file_path):
            verified = False
            temp_path.append(file_path)
            file_tuple = (name[0], file_path, 0, 0)
            file_verifier.append(file_tuple)
            continue
        # else:
        #     print(file_path+"\n"+"FILE EXISTS")
        file_object = get_by_id(crate, input)
        if "contentSize" in file_object:
            content_size = file_object["contentSize"]
            # Verify the above content size with the actual file size
            # Get the actual file size
            actual_size = os.path.getsize(file_path)

            # Verify the content size with the actual file size
            if actual_size != content_size:
                # print(name[0]+"\n"+file_path+"\n"+"SIZE MISMATCH")
                size_verifier = False
                temp_size.append(os.path.join(crate_path, input))
                file_tuple = (file_tuple[0], file_tuple[1], file_tuple[2], 0)
            else:
                file_tuple = (file_tuple[0], file_tuple[1], file_tuple[2], 1)
                # print(os.path.join(crate_path, input)+"\n"+"SIZE VERIFIED")

        file_verifier.append(file_tuple)

    print_colored(
        "STATUS TABLE (the crate includes the DATASETS needed by the workflow to run, data persistence was TRUE):",
        TextColor.YELLOW,
    )

    generate_file_status_table(file_verifier, "Included")

    if not size_verifier:
        if verified:
            raise ValueError(f"Content size mismatch in files: {temp_size}")
        else:
            raise ValueError(
                f"Content size mismatch in files: {temp_size}\nFiles missing: {temp_path}"
            )
    if not verified:
        raise FileNotFoundError(f"Files missing in directory: {temp_path}")

    print_colored(
        "All files in the crate have been verified successfully", TextColor.GREEN
    )
