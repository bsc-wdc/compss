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
Utils Module for reproducibility_methods module

"""
import os
import subprocess

from rocrate.rocrate import ROCrate


def get_file_names(folder_path: str) -> dict:
    """
    Get the file names in the folder path
    """
    file_names = {}
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_names[file] = os.path.join(root, file)
    return file_names


def get_Create_Action(entity: ROCrate):
    """
    Get the Create Action entity from the ROCrate
    """
    for entity in entity.get_entities():
        if entity.type == "CreateAction":
            return entity
    return None


def get_results_dict(entity: ROCrate):
    """
    Get the results dictionary from the Create Action entity
    """
    createAction = get_Create_Action(entity)
    results = {}
    if (
        "result" in createAction
    ):  # It is not necessary to have inputs/objects in Create Action
        temp = createAction["result"]
    else:
        return None

    for result in temp:
        results[result["name"]] = result.id
    return results


def check_slurm_cluster() -> tuple[bool, str]:
    """
    To check if the program is running on a SLURM cluster.

    Returns:
        tuple[bool, str]: tuple of a boolean indicating if the program
            is running on a SLURM cluster and a message.
    """
    try:
        result = subprocess.run(["squeue"], capture_output=True, text=True)
        if result.returncode == 0:
            return True, result.stdout
        else:
            return False, result.stdout
    except Exception as e:
        return False, str(e)
