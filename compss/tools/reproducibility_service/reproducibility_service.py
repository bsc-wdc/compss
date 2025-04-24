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
Reproducibility Service Main Module

This module serves as the entry point for the Reproducibility Service, a tool designed to automate
the process of reproducing computational experiments and workflows. It integrates various components
and modules to ensure that experiments can be consistently and accurately reproduced, either using
existing datasets or new ones.

Authors: Archit Dabral and Raül Sirvent

"""

import os
import sys
import signal
from rocrate.rocrate import ROCrate

from reproducibility_methods import generate_command_line
from file_operations import move_results_created, create_new_execution_directory
from file_verifier import files_verifier
from utils import (
    check_compss_version,
    check_slurm_cluster,
    executor,
    get_compss_crate_version,
    get_data_persistence_status,
    get_instument,
    get_objects_dict,
    get_previous_flags,
    get_yes_or_no,
    print_colored,
    print_welcome_message,
    TextColor,
    get_create_action_name,
)
from new_dataset_backend import new_dataset_info_collector
from provenance_backend import (
    provenance_info_collector,
    update_yaml,
    provenance_checker,
)
from get_workflow import get_workflow, get_more_flags, get_change_values
from remote_dataset import remote_dataset
from data_persistance_false import data_persistence_false_verifier, run_dpf

SUB_DIRECTORY_PATH: str = None
SERVICE_PATH: str = None
COMPSS_VERSION: str = None
SLURM_CLUSTER: bool = None
DPF: bool = False
CRATE_PATH: str = None
DATA_PERSISTENCE: bool = False


def interrupt_handler(
    signum, frame
):  # signal handler for cleaning up in case of an interrupt
    """
    Signal handler for safely exiting in case of interrupt.
    """
    print_colored(
        f"Reproducibility Service has been interrupted with signal {signum}.",
        TextColor.RED,
    )
    print_colored("Exiting the program.", TextColor.RED)
    sys.exit(0)


signal.signal(signal.SIGINT, interrupt_handler)  # register the signal handler


class Unbuffered:
    """
    Unbuffered class for logging purposes.
    """

    def __init__(self, stream):
        self.stream = stream

    def write(self, data):
        self.stream.write(data)
        self.stream.flush()
        te.write(data)

    def flush(self):
        self.stream.flush()
        te.flush()


class ReproducibilityService:
    """
    Reproducibility Service class for executing the reproducibility service.

    __init__: Initialises the Reproducibility Service with the given flags.
        And verifies the files and fills other necessary information.

    run(): Run the reproducibility service by submitting the final command to the executor.
    """

    def __init__(self, provenance_flag: bool, new_dataset_flag: bool) -> bool:
        global CRATE_PATH
        self.crate_directory = CRATE_PATH
        self.provenance_flag = provenance_flag
        self.new_dataset_flag = new_dataset_flag
        self.root_folder = SERVICE_PATH
        self.remote_dataset_flag = False

        crate_compss_version: str = get_compss_crate_version(self.crate_directory)
        print_colored(
            f"COMPSs version used in the original run: {crate_compss_version}",
            TextColor.BLUE,
        )

        # not using currently to run 3.3.1,3.3 examples on a 3.3 or 3.3.1 compss machine
        if COMPSS_VERSION != crate_compss_version:
            print_colored(
                f"WARNING: The crate was created with COMPSs version: {get_compss_crate_version(self.crate_directory)}, which differs with the COMPSs version found locally: {COMPSS_VERSION}",
                TextColor.YELLOW,
            )

        try:
            crate = ROCrate(self.crate_directory)
            print_colored(
                f"THE RUN WAS: {get_create_action_name(crate)}", TextColor.YELLOW
            )
            global DATA_PERSISTENCE
            if not DATA_PERSISTENCE:
                data_persistence_false_verifier(self.crate_directory)
                global DPF
                DPF = True
                return

            if new_dataset_flag:
                new_dataset_info_collector(self.crate_directory)
            else:  # verify the metadata only if the old dataset is used
                # print("Reproducing the crate on the old dataset.")
                instrument = get_instument(crate)
                objects = get_objects_dict(crate)
                # download the remote data-set if it exists and return true if it exists
                (self.remote_dataset_flag, remote_dataset_dict) = remote_dataset(
                    crate, self.crate_directory
                )
                files_verifier(
                    self.crate_directory, instrument, objects, remote_dataset_dict
                )
            if provenance_flag:  # update the sources inside the yaml file
                update_yaml(self.crate_directory)

        except Exception as e:
            print_colored(e, TextColor.RED)
            sys.exit(1)

        self.log_folder = os.path.join(SUB_DIRECTORY_PATH, "log")

    def run(self):
        try:
            new_command = generate_command_line(self, SUB_DIRECTORY_PATH)
            initial_files = set(os.listdir(os.getcwd()))
            if self.provenance_flag:  # add the provenance flag to the command
                new_command.insert(1, "--provenance")

            previous_flags = get_previous_flags(
                self.crate_directory
            )  # get the previous flags to show the user as reference
            new_command = get_more_flags(
                new_command, previous_flags
            )  # ask user for more flags he/she wants to add to the final compss command
            new_command = get_change_values(new_command)

            result = executor(new_command, SUB_DIRECTORY_PATH)
            move_results_created(initial_files, SUB_DIRECTORY_PATH)

            return result

        except Exception as e:
            print_colored(e, TextColor.RED)
            return False


if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            print_colored(
                "Please provide the link or the path to the RO-Crate.", TextColor.RED
            )
            sys.exit(1)
        if len(sys.argv) > 2:
            print_colored(
                "Too many arguments provided. Please provide only the link or the path to the RO-Crate.",
                TextColor.RED,
            )
            sys.exit(1)
        print_welcome_message()
        NEW_DATASET_FLAG = False
        PROVENANCE_FLAG = False
        COMPSS_VERSION: str = (
            check_compss_version()
        )  # To check if compss is installed, if yes extract the version, else exit the program
        SLURM_CLUSTER = check_slurm_cluster()[
            0
        ]  # To check if the program is running on the SLURM cluster
        print("Slurm cluster:", SLURM_CLUSTER)
        SERVICE_PATH = os.path.dirname(os.path.abspath(__file__))
        print("Service path is:", SERVICE_PATH)
        SUB_DIRECTORY_PATH = create_new_execution_directory(SERVICE_PATH)
        print("Sub-directory path is:", SUB_DIRECTORY_PATH)

        te = open(
            os.path.join(SUB_DIRECTORY_PATH, "log/rs_log.txt"), "w", encoding="utf-8"
        )  #  for logging purposes

        sys.stdout = Unbuffered(sys.stdout)  # for logging

        link_or_path = sys.argv[1]  # take the link or path given by the user
        print(f"Source for crate: {link_or_path}")
        CRATE_PATH = get_workflow(SUB_DIRECTORY_PATH, link_or_path)
        # print("Crate path is:",CRATE_PATH)
        DATA_PERSISTENCE = get_data_persistence_status(CRATE_PATH)
        print_colored(
            f"DATA PERSISTENCE IN THE CRATE WAS: {DATA_PERSISTENCE}", TextColor.YELLOW
        )
        os.chdir(
            SUB_DIRECTORY_PATH
        )  # Avoid problems when relative paths are used as parameter

        if not SLURM_CLUSTER:
            NEW_DATASET_FLAG = get_yes_or_no(
                "Do you want to reproduce the crate on a new dataset?"
            )

        if not SLURM_CLUSTER or DATA_PERSISTENCE:  # can generate provenance for dpt
            PROVENANCE_FLAG = provenance_info_collector(
                SUB_DIRECTORY_PATH, SERVICE_PATH
            )

        rs = ReproducibilityService(PROVENANCE_FLAG, NEW_DATASET_FLAG)
        RESULT = False  # default value
        if DPF:
            # print(rs.crate_directory)
            RESULT = run_dpf(SUB_DIRECTORY_PATH, rs.crate_directory)
        else:
            RESULT = rs.run()
        if RESULT:
            print_colored(
                "Reproducibility Service has been executed successfully",
                TextColor.GREEN,
            )
        else:
            print_colored("Reproducibility Service has failed", TextColor.RED)

        if PROVENANCE_FLAG and not SLURM_CLUSTER:
            provenance_checker(SUB_DIRECTORY_PATH)

    except FileNotFoundError as e:
        print_colored(e, TextColor.RED)
        sys.exit(1)
    except ValueError as e:
        print_colored(e, TextColor.RED)
        sys.exit(1)
    except Exception as e:
        print_colored(e, TextColor.RED)
        sys.exit(1)

    sys.exit(0)
