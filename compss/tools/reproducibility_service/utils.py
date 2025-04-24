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
Utils Module

It contains utility functions that are used in the main script or other modules.
"""
import os
import yaml
import shutil
import subprocess
import threading
import time
import urllib.request
import zipfile
import sys

from ruamel.yaml import YAML
from rocrate.rocrate import ROCrate
from tabulate import tabulate


class TextColor:
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    RESET = "\033[0m"


def print_colored(text, color):
    """
    To print colored text in the console.
    Args:
        text (str):  The text to be printed.
        color (str):  The color to be used for printing the text.
    """
    print(f"\n{color}{text}{TextColor.RESET}\n")


def print_colored_ns(text, color):
    """
    To print colored text in the console without new line.
    Args:
        text (str):  The text to be printed.
        color (str):  The color to be used for printing the text.
    """
    print(f"{color}{text}{TextColor.RESET}")


def print_welcome_message():
    """
    To print the welcome message in the console.
    """
    welcome_text = """
    ╔════════════════════════════════════════════════════╗
    ║                                                    ║
    ║          Welcome to COMPSS Reproducibility         ║
    ║                 Service v1.0.0                     ║
    ║                                                    ║
    ║                COMPSS Version: 3.3.1               ║
    ║                                                    ║
    ║   Ensuring reproducibility in computational        ║
    ║   workflows with precision and reliability.        ║
    ║                                                    ║
    ║   Let's make your computations reproducible!       ║
    ║                                                    ║
    ╚════════════════════════════════════════════════════╝
    """
    print_colored(
        welcome_text, TextColor.GREEN
    )  # sleep for 1s for the user to see this
    time.sleep(1)


def get_by_id(entity: ROCrate, id: str):
    """
    To parse the ROCrate and get the entity with the specified ID.
    Args:
        entity (ROCrate): The ROCrate object.
        id (str): The ID of the entity to be retrieved.

    Returns:
        _type_: None if not found else the entity with the specified ID.
    """
    # Loop through all entities in the RO-Crate
    for entity in entity.get_entities():
        if entity.id == id:
            return entity
    return None


def get_Create_Action(crate: ROCrate):
    """
    To get the CreateAction entity from the ROCrate.
    Args:
        entity (ROCrate): The ROCrate object.

    Returns:
        _type_: None if not found else the CreateAction entity.
    """
    # Loop through all entities in the RO-Crate
    for entity in crate.get_entities():
        if entity.type == "CreateAction":
            return entity
    return None


def get_instument(entity: ROCrate):
    """
    To get the instrument ID from the CreateAction entity.
    Args:
        entity (ROCrate): The ROCrate object.

    Returns:
        _type_: The ID of the instrument.
    """
    createAction = get_Create_Action(entity)
    return createAction["instrument"].id


def get_objects(entity: ROCrate) -> list[str]:
    """
    To get the objects from the CreateAction entity.
    Args:
        entity (ROCrate): The ROCrate object.

    Returns:
        _type_: A list of object IDs.
    """
    createAction = get_Create_Action(entity)
    objects = []
    if "object" in createAction:
        # It is not necessary to have inputs/objects in Create Action
        temp = createAction["object"]
    else:
        return objects  # Empty

    for val in temp:
        if "hasPart" in val:
            for has in val["hasPart"]:
                objects.append(has.id)
        else:
            objects.append(val.id)
    return objects


def get_results_dict(entity: ROCrate) -> dict:
    """
    To get the results from the CreateAction entity.
    Args:
        entity (ROCrate): The ROCrate object.

    Returns:
        _type_: A dictionary mapped from the result name to the result ID.
    """
    createAction = get_Create_Action(entity)
    results = {}
    if "result" in createAction:
        # It is not necessary to have inputs/objects in Create Action
        temp = createAction["result"]
    else:
        return results  # Empty

    for result in temp:
        results[result["name"]] = result.id
    return results


def get_objects_dict(entity: ROCrate) -> dict:
    """
    To get the objects from the CreateAction entity.
    Args:
        entity (ROCrate): The ROCrate object.

    Returns:
        dict:A dict of (name,id) -> id , so that it does'nt collide with the same name
    """
    createAction = get_Create_Action(entity)
    objects = {}
    if "object" in createAction:
        # It is not necessary to have inputs/objects in Create Action
        temp = createAction["object"]
    else:
        return objects  # Empty

    for input in temp:
        if "hasPart" in input:
            # if hasPart exists, it means it is a composite object
            for has in input["hasPart"]:
                objects[(has["name"], has.id)] = has.id
        else:
            objects[(input["name"], input.id)] = input.id
            # else it is just a single object
    return objects


def get_create_action_name(entity: ROCrate) -> str:
    """
    Gets the COMPSs execution details in CreateAction["name"].
    Args:
        entity (ROCrate): The ROCrate object.

    Returns:
        str: The CreateAction["name"].
    """
    createAction = get_Create_Action(entity)
    return createAction["name"]


def key_exists_with_first_element(d, first_element):
    return any(key[0] == first_element for key in d)


def get_file_names(folder_path: str) -> dict:
    """
        To get the file names from the specified folder path.
    Args:
        folder_path (str): path to the folder.

    Returns:
        dict: dictionary of file names and their full paths.
    """
    file_names = {}
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_names[file] = os.path.join(root, file)
    return file_names


def get_compss_crate_version(crate_path: str) -> str:
    """
    Gets the COMPSs version used to create the ROCrate.
    Args:
        crate_path (str): The path to the ROCrate.

    Returns:
        float: The version of COMPSs used to create the ROCrate.
    """
    crate = ROCrate(crate_path)
    compss_object = get_by_id(crate, "#compss")
    return compss_object["version"]


def get_yes_or_no(msg: str):
    """
    To get the user input as 'y' or 'n'.
    Args:
        msg (str): The message outputed to the user

    Returns:
        _type_: True if 'y' else False if 'n'.
    """
    while True:
        user_input = input(f"{msg} (y/n):").lower()
        if user_input == "y" or user_input == "n":
            if user_input == "y":
                return True
            else:
                return False
        else:
            print("Invalid input. Please enter 'y' or 'n'.")


def get_data_persistence_status(crate_path: str) -> bool:
    """
    To get data_persistence status from ro-crate-yaml file.
    Args:
        crate_path (str): Path for the crate directory.
    Raises:
        FileNotFoundError: If ro-crate-info.yaml file not found in the crate.

    Returns:
        bool: True if data_persistence is True else False.
    """
    # It may not be named ro-crate-info.yaml, eg: 838-1 crate
    yaml_file_path = None
    for name in os.listdir(crate_path):
        if name.endswith(".yaml"):
            yaml_file_path = f"{crate_path}/{name}"
            break
    if not yaml_file_path:
        raise FileNotFoundError("YAML file not found in the crate")
    # Open and read the YAML file
    with open(yaml_file_path, "r") as file:
        # Load the content of the YAML file
        config = yaml.safe_load(file)
    # Extract the value of data_persistence
    data_persistence = config.get("COMPSs Workflow Information", {}).get(
        "data_persistence", None
    )

    return data_persistence


def get_name_and_description(crate_path: str) -> tuple:
    """
    To get the name and description from the ro-crate-info.yaml file.
    Args:
        crate_path (str): Path to the crate directory.

    Returns:
        tuple: returns tuple of name, description and authors.
    """
    # It may not be named ro-crate-info.yaml, eg: 838-1 crate
    yaml_file_path = None
    for name in os.listdir(crate_path):
        if name.endswith(".yaml"):
            yaml_file_path = f"{crate_path}/{name}"
            break
    if not yaml_file_path:
        raise Exception("ro-crate-info.yaml file not found in the crate")
    # Open and read the YAML file
    with open(yaml_file_path, "r") as file:
        # Load the content of the YAML file
        data = YAML().load(file)

    # Extract name and description
    name = data["COMPSs Workflow Information"].get("name", "")
    description = data["COMPSs Workflow Information"].get("description", "")
    authors = data["Authors"]

    return name, description, authors


def get_ro_crate_info(execution_path: str, service_path: str):
    """
    Copies a ro-crate-info.yaml files to the current working directory.

    """
    source = os.path.join(service_path, "APP-REQ/ro-crate-info.yaml")
    # Get the current working directory
    cwd = os.getcwd()
    file_name = "ro-crate-info.yaml"
    # Construct the full destination path
    destination = os.path.join(cwd, file_name)

    try:
        # Copy the file to the current working directory
        shutil.copy(source, destination)
        # print(f'ro-crate-info.yaml file copied to the current working directory.')
    except Exception as e:
        print(f"Error copying ro-crate-info.yaml file from {source}: {e}")


def executor(command: list[str], execution_path: str):
    """
    Uses subprocess librray to execute the command and logs the output to a file.
    Args:
        command (list[str]): list of str containing the command to be executed
        execution_path (str): path to the execution directory

    Returns:
        _type_: True if the command executed successfully else False.
    """
    joined_command = " ".join(command)
    print_colored(f"Executing command: {joined_command}", TextColor.BLUE)

    # Create log directory if it doesn't exist
    log_dir = os.path.join(execution_path, "log")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Define log file names
    stdout_log = os.path.join(log_dir, "out.log")
    stderr_log = os.path.join(log_dir, "err.log")

    # Start the subprocess
    process = subprocess.Popen(
        command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    # Define functions to read and log output
    def read_stdout(pipe, log_file):
        with open(log_file, "a") as f:
            for line in iter(pipe.readline, ""):
                print(line.strip())
                f.write(line)
        pipe.close()

    def read_stderr(pipe, log_file):
        with open(log_file, "a") as f:
            for line in iter(pipe.readline, ""):
                print(line.strip())
                f.write(line)
        pipe.close()

    # Create threads to read and log stdout and stderr
    stdout_thread = threading.Thread(
        target=read_stdout, args=(process.stdout, stdout_log)
    )
    stderr_thread = threading.Thread(
        target=read_stderr, args=(process.stderr, stderr_log)
    )

    stdout_thread.start()
    stderr_thread.start()

    # Wait for the process to complete
    process.wait()

    # Ensure all output has been logged
    stdout_thread.join()
    stderr_thread.join()

    # Print log file locations
    print(f"Standard output logged to: {stdout_log}")
    print(f"Standard error logged to: {stderr_log}")

    # Check the return code
    if process.returncode == 0:
        print("Command executed successfully.")
        return True
    else:
        print("Command failed with return code:", process.returncode)
        return False


def download_file(url: str, download_path: str, file_name: str):
    """
    Downloads a file from the specified URL and saves it to the specified path.
    Args:
        url (str):  The URL of the file to be downloaded.
        download_path (str):  The path where the file should be saved.
        file_name (str):  The name of the file to be saved.
    """
    if not os.path.exists(download_path):
        os.makedirs(download_path)
    # Create the full path to the file
    full_path = os.path.join(download_path, file_name)
    print_colored(
        f"Downloading {file_name} from {url} to {full_path}, please wait...",
        TextColor.YELLOW,
    )
    # Download the file and save it to the specified path
    urllib.request.urlretrieve(url, full_path)
    print(f"File downloaded as {full_path}")
    # Check if the file is a zip file and extract it
    if zipfile.is_zipfile(full_path):
        with zipfile.ZipFile(full_path, "r") as zip_ref:
            zip_ref.extractall(download_path)
        print(f"Extracted {file_name} in {download_path}")

        # Remove the zip file after extraction
        os.remove(full_path)
        print(f"Removed the zip file {file_name}")


def check_compss_version() -> str:
    """
    To check the version of COMPSs installed on the system.
    Returns:
        float: The version of COMPSs installed on the system.
    """
    try:
        # Execute the command
        result = subprocess.run(
            ["runcompss", "-v"], capture_output=True, text=True, check=True
        )

        # Parse the output
        output = result.stdout.strip()
        if "COMPSs version" in output:
            version = output.split("COMPSs version ")[1].split(" ")[0]
            print(f"COMPSs Version Found: {version}")
            return version
        else:
            return "COMPSs version not found in the output."

    except subprocess.CalledProcessError as e:
        return f"An error occurred while trying to get COMPSs version: {e}"
    except FileNotFoundError:
        return "runcompss command not found. Please ensure that COMPSs is installed and the command is available in your PATH."


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
    except Exception as e:
        return False, str(e)

    return False, "squeue command failed without raising an exception"


def get_previous_flags(crate_path: str) -> list[str]:
    """
    To get the previous flags used in the COMPSs submission command.

    Args:
        crate_path (str): The path to the crate directory.

    Returns:
        list[str]: A list of the previous flags used in the COMPSs submission command.
    """
    # compss_submission_command_path = os.path.join(crate_path, "compss_submission_command_line.txt")
    #
    # with open(compss_submission_command_path, 'r', encoding='utf-8') as file:
    #     command = next(file).strip()

    command = get_submission_command(crate_path)
    previous_flags = []
    for cmd in command.split():
        _, ext = os.path.splitext(cmd)
        if ext:
            break
        if cmd.startswith("--") or cmd.startswith("-"):
            if not (cmd.startswith("--provenance") or cmd.startswith("-p")):
                previous_flags.append(cmd)

    return previous_flags


def print_symbol_reference():
    """
    To print the reference line for the symbols used in the file status table
    """
    references = {"✔": "SUCCESS", "✘": "FAILURE", "–": "NOT IN METADATA"}

    # Create a single line of references
    reference_line = " | ".join(
        f"{symbol}: {meaning}" for symbol, meaning in references.items()
    )

    print(reference_line)


# Function to convert status codes to symbols
def get_status_symbol(file_exists, file_size_verified):
    """
    To convert the status codes to symbols.
    """
    exists_symbol = "✅" if file_exists == 1 else "❌"
    size_verified_symbol = (
        "✅" if file_size_verified == 1 else "❌" if file_size_verified == 0 else "—"
    )
    return exists_symbol, size_verified_symbol


# Function to wrap the file path based on a length limit
def wrap_text(text, width):
    """
    To wrap the text based on the specified width.
    """
    return "\n".join([text[i : i + width] for i in range(0, len(text), width)])


# Function to generate the table
def generate_file_status_table(file_status_list, Third_field: str, path_width_limit=40):
    """
    To generate a table to display the file status.
    Args:
        file_status_list (): list of tuples containing the file status information.
        Third_field (str): The name of the third field in the table.
    """
    table = []
    # Adding header row
    table.append(["", "Metadata File Name", "Host File Path", Third_field, "Size"])

    # Adding file status rows
    for i, (filename, file_path, file_exists, file_size_verified) in enumerate(
        file_status_list, start=1
    ):
        exists_symbol, size_verified_symbol = get_status_symbol(
            file_exists, file_size_verified
        )

        # Wrap the file path if it exceeds the specified width limit
        wrapped_file_path = wrap_text(file_path, path_width_limit)
        wrapped_filename = wrap_text(filename, path_width_limit)

        table.append(
            [
                i,
                wrapped_filename,
                wrapped_file_path,
                exists_symbol,
                size_verified_symbol,
            ]
        )

    # Print the table
    print(tabulate(table, headers="firstrow", tablefmt="grid"))
    print_symbol_reference()


def get_submission_command(crate_path: str) -> str:
    new_command = ""
    compss_submission_command_path = os.path.join(
        crate_path, "compss_submission_command_line.txt"
    )
    if os.path.isfile(compss_submission_command_path):
        with open(compss_submission_command_path, "r", encoding="utf-8") as file:
            new_command = next(file).strip()
    else:
        crate = ROCrate(crate_path)
        for e in crate.get_entities():
            if "#COMPSs_Workflow_Run_Crate_" in e.id:
                new_command = e["description"]
                if not new_command.startswith(("runcompss", "enqueue_compss")):
                    print_colored(
                        f"The command found is this: \n{new_command}", TextColor.YELLOW
                    )
                    print_colored(
                        "Error: the command found is not valid.", TextColor.RED
                    )
                    sys.exit(0)
                break
    return new_command
