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
Get Workflow Module

Does different operations such as to get the workflow, get more flags,
and change values in the final command

"""
import os
import shutil
import zipfile
import urllib.parse

from utils import print_colored, print_colored_ns, TextColor, executor, get_yes_or_no


def get_workflow(execution_path: str, link_or_path: str) -> str:
    """
    Get the workflow from the path or link provided by the user.
    If path it may need to extract it to the workflow directory.

    Args:
        execution_path (str): The path to the execution directory.
        link_or_path (str): The link or path to the workflow.

    Raises:
        ValueError: It can occur if the file is not a valid zip file or the link is not a valid URL.
    Returns:
        str: The path to the workflow
    """
    workflow_path = os.path.join(execution_path, "Workflow")
    workflow_source = None

    if link_or_path.startswith("http"):
        workflow_source = "link"
    else:
        workflow_source = "path"

    if workflow_source == "path":
        print_colored(
            "WARNING: Please ensure this is the path to a zip file or a directory",
            TextColor.YELLOW,
        )
        crate_path = os.path.abspath(link_or_path)

        if zipfile.is_zipfile(crate_path):
            shutil.copy(crate_path, os.path.join(workflow_path, "my_crate.zip"))
        elif os.path.isdir(crate_path):
            return crate_path
        else:
            raise ValueError(f"The file at path {crate_path} is not a valid")

    elif workflow_source == "link":
        print_colored(
            "WARNING: Please try using the wget command to ensure the link works before submitting the link here.",
            TextColor.YELLOW,
        )
        print_colored(
            "Example: wget -O my_crate.zip https://example.com/my_crate.zip",
            TextColor.YELLOW,
        )
        crate_link = link_or_path
        # print("The link to download the crate is:", crate_link)
        if not urllib.parse.urlparse(crate_link).scheme in ["http", "https"]:
            raise ValueError("The link provided is not a valid URL.")

        executor(
            ["wget", "-O", os.path.join(workflow_path, "my_crate.zip"), crate_link],
            execution_path,
        )

    else:
        raise ValueError("Invalid input. Please enter 'path' or 'link'.")

    crate_zip_path = os.path.join(workflow_path, "my_crate.zip")

    try:
        with zipfile.ZipFile(crate_zip_path, "r") as zip_ref:
            zip_ref.extractall(workflow_path)
            names = zip_ref.namelist()
            cleaned = [name.rstrip("/") for name in names if name.strip()]
            common_path = os.path.commonpath(
                [os.path.join(workflow_path, name) for name in cleaned]
            )
        print(f"The workflow has been successfully extracted to {common_path}")
        return common_path  # returns crate path
    except zipfile.BadZipFile as e:
        raise ValueError(
            f"The file {crate_zip_path} is not a valid zip file or it is corrupted."
        )
    finally:
        os.remove(crate_zip_path)


def get_more_flags(command: list[str], previous_flags: list[str]) -> list[str]:
    """
    Get more flags from the user to add to the final command.

    Args:
        command (list[str]): current command
        previous_flags (list[str]): old flags as reference

    Returns:
        list[str]: new command
    """
    print_colored("The current command is as follows:", TextColor.YELLOW)
    print_colored(" ".join(command), TextColor.YELLOW)
    previous_flags_str = " ".join(previous_flags)
    print_colored(
        f"For Reference) The previously applied flags are as follows: {previous_flags_str}",
        TextColor.BLUE,
    )
    more = get_yes_or_no(
        "Do you want to add more flags to the compss runtime command shown above"
    )

    if not more:  # return if no more flags are needed
        return command

    print_colored(
        "WARNING: Submit the flags in one go. Example) Please enter the flags you want to add: --lang=python -d -p",
        TextColor.RED,
    )
    flag = input("Please enter the flags you want to add: ")
    flags: list[str] = flag.split(" ")
    for f in flags:
        command.insert(1, f)

    return command


def get_change_values(command: list[str]) -> list[str]:
    """
    Change the values of the final command based on the user input.

    Args:
        command (list[str]): The current command

    Raises:
        ValueError: If the user enters an invalid integer.

    Returns:
        list[str]: The new command
    """
    print_colored("The current command is as follows:", TextColor.YELLOW)
    print_colored_ns(" ".join(command), TextColor.YELLOW)
    if not get_yes_or_no("Do you want to change anything from the above command?"):
        return command
    else:
        n = len(command)
        print_colored("The current command indexed is as follows:", TextColor.YELLOW)
        for i in range(n):
            print_colored_ns(f"{i+1}. {command[i]}", TextColor.YELLOW)
        satisfied = True
        while satisfied:
            try:
                m = int(
                    input("How many values do you want to change? Enter the number: ")
                )
            except ValueError:
                print("Invalid input. Please enter a valid integer.")
                return command

            for i in range(m):
                try:
                    index = int(
                        input(
                            f"Enter the index of the value you want to change (1-{len(command)}): "
                        )
                    )
                    if index < 1 or index > len(command):
                        raise ValueError
                except ValueError:
                    print(
                        "Invalid input. Please enter a valid integer between 1 and the number of values."
                    )
                    return command

                command[index - 1] = input(
                    f"Enter the new value for index {index}: "
                ).strip()

            for i in range(n):
                print_colored_ns(f"{i+1}. {command[i]}", TextColor.YELLOW)
            print_colored("Are you happy with the changes above?", TextColor.YELLOW)
            satisfied = not get_yes_or_no("")
        return command
