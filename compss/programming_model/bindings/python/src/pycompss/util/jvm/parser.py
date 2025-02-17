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
PyCOMPSs Util - JVM - Configuration Parser.

This file contains all methods required to parse the jvm options file.
"""

from pycompss.util.typing_helper import typing


def convert_to_dict(
    jvm_opt_file: str,
) -> typing.Dict[str, typing.Union[bool, str]]:
    """Convert the JVM parameters of jvm_opt_file into a dictionary.

    :param jvm_opt_file: JVM parameters file.
    :return: Dictionary with the parameters specified on the file.
    """
    opts = {}  # type: typing.Dict[str, typing.Union[bool, str]]
    with open(jvm_opt_file) as jvm_opt_file_fd:
        for line in jvm_opt_file_fd:
            line = line.strip()
            if line:
                if line.startswith("-XX:"):
                    # These parameters have no value
                    key = line.split(":")[1].replace("\n", "")
                    opts[key] = True
                elif line.startswith("-D"):
                    key = line.split("=")[0]
                    value = line.split("=")[1].replace("\n", "")
                    value = value.strip()
                    opts[key] = value
                else:
                    key = line.replace("\n", "")
                    opts[key] = True
    return opts
