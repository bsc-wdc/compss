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
import typing

from urllib.parse import urlsplit


def fix_dir_url(in_url: str) -> str:
    """
    Fix dir:// URL returned by the runtime, change it to file:// and ensure it ends with '/'

    :param in_url: URL that may need to be fixed

    :returns: A file:// URL
    """

    runtime_url = urlsplit(in_url)
    if (
        runtime_url.scheme == "dir"
    ):  # Fix dir:// to file:// and ensure it ends with a slash
        new_url = "file://" + runtime_url.netloc + runtime_url.path
        if new_url[-1] != "/":
            new_url += "/"  # Add end slash if needed
        return new_url
    # else
    return in_url  # No changes required


def fix_in_files_at_out_dirs(
    inputs_list: list, outputs_list: list
) -> typing.Tuple[list, list]:
    """
    Remove any file inputs that the user may have declared as directory outputs

    :param inputs_list: list of all input directories and files as URLs
    :param compss_wf_info: list of all output directories and files as URLs


    :returns: Updated inputs and outputs lists
    """

    # Now erase any file:// input that is included as dir:// output
    directories_list = []
    file_found = False
    for item in outputs_list:
        url_parts = urlsplit(item)
        if url_parts.scheme == "dir":
            if url_parts.path not in directories_list:
                directories_list.append(url_parts.path)
        else:
            file_found = True
            break

    i = 0
    file_found = False
    for item in inputs_list:
        url_parts = urlsplit(item)
        if url_parts.scheme == "dir":
            i += 1
        else:
            file_found = True
            break

    if not file_found:
        return inputs_list, outputs_list

    url_files_list = inputs_list[i:]  # Slice out directories
    for item in url_files_list:
        url_parts = urlsplit(item)
        if any(url_parts.path.startswith(dir_path) for dir_path in directories_list):
            print(
                f"PROVENANCE | WARNING: Metadata of an input file has been removed since it is included at an output directory: {url_parts.path}"
            )
            inputs_list.remove(item)

    # print(f"PROVENANCE DEBUG | RESULT FROM fix_in_files_at_out_dirs:\n {inputs_list}")

    return inputs_list, outputs_list
