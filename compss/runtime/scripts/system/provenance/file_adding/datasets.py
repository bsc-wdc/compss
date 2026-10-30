#!/usr/bin/env python3
#
#  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
import atexit
import datetime as dt
import os
import socket
import tempfile
from datetime import timezone
from pathlib import Path
from urllib.parse import urlsplit

import requests

from provenance.utils.url_fixes import fix_dir_url, write_external_url
from rocrate.model import ContextEntity
from rocrate.rocrate import ROCrate
from rocrate.utils import iso_now

from models.Task import Task

EMPTY_FILE = None  # Source of the .gitkeep files of empty directories, see get_empty_file
PRINTED_WARNINGS = set()  # A directory is processed once per reference: print each warning once


def get_description_plot(metric, node_name="unknown node"):
    description_plots = {
        "cpu_aggregated": f"Profiling plot showing the percentage of CPU used during workflow run, aggregating all nodes",
        "mem_aggregated": f"Profiling plot showing the amount of memory used during workflow run, aggregating all nodes",
        "gpu_usage_aggregated": f"Profiling plot showing the percentage of GPU used during workflow run, aggregating all nodes",
        "gpu_mem_aggregated": f"Profiling plot showing the amount of GPU memory used during workflow run, aggregating all nodes",
        "cpu": f"Profiling plot for node {node_name} showing the percentage of CPU used during workflow run",
        "mem": f"Profiling plot for node {node_name} showing the amount of memory used during workflow run",
        "gpu_usage": f"Profiling plot for node {node_name} showing the percentage of GPU used during workflow run",
        "gpu_mem": f"Profiling plot for node {node_name} showing the amount of GPU memory used during workflow run",
        "disk_usage": f"Profiling plot for node {node_name} showing the cumulative amount of data read and written on the disk during workflow run",
        "network_usage": f"Profiling plot for node {node_name} showing the cumulative amount of data sent and received during workflow run",
        "cpu_nodes": "Profiling plot for the percentage of CPU for all nodes during workflow run",
        "mem_nodes": "Profiling plot for the percentage of memory usage for all nodes during workflow run",
        "gpu_nodes": "Profiling plot for the percentage of GPU used for all nodes during workflow run",
        "gpu_mem_nodes": "Profiling plot for the percentage of GPU memory used for all nodes during workflow run",
        # The following plots represent bursts over time and are currently unused
        "bytes_read": "Profiling plot for the amount of data read from the disk during workflow run",
        "bytes_written": "Profiling plot for the amount of data written from the disk during workflow run",
        "bytes_sent": "Profiling plot for the amount of data sent across the network during workflow run",
        "bytes_received": "Profiling plot for the amount of data received across the network during workflow run",
    }

    if metric not in description_plots:
        return f"Profiling plot for node {node_name} showing metric {metric} during workflow run"

    return description_plots[metric]


def get_crate_path(path: str, common_paths: list) -> str:
    """
    Get the path of a dataset file or directory in the crate (dataset/...): its path relative to the working
    directory when it is inside it, so a reproducibility service can recreate it where the application expects it
    (e.g. dataset/inputs/A/ for the DIRECTORY parameter inputs/A); otherwise, its path below the identified common
    path that contains it, under the name of that common path.

    :param path: Absolute path of the file, or of the directory (ending with '/')
    :param common_paths: List of identified common paths among all dataset files, all finish with '/'

    :returns: The path in the crate, starting with dataset/
    """
    # When this script runs, the user application has finished, so cwd is the folder where the application was started
    cwd_endslash = os.getcwd() + "/"  # os.getcwd does not add the final slash
    # BSC hack, inconsistent behaviour in MN5. /gpfs/home/ and /home/ are equivalent. Randomly, sometimes we get
    # /home/ and others /gpfs/home at the dataprovenance.log and getting paths
    if not path.startswith("/gpfs"):
        cwd_endslash = cwd_endslash.removeprefix("/gpfs")
    if path.startswith(cwd_endslash) and len(path) > len(cwd_endslash):
        # Workingdir dataset file or folder, add it to the root keeping its relative path
        return "dataset/" + path[len(cwd_endslash) :]
    for item in common_paths:  # All files must have a match
        if path.startswith(item):
            # Base name of the identified common path. It does not avoid collisions if the user defines the same
            # folder name in two different locations
            return "dataset/" + Path(item).name + "/" + path[len(item) :]
    return "dataset/" + Path(path).name + ("/" if path.endswith("/") else "")


def print_warning_once(message: str) -> None:
    """
    Print a PROVENANCE warning only the first time it appears

    :param message: Text of the warning

    :returns: None
    """
    if message not in PRINTED_WARNINGS:
        PRINTED_WARNINGS.add(message)
        print(f"PROVENANCE | WARNING: {message}")


def get_empty_file() -> str:
    """
    Get an empty temporary file, used as the source of the .gitkeep files that represent empty directories in the
    crate, so the user's folders are never modified. Removed when the generator ends

    :returns: Path of the empty file
    """
    global EMPTY_FILE
    if EMPTY_FILE is None:
        fd, EMPTY_FILE = tempfile.mkstemp(prefix="compss_provenance_gitkeep_")
        os.close(fd)
        atexit.register(lambda: os.path.exists(EMPTY_FILE) and os.remove(EMPTY_FILE))
    return EMPTY_FILE


def add_missing_entity(
    compss_crate: ROCrate, in_url: str, reason: str, missing_details: dict = None
) -> str:
    """
    Add an entity without details for a file or directory that could not be added, so the references to it point to
    an existing entity. It uses the same external scp:// identifier as the existing files (file:// is only the
    runtime's internal URL). The warning is printed only once, when the entity is created

    :param compss_crate: The COMPSs RO-Crate being generated
    :param in_url: File or directory URL, as found in the logs
    :param reason: Why it could not be added, for the warning
    :param missing_details: Size, date and task of the last version written by a finished task, by URL, to describe
        the files that are not found at their location (see get_missing_file_details)

    :returns: The @id of the entity
    """
    url_parts = urlsplit(in_url)
    missing_id = write_external_url(fix_dir_url(in_url))
    missing_file = compss_crate.get(missing_id)
    if missing_file is None:
        print(f"PROVENANCE | WARNING: Could not add {url_parts.path}. {reason}")
        missing_type = "Dataset" if url_parts.scheme == "dir" or in_url.endswith("/") else "File"
        missing_file = compss_crate.add(ContextEntity(compss_crate, missing_id, properties={"@type": missing_type}))
        compss_crate.root_dataset.append_to("hasPart", missing_file)
    # If a finished task wrote the file, describe the last version it wrote (the workflow-level inputs and
    # outputs are added before the worker logs are read, so the details may come in a later call)
    details = (missing_details or {}).get(in_url)
    if details and "contentSize" not in missing_file.properties():
        missing_file["name"] = Path(url_parts.path).name
        missing_file["contentSize"] = details["contentSize"]
        missing_file["dateModified"] = details["dateModified"]
        missing_file["description"] = (
            f"Last version of the file, written by task {details['task']}. The file was not found at its "
            f"location when the provenance was generated (deleted with compss_delete_file(), or not copied back "
            f"because the run stopped early)"
        )
    return missing_id


def get_date_modified(path: str) -> str:
    """
    Get the last modification date of a file or directory, in ISO format (UTC, seconds)

    :param path: Path of the file or directory

    :returns: The date
    """
    return (
        dt.datetime.fromtimestamp(os.path.getmtime(path), timezone.utc)
        .replace(microsecond=0)
        .isoformat()
    )


def add_dataset_file_to_crate(
    compss_crate: ROCrate, in_url: str, persist: bool, common_paths: list, missing_details: dict = None
) -> str:
    """
    Add the file (or a reference to it) belonging to the dataset of the application (both input or output)
    When adding local files that we don't want to be physically in the Crate, they must be added with a file:// URI
    CAUTION: If the file has been already added (e.g. for INOUT files) add_file won't succeed in adding a second entity
    with the same name

    :param compss_crate: The COMPSs RO-Crate being generated
    :param in_url: File added as input or output
    :param persist: True to attach the file to the crate, False otherwise
    :param common_paths: List of identified common paths among all dataset files, all finish with '/'
    :param missing_details: Size, date and task of the last version written by a finished task, by URL, to describe
        the files that are not found at their location (see get_missing_file_details)

    :returns: The crate path if persist is true, the external scp:// URL if persist is false. An entity without
        details if the file or directory could not be added. None if in_url is not a URL
    """

    if not isinstance(in_url, str) or not in_url:
        print(f"PROVENANCE | WARNING: Could not process URL: {in_url}")
        return None

    try:
        # urlsplit returns in url_parts.path everything when it is a File, not only the directory that contains it
        url_parts = urlsplit(in_url)
        # dir:// URLs must come with a final '/'. Ensure that the directory URL ends with '/'
        if url_parts.scheme == "dir" and not in_url.endswith("/"):
            in_url += "/"
            url_parts = urlsplit(in_url)
        # If in_url ends up with '/', os.path.basename will be empty, thus we need Pathlib to get final_item_name
        url_path = Path(url_parts.path)
        final_item_name = url_path.name

        if url_parts.scheme in ["dir", "file"]:
            # Dealing with a local file
            file_properties = {
                "name": final_item_name,
                "sdDatePublished": iso_now(),  # Register when the Data Entity was last accessible
                "dateModified": get_date_modified(url_parts.path),  # Schema.org
            }
        else:
            # Remote file
            file_properties = {"name": final_item_name}

        if url_parts.scheme == "file":  # Dealing with a local file
            file_properties["contentSize"] = os.path.getsize(url_parts.path)
            crate_path = get_crate_path(url_parts.path, common_paths)
            if persist:  # Remove scheme so it is added as a regular file
                if __debug__:
                    print(
                        f"PROVENANCE DEBUG | Adding SINGLE FILE {url_parts.path} as {crate_path}"
                    )
                compss_crate.add_file(
                    source=url_parts.path,
                    dest_path=crate_path,
                    properties=file_properties,
                )
                return crate_path
            # Adding 'localPath' to the entity so that a reproducibility service knows
            # where to place the data on the filesystem when re-executing the workflow.
            file_properties["localPath"] = crate_path
            # URL rewriting to scp:// is applied only at the point of adding to the crate,
            # not during processing.
            modified_url = write_external_url(in_url)
            compss_crate.add_file(
                modified_url,
                fetch_remote=False,
                validate_url=False,  # True fails at MN4 when file URI points to a node hostname (only localhost works)
                properties=file_properties,
            )
            return modified_url

        if url_parts.scheme == "dir":  # DIRECTORY parameter
            return add_directory_to_crate(compss_crate, in_url, url_parts, file_properties, persist, common_paths)

        if url_parts.scheme.startswith("http"):
            # Remote file, currently not supported in COMPSs. validate_url=True already adds contentSize and
            # encodingFormat from the remote file
            if compss_crate.get(in_url) is not None:
                return in_url  # Already added (and checked) for a previous reference
            val_url = True
            if os.getenv("BSC_MACHINE"):
                # Cluster without outside connectivity (e.g. at BSC)
                val_url = False
            else:
                # ro-crate-py validates the URL when writing the crate (requests.head, no timeout), and an unreachable
                # URL would make the whole crate fail. Check it now
                try:
                    requests.head(in_url, timeout=10, allow_redirects=True).close()
                except requests.RequestException as exc:
                    print_warning_once(f"Could not reach {in_url}, added without validation ({exc.__class__.__name__})")
                    val_url = False
            compss_crate.add_file(
                source=in_url,
                validate_url=val_url,
                fetch_remote=False,
                properties=file_properties,
            )
            return in_url

    except FileNotFoundError:
        return add_missing_entity(compss_crate, in_url, "The file does not exist.", missing_details)
    except Exception as exc:
        return add_missing_entity(compss_crate, in_url, f"Unexpected error: {exc!r}", missing_details)

    # Other schemes
    return write_external_url(fix_dir_url(in_url)) if not persist else fix_dir_url(in_url)


def add_directory_to_crate(
    compss_crate: ROCrate, in_url: str, url_parts, dir_properties: dict, persist: bool, common_paths: list
) -> str:
    """
    Add a directory parameter, and all the files and empty sub-directories inside it. A file or sub-directory that
    cannot be added (e.g. a broken symlink) is skipped with a warning, the rest of the directory is added

    :param compss_crate: The COMPSs RO-Crate being generated
    :param in_url: dir:// URL of the directory, ending with '/'
    :param url_parts: in_url split by urlsplit
    :param dir_properties: Properties of the directory entity (name, dates)
    :param persist: True to attach the directory to the crate, False otherwise
    :param common_paths: List of identified common paths among all dataset files, all finish with '/'

    :returns: The crate path if persist is true, the external scp:// URL if persist is false
    """
    # Path of the directory in the crate (persist) or relative to the working directory (localPath)
    dir_crate_path = get_crate_path(url_parts.path, common_paths)  # url_parts.path ends with '/'
    if not persist:
        dir_properties["localPath"] = dir_crate_path

    # For directories, describe all files inside the directory
    has_part_list = []
    for root, dirs, files in os.walk(
        url_parts.path, topdown=True, followlinks=True
    ):  # Ignore references to sub-directories (they are not a specific in or out of the workflow),
        # but not their files
        if "__pycache__" in root:
            continue  # We skip __pycache__ subdirectories
        dirs.sort()
        files.sort()
        for f_name in files:
            if f_name.startswith("*"):
                # Avoid dealing with symlinks with wildcards
                continue
            listed_file = os.path.join(root, f_name)
            # url_parts.path includes a final '/'
            filtered_url = listed_file[len(url_parts.path) :]  # Does not include an initial '/'
            try:
                dir_f_properties = {
                    "name": f_name,
                    "sdDatePublished": iso_now(),  # Register when the Data Entity was last accessible
                    "dateModified": get_date_modified(listed_file),  # Schema.org
                    "contentSize": os.path.getsize(listed_file),
                }
            except OSError as exc:
                # E.g. a broken symlink, or a file deleted meanwhile
                print_warning_once(f"Could not add {listed_file} inside directory {url_parts.path} ({exc})")
                continue
            if persist:
                dir_f_url = dir_crate_path + filtered_url
                if __debug__:
                    print(f"PROVENANCE DEBUG | Adding DATASET FILE {listed_file} as {dir_f_url}")
                compss_crate.add_file(
                    source=listed_file,
                    dest_path=dir_f_url,
                    fetch_remote=False,
                    validate_url=False,
                    # True fails at MN4 when file URI points to a node hostname (only localhost works)
                    properties=dir_f_properties,
                )
                has_part_list.append({"@id": dir_f_url})
            else:
                dir_f_properties["localPath"] = dir_crate_path + filtered_url
                modified_url = write_external_url("file://" + url_parts.netloc + listed_file)
                compss_crate.add_file(
                    modified_url,
                    fetch_remote=False,
                    validate_url=False,
                    # True fails at MN4 when file URI points to a node hostname (only localhost works)
                    properties=dir_f_properties,
                )
                has_part_list.append({"@id": modified_url})

        for dir_name in dirs:
            # Check if it's an empty directory, needs to be added by hand
            full_dir_name = os.path.join(root, dir_name)
            try:
                if os.listdir(full_dir_name):
                    continue
                empty_dir_properties = {
                    "sdDatePublished": iso_now(),  # Register when the Data Entity was last accessible
                    "dateModified": get_date_modified(full_dir_name),  # Schema.org
                }
            except OSError as exc:
                print_warning_once(f"Could not add {full_dir_name} inside directory {url_parts.path} ({exc})")
                continue
            if __debug__:
                print(
                    f"PROVENANCE DEBUG | Adding an empty directory. root ({root}), full_dir_name ({full_dir_name})"
                )
            if persist:
                # Workaround to add empty directories in a git repository. The .gitkeep only exists in the crate
                empty_dir_properties["name"] = ".gitkeep"
                dir_f_url = dir_crate_path + full_dir_name[len(url_parts.path) :] + "/" + ".gitkeep"
                compss_crate.add_file(
                    source=get_empty_file(),
                    dest_path=dir_f_url,
                    fetch_remote=False,
                    validate_url=False,
                    properties=empty_dir_properties,
                )
                has_part_list.append({"@id": dir_f_url})
            else:
                empty_dir_properties["name"] = dir_name
                empty_dir_properties["localPath"] = dir_crate_path + full_dir_name[len(url_parts.path) :] + "/"
                # Directories must finish with slash
                modified_url = write_external_url("file://" + url_parts.netloc + full_dir_name + "/")
                compss_crate.add_dataset(source=modified_url, properties=empty_dir_properties)
                has_part_list.append({"@id": modified_url})

    if persist and not os.listdir(url_parts.path):
        # The root directory itself is empty. Workaround to add empty directories in a git repository. The
        # .gitkeep only exists in the crate
        if __debug__:
            print(f"PROVENANCE DEBUG | Adding an empty directory: {url_parts.path}")
        gitkeep_path = dir_crate_path + ".gitkeep"
        compss_crate.add_file(
            source=get_empty_file(),
            dest_path=gitkeep_path,
            fetch_remote=False,
            validate_url=False,
            properties={
                "name": ".gitkeep",
                "sdDatePublished": dir_properties["sdDatePublished"],
                "dateModified": dir_properties["dateModified"],
            },
        )
        has_part_list.append({"@id": gitkeep_path})

    if has_part_list:
        dir_properties["hasPart"] = has_part_list
    if persist:
        if __debug__:
            print(f"PROVENANCE DEBUG | Adding DATASET DIRECTORY {url_parts.path} as {dir_crate_path}")
        # Without source: its files are added one by one above. With the folder as source, ro-crate-py would also copy
        # the files skipped (e.g. a broken symlink, which makes writing the crate fail)
        compss_crate.add_dataset(source=None, dest_path=dir_crate_path, properties=dir_properties)
        return dir_crate_path
    # Directories must finish with slash
    modified_url = write_external_url(fix_dir_url(in_url))
    compss_crate.add_dataset(modified_url, properties=dir_properties)
    return modified_url


def add_manual_datasets(
    yaml_term: str, compss_wf_info: dict, data_list: list, info_yaml: str
) -> list:
    """
    Adds to a list of dataset entities (files or directories) the ones specified by the user. At the end, removes any
    file:// references that belong to other dir:// references

    :param yaml_term: Term specified in the YAML file (i.e. 'inputs' or 'outputs')
    :param compss_wf_info: YAML dict to extract info form the application, as specified by the user
    :param data_list: Sorted list of file and dir URLs as generated by COMPSs runtime
    :param info_yaml: Name of the YAML file specified by the user

    :returns: Updated List of identified common paths among the URLs
    """

    # Add entities defined by the user
    # Input files or directories added by hand from the user
    data_entities_list = []
    if isinstance(compss_wf_info[yaml_term], list):
        data_entities_list.extend(compss_wf_info[yaml_term])
    else:
        data_entities_list.append(compss_wf_info[yaml_term])
    for item in data_entities_list:
        # Check if remote file with URI scheme: http or https
        url_parts = urlsplit(item)
        if url_parts.scheme.startswith("http"):
            data_list.append(item)
            continue

        path_data_entity = Path(item).expanduser()

        if not path_data_entity.exists():
            print(
                f"PROVENANCE | WARNING: A file or directory defined as '{yaml_term}' in {info_yaml} does not exist "
                f"({item})"
            )
            continue
        resolved_data_entity = str(path_data_entity.resolve())
        # MN5 Hack: sometimes we get /gpfs/home, sometimes /home. Normalise to /home
        if os.getenv("BSC_MACHINE") and resolved_data_entity.startswith("/gpfs/home/"):
            resolved_data_entity = resolved_data_entity.removeprefix("/gpfs")
        if os.path.isfile(resolved_data_entity):
            new_data_entity = "file://" + socket.gethostname() + resolved_data_entity
        elif os.path.isdir(resolved_data_entity):
            new_data_entity = (
                "dir://" + socket.gethostname() + resolved_data_entity + "/"
            )
        else:
            print(
                f"FATAL ERROR: a reference is neither a file, nor a directory ({resolved_data_entity})"
            )
            raise FileNotFoundError
        if new_data_entity not in data_list:
            # Checking if a file is in a dir would be costly
            data_list.append(new_data_entity)
        else:
            print(
                f"PROVENANCE | WARNING: A file or directory defined as '{yaml_term}' in {info_yaml} was already part of the dataset "
                f"({item})"
            )
    data_list.sort()  # Sort again, needed for next methods applied to the list

    # POSSIBLE TODO: keep dir and files in separated lists to avoid traversing them too many times, improving efficiency

    # Now erase any file:// that is inside dir://
    i = 0
    directories_list = []
    file_found = False
    for item in data_list:
        url_parts = urlsplit(item)
        if url_parts.scheme == "dir":
            if url_parts.path not in directories_list and not (
                any(
                    url_parts.path.startswith(dir_item) for dir_item in directories_list
                )
            ):
                directories_list.append(url_parts.path)
            i += 1
            continue
        else:
            file_found = True
            break
    if file_found:  # Not all are directories
        #  TODO Can this two loops be merged????
        data_list_copy = (
            data_list.copy()
        )  # So we can remove the element we are iterating upon

        for item in data_list_copy:
            # Check both dir:// and file:// references
            url_parts = urlsplit(item)
            if any(
                (url_parts.path != dir_path and url_parts.path.startswith(dir_path))
                for dir_path in directories_list
            ):
                # If the url dir:// does not finish with a slash, can add errors (e.g. /inputs vs /inputs.zip)
                print(
                    f"PROVENANCE | WARNING: Item {url_parts.path} removed as {yaml_term}, since it already belongs to a dataset"
                )
                data_list.remove(item)
    print(
        f"PROVENANCE | Manually added data assets as '{yaml_term}' ({len(data_entities_list)})"
    )

    return data_list


def add_file_to_crate(
    crate: ROCrate,
    source: Path,
    destination: str = None,
    task: Task = None,
    e_create_action: ContextEntity = None,
):
    """
    Adds a file to the crate at the specified destination folder.

    :param crate:    The COMPSs RO-Crate being generated.
    :param source:          Path object of the file being added.
    :param destination:     The name of the destination subfolder.
    :param task:            The task associated with the file (optional).
    :param e_create_action: The CreateAction instance associated with the file (optional).

    :return: The URL of the added file.
    """
    if not source.exists():
        print(
            f"PROVENANCE | WARNING: File {source} does not exist. Could not be added to crate."
        )
        return None

    file_properties = {
        "name": source.name,
        "sdDatePublished": iso_now(),
        "dateModified": dt.datetime.fromtimestamp(
            os.path.getmtime(source), timezone.utc
        )
        .replace(microsecond=0)
        .isoformat(),  # Schema.org
        "contentSize": os.path.getsize(source),
        "encodingFormat": "text/plain",
    }

    if task:
        channel = "Error" if source.name.endswith(".err") else "Output"
        file_properties["description"] = f"{channel} log file for Task {task.tid}"
    if e_create_action:
        file_properties["about"] = e_create_action

    return crate.add_file(
        source=source,
        validate_url=True,
        dest_path=os.path.join(destination, source.name),
        properties=file_properties,
    )


def add_master_out_and_err(compss_crate, create_action):
    # Add out and err logs in SLURM executions
    if job_id := os.getenv("SLURM_JOB_ID"):
        suffix = [".out", ".err"]
        msg = ["output", "error"]
        for f_suffix, f_msg in zip(suffix, msg):
            file_properties = {}
            file_properties["name"] = "compss-" + job_id + f_suffix
            if not os.path.isfile(file_properties["name"]):
                # E.g. regenerating the crate by hand inside a different job (salloc)
                print(
                    f"PROVENANCE | WARNING: SLURM job {f_msg} log {file_properties['name']} not found. "
                    f"Not added to the crate"
                )
                continue
            file_properties["contentSize"] = os.path.getsize(file_properties["name"])
            file_properties["description"] = (
                "COMPSs console standard " + f_msg + " log file"
            )
            file_properties["encodingFormat"] = "text/plain"
            file_properties["about"] = create_action
            compss_crate.add_file(file_properties["name"], properties=file_properties)


def add_trace_files(compss_crate, compss_wf_info, log_dir, create_action):
    # Add Paraver trace files if they have been generated in PRV_DIR/ folder
    if (
        "trace_persistence" in compss_wf_info
        and compss_wf_info["trace_persistence"] is True
    ):
        prv_persist = True
    else:
        prv_persist = False
    prv_dir = log_dir / "trace/"
    if prv_dir.exists() and prv_dir.is_dir():
        print(f"PROVENANCE | RO-Crate adding PARAVER trace files")
        if not prv_persist:
            print(
                f"PROVENANCE | RO-Crate PARAVER trace files persistence is False (trace_persistence)"
            )
        for file in prv_dir.iterdir():
            if file.is_file():
                file_properties = {}
                file_properties["name"] = file.name
                file_properties["contentSize"] = file.stat().st_size
                file_properties["description"] = "PARAVER trace files"
                file_properties["encodingFormat"] = "text/plain"
                file_properties["about"] = create_action
                if prv_persist:
                    crate_path = "trace/" + file.name
                    compss_crate.add_file(
                        source=file.resolve(),
                        dest_path=crate_path,
                        properties=file_properties,
                    )
                else:
                    file_url = "file://" + socket.gethostname() + str(file.resolve())
                    # print(f"TRACE URL:{file_url}")
                    # Paraver trace files are referenced via scp:// URLs for external access
                    # when trace_persistence is False, so they are not physically copied into the crate.
                    modified_url = write_external_url(file_url)
                    compss_crate.add_file(
                        source=modified_url,
                        fetch_remote=False,
                        validate_url=False,
                        properties=file_properties,
                    )
    elif prv_persist:
        print(
            f"PROVENANCE | WARNING: PARAVER trace files not found at COMPSs log dir, and trace_persistence is True at the Workflow Provenance YAML file"
        )

def add_all_log_files(compss_crate, compss_wf_info, log_dir, create_action):
    # Add all COMPSs runtime execution log files. They can be useful for debugging purposes
    if (
        "runtime_logs_persistence" not in compss_wf_info
        or compss_wf_info["runtime_logs_persistence"] is True
    ):
        log_persist = True
    else:
        log_persist = False
    if log_dir.exists() and log_dir.is_dir():
        print(f"PROVENANCE | RO-Crate adding all COMPSs log files. Persitence is {log_persist}")
        for root, dirs, files in os.walk(log_dir):
            # Avoid these directories
            dirs[:] = [d for d in dirs if d not in {"stats", "monitor", "jobs", "trace"}]
            for file in files:
                if file.endswith("compss_trace.tar.gz") or (file.endswith(".csv") and Path(root).name == "Log"):
                    # Do not add intermediate PARAVER trace generation files or profiling duplicated CSV files
                    continue
                path = Path(root) / file
                file_properties = {}
                file_properties["name"] = file
                file_properties["contentSize"] = path.stat().st_size
                file_properties["description"] = "COMPSs runtime log file"
                file_properties["encodingFormat"] = "text/plain"
                file_properties["about"] = create_action
                if log_persist:
                    crate_path = Path("runtime_logs") / path.relative_to(log_dir)
                    compss_crate.add_file(
                        source=path.resolve(),
                        dest_path=crate_path,
                        properties=file_properties,
                    )
                else:
                    # WorkflowHub cannot handle such a long list of files in the 'hasPart' term, thus, do 
                    # not add URL references to log files
                    continue
                    file_url = "file://" + socket.gethostname() + str(path.resolve())
                    modified_url = write_external_url(file_url)
                    compss_crate.add_file(
                        source=modified_url,
                        fetch_remote=False,
                        validate_url=False,
                        properties=file_properties,
                    )

def add_stats_and_plots(compss_crate, compss_wf_info, log_dir, create_action, main_entity):
    # Adding stats and profiling plots to RO-Crate
    if (
        "runtime_logs_persistence" not in compss_wf_info
        or compss_wf_info["runtime_logs_persistence"] is True
    ):
        log_persist = True
    else:
        log_persist = False
    resolved_main_entity = main_entity
    for entity in compss_crate.get_entities():
        if "ComputationalWorkflow" in entity.type:
            resolved_main_entity = entity

    stats_folder = log_dir / "stats/"
    if os.path.exists(stats_folder):
        # Nodes with profiling data (resource_profiling_<node>.csv). On single-node runs, the profiling tool writes
        # the plots of that node directly in plots/, so its name cannot be taken from the parent folder
        profiled_nodes = [
            f[len("resource_profiling_"):-len(".csv")]
            for f in os.listdir(stats_folder)
            if f.startswith("resource_profiling_") and f.endswith(".csv")
        ]
        for root, _, files in os.walk(stats_folder):
            for file in files:
                if file.endswith(".csv") or file.endswith(".svg"):
                    full_path = os.path.join(root, file)
                    if file.endswith(".svg"):
                        # Add always: aggregated plots (multi-node runs) or the plots of the only node (single-node runs)
                        relative_file_path = full_path.split("plots/")[1]
                        relative_path = "profiling/" + relative_file_path
                        # Determine metric and node_name based on path depth
                        path_parts = relative_path.split("/")
                        # path_parts example for aggregated or single-node plots: ["profiling", "cpu.svg"]
                        # path_parts example for per-node plots: ["profiling", "gs23r1b30-MASTER", "cpu.svg"]
                        metric = path_parts[-1].split(".")[0]   # filename without extension
                        if len(path_parts) == 2 and len(profiled_nodes) == 1:
                            node_name = profiled_nodes[0]       # single-node run
                        else:
                            node_name = path_parts[-2]          # parent directory name
                        description = get_description_plot(metric, node_name)
                        file_properties = {}
                        file_properties["@id"] = relative_path
                        file_properties["@type"] = ["File", "ImageObject"]
                        file_properties["name"] = path_parts[-1]
                        file_properties["description"] = description
                        file_properties["contentSize"] = os.stat(full_path).st_size
                        file_properties["encodingFormat"] = [
                            "image/svg+xml",
                            {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/91"},
                        ]
                        file_properties["about"] = resolved_main_entity
                        compss_crate.add_file(
                                source=full_path,
                                dest_path=relative_path,
                                properties=file_properties,
                            )
                    else:
                        # CSV files
                        relative_file_path = full_path.split("stats/")[1]
                        relative_path = "runtime_logs/stats/" + relative_file_path
                        file_properties = {}
                        file_properties["name"] = file
                        file_properties["contentSize"] = os.stat(full_path).st_size
                        file_properties["description"] = f"Resource Usage CSV file for node {file.split('resource_profiling_')[1].split('.')[0]}"
                        file_properties["encodingFormat"] = [
                            "text/csv",
                            {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/800"},
                        ]
                        file_properties["about"] = create_action
                        if log_persist:
                            compss_crate.add_file(
                                source=full_path,
                                dest_path=relative_path,
                                properties=file_properties,
                            )
                        else:
                            # WorkflowHub cannot handle such a long list of files in the 'hasPart' term, thus, do 
                            # not add URL references to log files
                            continue
                            file_url = "file://" + socket.gethostname() + str(Path(full_path).resolve())
                            modified_url = write_external_url(file_url)
                            compss_crate.add_file(
                                source=modified_url,
                                fetch_remote=False,
                                validate_url=False,
                                properties=file_properties,
                            )                        
    else:
        print("Stats folder does not exist")

    # if os.path.isdir(energy_path):
    #     try:
    #         print(f"PROVENANCE | RO-Crate adding energy data")
    #         # Add the resource usage to the ROCrate object
    #         for data_file in os.listdir(energy_path):
    #             if data_file.endswith("time.csv"):
    #                 info_list = []
    #                 filename = Path(energy_path, data_file)
    #                 node = data_file.split(".")[1]
    #                 get_energy_usage_for_node(filename, info_list, node)
    #
    #                 id_info_list = []
    #                 for info_properties in info_list:
    #                     info_id = info_properties["id"]
    #                     del info_properties["id"]
    #                     compss_crate.add(
    #                         ContextEntity(
    #                             compss_crate, info_id, properties=info_properties
    #                         )
    #                     )
    #                     id_info_list.append({"@id": info_id})
    #                     create_action_properties["resourceUsage"] = id_info_list
    #                     compss_crate.add(
    #                         ContextEntity(
    #                             compss_crate, node, properties=create_action_properties
    #                         )
    #                     )
    #     except ValueError:
    #         print(
    #             f"PROVENANCE | WARNING: Error during data retrieving in directory {energy_path}"
    #         )
    #         print("PROVENANCE | EAR not used")
