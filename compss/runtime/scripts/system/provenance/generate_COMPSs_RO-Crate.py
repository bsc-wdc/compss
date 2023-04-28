#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
import datetime

from rocrate.rocrate import ROCrate
from rocrate.model.person import Person
from rocrate.model.contextentity import ContextEntity
from rocrate.model.entity import Entity
from rocrate.model.file import File
from rocrate.utils import iso_now

from pathlib import Path
from urllib.parse import urlsplit

import yaml
import os
import uuid
import typing
import datetime as dt
import json

PROFILES_BASE = "https://w3id.org/ro/wfrun"
PROFILES_VERSION = "0.1"
WROC_PROFILE_VERSION = "1.0"


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
    else:
        return in_url  # No changes required


def root_entity(compss_crate: ROCrate, yaml_content: dict) -> typing.Tuple[dict, list]:
    """
    Generate the Root Entity in the RO-Crate generated for the COMPSs application

    :param compss_crate: The COMPSs RO-Crate being generated
    :param yaml_content: Content of the YAML file specified by the user

    :returns: 'COMPSs Workflow Information' and 'Authors' sections, as defined in the YAML
    """

    # Get Sections
    compss_wf_info = yaml_content["COMPSs Workflow Information"]
    authors_info_yaml = yaml_content["Authors"]  # Now a list of authors
    authors_info = []
    if isinstance(authors_info_yaml, list):
        authors_info = authors_info_yaml
    else:
        authors_info.append(authors_info_yaml)

    # COMPSs Workflow RO Crate generation
    # Root Entity
    compss_crate.name = compss_wf_info["name"]
    compss_crate.description = compss_wf_info["description"]
    compss_crate.license = compss_wf_info[
        "license"
    ]  # License details could be also added as a Contextual Entity

    authors_set = set()
    organisations_set = set()
    for author in authors_info:
        authors_set.add(author["orcid"])
        organisations_set.add(author["ror"])
        compss_crate.add(
            Person(
                compss_crate,
                author["orcid"],
                {
                    "name": author["name"],
                    "contactPoint": {"@id": "mailto:" + author["e-mail"]},
                    "affiliation": {"@id": author["ror"]},
                },
            )
        )
        compss_crate.add(
            ContextEntity(
                compss_crate,
                "mailto:" + author["e-mail"],
                {
                    "@type": "ContactPoint",
                    "contactType": "Author",
                    "email": author["e-mail"],
                    "identifier": author["e-mail"],
                    "url": author["orcid"],
                },
            )
        )
        compss_crate.add(
            ContextEntity(
                compss_crate,
                author["ror"],
                {"@type": "Organization", "name": author["organisation_name"]},
            )
        )
    author_list = list()
    for creator in authors_set:
        author_list.append({"@id": creator})
    compss_crate.creator = author_list
    org_list = list()
    for org in organisations_set:
        org_list.append({"@id": org})
    compss_crate.publisher = org_list
    # print(f"compss_wf_info at the beginning: {compss_wf_info}")
    return compss_wf_info, author_list


def get_main_entities(wf_info: dict) -> typing.Tuple[str, str, str]:
    """
    Get COMPSs version and mainEntity from dataprovenance.log first lines
    3 First lines expected format: compss_version_number\n main_entity\n output_profile_file\n
    Next lines are for "accessed files" and "direction"
    mainEntity can be directly obtained for Python, or defined by the user in the YAML (sources_main_file)

    :param wf_info: YAML dict to extract info form the application, as specified by the user

    :returns: COMPSs version, main COMPSs file name, COMPSs profile file name
    """

    # Build the whole source files list in list_of_sources, and get a backup main entity, in case we can't find one
    # automatically. The mainEntity must be an existing file, otherwise the RO-Crate won't have a ComputationalWorkflow
    list_of_sources = []
    sources_list = []
    # Should contain absolute paths, for correct comparison (two files in different directories
    # could be named the same)
    main_entity = None
    backup_main_entity = None
    if "files" in wf_info:
        files_list = []
        if isinstance(wf_info["files"], list):
            files_list = wf_info["files"]
        else:
            files_list.append(wf_info["files"])
        for file in files_list:
            path_file = Path(file).expanduser()
            resolved_file = str(path_file.resolve())
            if path_file.exists():
                list_of_sources.append(resolved_file)
        # list_of_sources = wf_info["files"].copy()
        if list_of_sources:  # List of files not empty
            backup_main_entity = list_of_sources[0]
            # Assign first file name as mainEntity
    if "sources_dir" in wf_info:
        if isinstance(wf_info["sources_dir"], list):
            sources_list = wf_info["sources_dir"]
        else:
            sources_list.append(wf_info["sources_dir"])
        for source in sources_list:
            path_sources = Path(source).expanduser()
            if not path_sources.exists():
                print(
                    f"PROVENANCE | WARNING: Specified path in ro-crate-info.yaml 'sources_dir' does not exist ({path_sources})"
                )
                continue
            resolved_sources = str(path_sources.resolve())
            # print(f"resolved_sources is: {resolved_sources}")
            for root, dirs, files in os.walk(resolved_sources, topdown=True):
                if "__pycache__" in root:
                    continue  # We skip __pycache__ subdirectories
                for f_name in files:
                    # print(f"PROVENANCE DEBUG | ADDING FILE to list_of_sources: {f_name}. root is: {root}")
                    full_name = os.path.join(root, f_name)
                    list_of_sources.append(full_name)
                    if backup_main_entity is None and Path(f_name).suffix in {
                        ".py",
                        ".java",
                        ".jar",
                        ".class",
                    }:
                        backup_main_entity = full_name
                        # print(
                        #     f"PROVENANCE DEBUG | FOUND SOURCE FILE AS BACKUP MAIN: {backup_main_entity}"
                        # )

    # Can't get backup_main_entity from sources_main_file, because we do not know if it really exists
    if backup_main_entity is None:
        print(
            f"PROVENANCE | ERROR: Unable to find application source files. Please, review your "
            f"ro_crate_info.yaml definition ('sources_dir', and 'files' terms)"
        )
        raise FileNotFoundError
    # print(f"PROVENANCE DEBUG | backup_main_entity is: {backup_main_entity}")

    with open(dp_log, "r") as f:
        compss_v = next(f).rstrip()  # First line, COMPSs version number
        second_line = next(f).rstrip()
        # Second, main_entity. Use better rstrip, just in case there is no '\n'
        if second_line.endswith(".py"):
            # Python. Line contains only the file name, need to locate it
            detected_app = second_line
        else:  # Java app. Need to fix filename first
            # Translate identified main entity matmul.files.Matmul to a comparable path
            me_sub_path = second_line.replace(".", "/")
            detected_app = me_sub_path + ".java"
        # print(f"PROVENANCE DEBUG | Detected app is: {detected_app}")

        for file in list_of_sources:  # Try to find the identified mainEntity
            if file.endswith(detected_app):
                # print(
                #     f"PROVENANCE DEBUG | IDENTIFIED MAIN ENTITY FOUND IN LIST OF FILES: {file}"
                # )
                main_entity = file
                break
        # main_entity has a value if mainEntity has been automatically detected
        if "sources_dir" in wf_info and "sources_main_file" in wf_info:
            # Check what the user has defined
            # if sources_main_file is an absolute path, the join has no effect
            found = False
            for source in sources_list:  # Created before
                path_sources = Path(source).expanduser()
                if not path_sources.exists():
                    continue
                resolved_sources = str(path_sources.resolve())
                resolved_sources_main_file = os.path.join(
                    resolved_sources, wf_info["sources_main_file"]
                )
                if any(file == resolved_sources_main_file for file in list_of_sources):
                    # The file exists
                    # print(
                    #     f"PROVENANCE DEBUG | The file defined at sources_main_file exists: "
                    #     f" {resolved_sources_main_file}"
                    # )
                    if resolved_sources_main_file != main_entity:
                        print(
                            f"PROVENANCE | WARNING: The file defined at sources_main_file "
                            f"({resolved_sources_main_file}) in ro-crate-info.yaml does not match with the "
                            f"automatically identified mainEntity ({main_entity})"
                        )
                    # else: the user has defined exactly the file we found
                    # In both cases: set file defined by user
                    main_entity = resolved_sources_main_file
                    # Can't use Path, file may not be in cwd
                    found = True
                    break
            if not found:
                print(
                    f"PROVENANCE | WARNING: the defined 'sources_main_file' ({wf_info['sources_main_file']}) does "
                    f"not exist in the defined 'sources_dir' ({wf_info['sources_dir']}). Check your ro-crate-info.yaml."
                )
                # If we identified the mainEntity automatically, we select it when the one defined
                # by the user is not found

        if main_entity is None:
            # When neither identified, nor defined by user: get backup
            main_entity = backup_main_entity
            print(
                f"PROVENANCE | WARNING: the detected mainEntity {detected_app} does not exist in the list "
                f"of application files provided in ro-crate-info.yaml. Setting {main_entity} as mainEntity"
            )

        third_line = next(f).rstrip()
        out_profile_fn = Path(third_line)

    print(
        f"PROVENANCE | COMPSs version: {compss_v}, main_entity is: {main_entity}, out_profile is: {out_profile_fn.name}"
    )

    return compss_v, main_entity, out_profile_fn.name


def process_accessed_files() -> typing.Tuple[list, list]:
    """
    Process all the files the COMPSs workflow has accessed. They will be the overall inputs needed and outputs
    generated of the whole workflow.
    - If a task that is an INPUT, was previously an OUTPUT, it means it is an intermediate file, therefore we discard it
    - Works fine with COLLECTION_FILE_IN, COLLECTION_FILE_OUT and COLLECTION_FILE_INOUT

    :returns: List of Inputs and Outputs of the COMPSs workflow
    """

    part_time = time.time()

    inputs = set()
    outputs = set()

    with open(dp_log, "r") as f:
        for line in f:
            file_record = line.rstrip().split(" ")
            if len(file_record) == 2:
                if (
                    file_record[1] == "IN" or file_record[1] == "IN_DELETE"
                ):  # Can we have an IN_DELETE that was not previously an OUTPUT?
                    if (
                        file_record[0] not in outputs
                    ):  # A true INPUT, not an intermediate file
                        inputs.add(file_record[0])
                    #  Else, it is an intermediate file, not a true INPUT or OUTPUT. Not adding it as an input may
                    # be enough in most cases, since removing it as an output may be a bit radical
                    #     outputs.remove(file_record[0])
                elif file_record[1] == "OUT":
                    outputs.add(file_record[0])
                else:  # INOUT, COMMUTATIVE, CONCURRENT
                    if (
                        file_record[0] not in outputs
                    ):  # Not previously generated by another task (even a task using that same file), a true INPUT
                        inputs.add(file_record[0])
                    # else, we can't know for sure if it is an intermediate file, previous call using the INOUT may
                    # have inserted it at outputs, thus don't remove it from outputs
                    outputs.add(file_record[0])
            # else dismiss the line

    l_ins = list(inputs)
    l_ins.sort()
    l_outs = list(outputs)
    l_outs.sort()

    print(f"PROVENANCE | INPUTS({len(l_ins)})")
    print(f"PROVENANCE | OUTPUTS({len(l_outs)})")
    print(
        f"PROVENANCE | RO-CRATE data_provenance.log processing TIME (process_accessed_files): "
        f"{time.time() - part_time} s"
    )

    return l_ins, l_outs


def add_file_to_crate(
    compss_crate: ROCrate,
    file_name: str,
    compss_ver: str,
    main_entity: str,
    out_profile: str,
    in_sources_dir: str,
) -> str:
    """
    Get details of a file, and add it physically to the Crate. The file will be an application source file, so,
    the destination directory should be 'application_sources/'

    :param compss_crate: The COMPSs RO-Crate being generated
    :param file_name: File to be added physically to the Crate, full path resolved
    :param compss_ver: COMPSs version number
    :param main_entity: COMPSs file with the main code, full path resolved
    :param out_profile: COMPSs application profile output
    :param in_sources_dir: Path to the defined sources_dir. May be passed empty

    :returns: Path where the file has been stored in the crate
    """

    file_path = Path(file_name)
    file_properties = dict()
    file_properties["name"] = file_path.name
    file_properties["contentSize"] = os.path.getsize(file_name)

    # main_entity has its absolute path, as well as file_name
    if file_name == main_entity:
        file_properties["description"] = "Main file of the COMPSs workflow source files"
        if file_path.suffix == ".jar":
            file_properties["encodingFormat"] = (
                [
                    "application/java-archive",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/412"},
                ],
            )
            # Add JAR as ContextEntity
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/412",
                    {"@type": "WebSite", "name": "Java Archive Format"},
                )
            )
        elif file_path.suffix == ".class":
            file_properties["encodingFormat"] = (
                [
                    "application/java",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/415"},
                ],
            )
            # Add CLASS as ContextEntity
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/415",
                    {"@type": "WebSite", "name": "Java Compiled Object Code"},
                )
            )
        else:  # .py, .java, .c, .cc, .cpp
            file_properties["encodingFormat"] = "text/plain"
        if complete_graph.exists():
            file_properties["image"] = {
                "@id": "complete_graph.svg"
            }  # Name as generated

        # input and output properties not added to the workflow, since we do not comply with BioSchemas
        # (i.e. no FormalParameters are defined)

    else:
        # Any other extra file needed
        file_properties["description"] = "Auxiliary File"
        if file_path.suffix == ".py" or file_path.suffix == ".java":
            file_properties["encodingFormat"] = "text/plain"
            file_properties["@type"] = ["File", "SoftwareSourceCode"]
        elif file_path.suffix == ".json":
            file_properties["encodingFormat"] = [
                "application/json",
                {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/817"},
            ]
        elif file_path.suffix == ".pdf":
            file_properties["encodingFormat"] = (
                [
                    "application/pdf",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/276"},
                ],
            )
        elif file_path.suffix == ".svg":
            file_properties["encodingFormat"] = (
                [
                    "image/svg+xml",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/92"},
                ],
            )
        elif file_path.suffix == ".jar":
            file_properties["encodingFormat"] = (
                [
                    "application/java-archive",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/412"},
                ],
            )
            # Add JAR as ContextEntity
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/412",
                    {"@type": "WebSite", "name": "Java Archive Format"},
                )
            )
        elif file_path.suffix == ".class":
            file_properties["encodingFormat"] = (
                [
                    "Java .class",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/415"},
                ],
            )
            # Add CLASS as ContextEntity
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/415",
                    {"@type": "WebSite", "name": "Java Compiled Object Code"},
                )
            )

    # Build correct dest_path. If the file belongs to sources_dir, need to remove all "sources_dir" from file_name,
    # respecting the sub_dir structure.
    # If the file is defined individually, put in the root of application_sources

    if in_sources_dir:
        # /home/bsc/src/file.py must be translated to application_sources/src/file.py,
        # but in_sources_dir is /home/bsc/src
        new_root = str(Path(in_sources_dir).parents[0])
        final_name = file_name[len(new_root) + 1 :]
        path_in_crate = "application_sources/" + final_name
    else:
        path_in_crate = "application_sources/" + file_path.name

    if file_name != main_entity:
        # print(f"PROVENANCE DEBUG | Adding auxiliary source file: {file_name}")
        compss_crate.add_file(
            source=file_name, dest_path=path_in_crate, properties=file_properties
        )
    else:
        # We get lang_version from dataprovenance.log
        # print(f"PROVENANCE DEBUG | Adding main source file: {file_path.name}, file_name: {file_name}")
        compss_crate.add_workflow(
            source=file_name,
            dest_path=path_in_crate,
            main=True,
            lang="COMPSs",
            lang_version=compss_ver,
            properties=file_properties,
            gen_cwl=False,
        )

        # complete_graph.svg
        if complete_graph.exists():
            file_properties = dict()
            file_properties["name"] = "complete_graph.svg"
            file_properties["contentSize"] = complete_graph.stat().st_size
            file_properties["@type"] = ["File", "ImageObject", "WorkflowSketch"]
            file_properties[
                "description"
            ] = "The graph diagram of the workflow, automatically generated by COMPSs runtime"
            # file_properties["encodingFormat"] = (
            #     [
            #         "application/pdf",
            #         {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/276"},
            #     ],
            # )
            file_properties["encodingFormat"] = (
                [
                    "image/svg+xml",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/92"},
                ],
            )
            file_properties["about"] = {
                "@id": path_in_crate
            }  # Must be main_entity_location, not main_entity alone
            # Add PDF as ContextEntity
            # compss_crate.add(
            #     ContextEntity(
            #         compss_crate,
            #         "https://www.nationalarchives.gov.uk/PRONOM/fmt/276",
            #         {
            #             "@type": "WebSite",
            #             "name": "Acrobat PDF 1.7 - Portable Document Format",
            #         },
            #     )
            # )
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    "https://www.nationalarchives.gov.uk/PRONOM/fmt/92",
                    {
                        "@type": "WebSite",
                        "name": "Scalable Vector Graphics",
                    },
                )
            )
            compss_crate.add_file(complete_graph, properties=file_properties)
        else:
            print(
                f"PROVENANCE | WARNING: complete_graph.svg file not found. "
                f"Provenance will be generated without image property"
            )

        # out_profile
        if os.path.exists(out_profile):
            file_properties = dict()
            file_properties["name"] = out_profile
            file_properties["contentSize"] = os.path.getsize(out_profile)
            file_properties["description"] = "COMPSs application Tasks profile"
            file_properties["encodingFormat"] = [
                "application/json",
                {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/817"},
            ]

            # Fix COMPSs crappy format of JSON files
            with open(out_profile) as op_file:
                op_json = json.load(op_file)
            with open(out_profile, "w") as op_file:
                json.dump(op_json, op_file, indent=1)

            # Add JSON as ContextEntity
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    "https://www.nationalarchives.gov.uk/PRONOM/fmt/817",
                    {"@type": "WebSite", "name": "JSON Data Interchange Format"},
                )
            )
            compss_crate.add_file(out_profile, properties=file_properties)
        else:
            print(
                f"PROVENANCE | WARNING: COMPSs application profile has not been generated. \
                  Make sure you use runcompss with --output_profile=file_name"
                f"Provenance will be generated without profiling information"
            )

        # compss_command_line_arguments.txt
        file_properties = dict()
        file_properties["name"] = "compss_command_line_arguments.txt"
        file_properties["contentSize"] = os.path.getsize(
            "compss_command_line_arguments.txt"
        )
        file_properties[
            "description"
        ] = "COMPSs command line execution command, including parameters passed"
        file_properties["encodingFormat"] = "text/plain"
        compss_crate.add_file(
            "compss_command_line_arguments.txt", properties=file_properties
        )
        return ""

    return path_in_crate


def add_application_source_files(
    compss_crate: ROCrate,
    compss_wf_info: dict,
    compss_ver: str,
    main_entity: str,
    out_profile: str,
) -> None:
    """
    Add all application source files as part of the crate. This means, to include them physically in the resulting
    bundle

    :param compss_crate: The COMPSs RO-Crate being generated
    :param compss_wf_info: YAML dict to extract info form the application, as specified by the user
    :param compss_ver: COMPSs version number
    :param main_entity: COMPSs file with the main code, full path resolved
    :param out_profile: COMPSs application profile output file

    :returns: None
    """

    part_time = time.time()
    added_files = []
    crate_paths = []
    if "sources_dir" in compss_wf_info:
        # Optional, the user specifies a directory with all sources
        sources_list = []
        if isinstance(compss_wf_info["sources_dir"], list):
            sources_list = compss_wf_info["sources_dir"]
        else:
            sources_list.append(compss_wf_info["sources_dir"])
        for source in sources_list:
            path_sources = Path(source).expanduser()
            if not path_sources.exists():
                continue
            resolved_sources = str(path_sources.resolve())
            for root, dirs, files in os.walk(resolved_sources, topdown=True):
                if "__pycache__" in root:
                    continue  # We skip __pycache__ subdirectories
                for f_name in files:
                    resolved_file = os.path.join(root, f_name)
                    crate_paths.append(
                        add_file_to_crate(
                            compss_crate,
                            resolved_file,
                            compss_ver,
                            main_entity,
                            out_profile,
                            resolved_sources,
                        )
                    )
                    added_files.append(resolved_file)

    if "files" in compss_wf_info:
        files_list = []
        if isinstance(compss_wf_info["files"], list):
            files_list = compss_wf_info["files"]
        else:
            files_list.append(compss_wf_info["files"])
        for file in files_list:
            path_file = Path(file).expanduser()
            resolved_file = str(path_file.resolve())
            if not path_file.exists():
                print(
                    f"PROVENANCE | WARNING: A file defined as 'files' in ro-crate-info.yaml does not exist "
                    f"({resolved_file})"
                )
                continue
            if resolved_file not in added_files:
                crate_paths.append(
                    add_file_to_crate(
                        compss_crate,
                        resolved_file,
                        compss_ver,
                        main_entity,
                        out_profile,
                        "",
                    )
                )
                added_files.append(resolved_file)
            else:
                print(
                    f"PROVENANCE | WARNING: A file addition was attempted twice in 'files' and 'sources_dir': "
                    f"{resolved_file}"
                )

    # Add auxiliary files as hasPart to the ComputationalWorkflow main file
    # Not working well when an application has several versions (ex: Java matmul files, objects, arrays)
    # for e in compss_crate.data_entities:
    #     if 'ComputationalWorkflow' in e.type:
    #         for file in crate_paths:
    #             if file is not "":
    #                 e.append_to("hasPart", {"@id": file})

    print(f"PROVENANCE | Number of source files detected: {len(added_files)}")
    # print(f"PROVENANCE DEBUG | Source files detected: {added_files}")

    print(
        f"PROVENANCE | RO-CRATE adding physical files TIME (add_file_to_crate): {time.time() - part_time} s"
    )


def add_dataset_file_to_crate(
    compss_crate: ROCrate, in_url: str, persist: bool, common_paths: list
) -> str:
    """
    Add the file (or a reference to it) belonging to the dataset of the application (both input or output)
    When adding local files that we don't want to be physically in the Crate, they must be added with a file:// URI
    CAUTION: If the file has been already added (e.g. for INOUT files) add_file won't succeed in adding a second entity
    with the same name

    :param compss_crate: The COMPSs RO-Crate being generated
    :param in_url: File added as input or output
    :param persist: True to attach the file to the crate, False otherwise
    :param common_paths: List of identified common paths among all dataset files

    :returns: The original url if persist is false, the crate_path if persist is true
    """

    # method_time = time.time()
    url_parts = urlsplit(in_url)
    final_item_name = os.path.basename(in_url)
    file_properties = {
        "name": final_item_name,
        "sdDatePublished": iso_now(),
        "dateModified": dt.datetime.utcfromtimestamp(os.path.getmtime(url_parts.path))
        .replace(microsecond=0)
        .isoformat(),  # Schema.org
    }  # Register when the Data Entity was last accessible

    if url_parts.scheme == "file":  # Dealing with a local file
        file_properties["contentSize"] = os.path.getsize(url_parts.path)
        crate_path = ""
        # add_file_time = time.time()
        if persist:  # Remove scheme so it is added as a regular file
            for i, item in enumerate(common_paths):  # All files must have a match
                if url_parts.path.startswith(item):
                    crate_path = (
                        "dataset/" + "folder_" + str(i) + url_parts.path[len(item) :]
                    )  # Slice out the common part of the path
                    break
            print(f"ADDING {url_parts.path} as {crate_path}")
            compss_crate.add_file(
                source=url_parts.path, dest_path=crate_path, properties=file_properties
            )
            return crate_path
        else:
            compss_crate.add_file(
                in_url,
                fetch_remote=False,
                validate_url=False,  # True fails at MN4 when file URI points to a node hostname (only localhost works)
                properties=file_properties,
            )
            return in_url
        # add_file_time = time.time() - add_file_time

    # DIRECTORIES ENCARA FALTA IMPLEMENTAR I TESTING

    elif url_parts.scheme == "dir":  # DIRECTORY parameter
        # For directories, describe all files inside the directory
        has_part_list = []
        for root, dirs, files in os.walk(
            url_parts.path, topdown=True
        ):  # Ignore references to sub-directories (they are not a specific in or out of the workflow),
            # but not their files
            dirs.sort()
            files.sort()
            for f_name in files:
                listed_file = os.path.join(root, f_name)
                dir_f_url = "file://" + url_parts.netloc + listed_file
                has_part_list.append({"@id": dir_f_url})
                dir_f_properties = {
                    "name": f_name,
                    "sdDatePublished": iso_now(),  # Register when the Data Entity was last accessible
                    "dateModified": dt.datetime.utcfromtimestamp(
                        os.path.getmtime(url_parts.path)
                    )
                    .replace(microsecond=0)
                    .isoformat(),
                    # Schema.org
                    "contentSize": os.path.getsize(listed_file),
                }
                compss_crate.add_file(
                    dir_f_url,
                    fetch_remote=False,
                    validate_url=False,
                    # True fails at MN4 when file URI points to a node hostname (only localhost works)
                    properties=dir_f_properties,
                )
        file_properties["hasPart"] = has_part_list
        compss_crate.add_dataset(
            fix_dir_url(in_url), properties=file_properties
        )  # fetch_remote and validate_url false by default. add_dataset also ensures the URL ends with '/'

    else:  # Remote file, currently not supported in COMPSs. validate_url already adds contentSize and encodingFormat
        # from the remote file
        compss_crate.add_file(in_url, validate_url=True, properties=file_properties)

    # print(f"Method vs add_file TIME: {time.time() - method_time} vs {add_file_time}")

    return in_url


def wrroc_create_action(
    compss_crate: ROCrate,
    main_entity: str,
    author_list: list,
    ins: list,
    outs: list,
    yaml_content: dict,
) -> str:
    """
    Add a CreateAction term to the ROCrate to make it compliant with WRROC.  RO-Crate WorkflowRun Level 2 profile,
    aka. Workflow Run Crate.

    :param compss_crate: The COMPSs RO-Crate being generated
    :param main_entity: The name of the source file that contains the COMPSs application main() method
    :param author_list: List of authors as described in the YAML
    :param ins: List of input files of the workflow
    :param outs: List of output files of the workflow
    :param yaml_content: Content of the YAML file specified by the user

    :returns: UUID generated for this run
    """

    # Compliance with RO-Crate WorkflowRun Level 2 profile, aka. Workflow Run Crate
    import socket

    host_name = os.getenv(
        "SLURM_CLUSTER_NAME"
    )  # marenostrum4, nord3, ... BSC_MACHINE would also work
    if host_name is None:
        host_name = socket.gethostname()
    job_id = os.getenv("SLURM_JOB_ID")

    main_entity_pathobj = Path(main_entity)

    run_uuid = str(uuid.uuid4())

    if job_id is None:
        name_property = (
            "COMPSs " + main_entity_pathobj.name + " execution at " + host_name
        )
        userportal_url = None
        create_action_id = "#COMPSs_Workflow_Run_Crate_" + host_name + "_" + run_uuid
    else:
        name_property = (
            "COMPSs "
            + main_entity_pathobj.name
            + " execution at "
            + host_name
            + " with JOB_ID "
            + job_id
        )
        userportal_url = "https://userportal.bsc.es/"  # job_id cannot be added, does not match the one in userportal
        create_action_id = (
            "#COMPSs_Workflow_Run_Crate_" + host_name + "_SLURM_JOB_ID_" + job_id
        )
    compss_crate.root_dataset["mentions"] = {"@id": create_action_id}

    # OSTYPE, HOSTTYPE, HOSTNAME defined by bash and not inherited. Changed to "uname -a"
    import subprocess

    uname = subprocess.run(["uname", "-a"], stdout=subprocess.PIPE)
    uname_out = uname.stdout.decode("utf-8")[:-1]  # Remove final '\n'

    # SLURM interesting variables: SLURM_JOB_NAME, SLURM_JOB_QOS, SLURM_JOB_USER, SLURM_SUBMIT_DIR, SLURM_NNODES or
    # SLURM_JOB_NUM_NODES, SLURM_JOB_CPUS_PER_NODE, SLURM_MEM_PER_CPU, SLURM_JOB_NODELIST or SLURM_NODELIST.
    slurm_env_vars = ""
    for name, value in os.environ.items():
        if (
            name.startswith(("SLURM_JOB", "SLURM_MEM", "SLURM_SUBMIT", "COMPSS"))
            and name != "SLURM_JOBID"
        ):
            slurm_env_vars += "{0}={1} ".format(name, value)

    if len(slurm_env_vars) > 0:
        description_property = (
            uname_out + " " + slurm_env_vars[:-1]
        )  # Remove blank space
    else:
        description_property = uname_out

    resolved_main_entity = main_entity
    for e in compss_crate.get_entities():
        if "ComputationalWorkflow" in e.type:
            resolved_main_entity = e.id

    # Register user submitting the workflow
    if "Submitter" in yaml_content:
        compss_crate.add(
            Person(
                compss_crate,
                yaml_content["Submitter"]["orcid"],
                {
                    "name": yaml_content["Submitter"]["name"],
                    "contactPoint": {
                        "@id": "mailto:" + yaml_content["Submitter"]["e-mail"]
                    },
                    "affiliation": {"@id": yaml_content["Submitter"]["ror"]},
                },
            )
        )
        compss_crate.add(
            ContextEntity(
                compss_crate,
                "mailto:" + yaml_content["Submitter"]["e-mail"],
                {
                    "@type": "ContactPoint",
                    "contactType": "Author",
                    "email": yaml_content["Submitter"]["e-mail"],
                    "identifier": yaml_content["Submitter"]["e-mail"],
                    "url": yaml_content["Submitter"]["orcid"],
                },
            )
        )
        compss_crate.add(
            ContextEntity(
                compss_crate,
                yaml_content["Submitter"]["ror"],
                {
                    "@type": "Organization",
                    "name": yaml_content["Submitter"]["organisation_name"],
                },
            )
        )
        submitter = {"@id": yaml_content["Submitter"]["orcid"]}
    else:  # Choose first author, to avoid leaving it empty. May be true most of the times
        submitter = author_list[0]
        print(
            f"PROVENANCE | WARNING: 'Submitter' not specified in ro-crate-info.yaml. First author selected by default."
        )

    create_action_properties = {
        "@type": "CreateAction",
        "instrument": {"@id": resolved_main_entity},  # Resolved path of the main file
        "actionStatus": {"@id": "http://schema.org/CompletedActionStatus"},
        "agent": submitter,
        "endTime": iso_now(),  # Get current time
        "name": name_property,
        "description": description_property,
    }

    create_action = compss_crate.add(
        ContextEntity(compss_crate, create_action_id, create_action_properties)
    )  # id can be something fancy for MN4, otherwise, whatever
    create_action.properties()

    # "subjectOf": {"@id": userportal_url}
    if userportal_url is not None:
        create_action.append_to("subjectOf", userportal_url)

    # "object": [{"@id":}],  # List of inputs
    # "result": [{"@id":}]  # List of outputs
    for item in ins:
        create_action.append_to("object", {"@id": fix_dir_url(item)})
    for item in outs:
        create_action.append_to("result", {"@id": fix_dir_url(item)})
    create_action.append_to("result", {"@id": "./"})  # The generated RO-Crate

    return run_uuid


def get_common_paths(url_list: list) -> list:
    """
    Find the common paths in the list of files passed.

    :param url_list: List of file URLs as generated by COMPSs runtime

    :returns: List of identified common paths among the URLs
    """

    list_common_paths = []  # Create common_paths list, with counter of occurrences
    if not url_list:  # Empty list
        return list_common_paths

    url_parts = urlsplit(url_list[0])
    common_path = url_parts.path  # Need to remove schema and hostname from reference
    for i, item in enumerate(url_list):
        # url_list is a sorted list, important for this algorithm to work
        # if item and common_path have a common path, store that common path in common_path and continue, until the
        # shortest common path different than 0 has been identified
        # https://docs.python.org/3/library/os.path.html  # os.path.commonpath

        # First or last element, ignore. Directories, ignore
        if i == 0 or i == len(url_list):
            continue
        url_parts = urlsplit(item)
        if url_parts.scheme == "dir":
            print(f"SKIPPING DIRECTORY")
            continue
        # Remove schema and hostname
        tmp = os.path.commonpath([url_parts.path, common_path])
        if tmp != "/":  # String not empty, they have a common path
            # print(f"Searching. Previous common path is: {common_path}. tmp: {tmp}")
            common_path = tmp
        else:  # if they don't, we are in a new path, so, store the old in list_common_paths, and assign item to common_path
            print(f"New root to search common_path: {url_parts.path}")
            if common_path not in list_common_paths:
                list_common_paths.append(common_path)
            common_path = url_parts.path
    if common_path not in list_common_paths:
        list_common_paths.append(common_path)

    return list_common_paths


def main():
    yaml_template = (
        "COMPSs Workflow Information:\n"
        "  name: Name of your COMPSs application\n"
        "  description: Detailed description of your COMPSs application\n"
        "  license: Apache-2.0\n"
        "    # URL preferred, but these strings are accepted: https://about.workflowhub.eu/Workflow-RO-Crate/#supported-licenses\n"
        "  sources_dir: [path_to/dir_1, path_to/dir_2]\n"
        "    # Optional: List of directories containing the application source files. Relative or absolute paths can be used\n"
        "  sources_main_file: my_main_file.py\n"
        "    # Optional: Name of the main file of the application, located in one of the sources_dir.\n"
        "    # Relative paths from a sources_dir entry, or absolute paths can be used\n"
        "  files: [main_file.py, aux_file_1.py, aux_file_2.py]\n"
        "    # List of application files. Relative or absolute paths can be used\n"
        #        "  data_persistence: False\n"
        #        "    # True to include all input and output files of the application in the resulting crate"
        "\n"
        "Authors:\n"
        "  - name: Author_1 Name\n"
        "    e-mail: author_1@email.com\n"
        "    orcid: https://orcid.org/XXXX-XXXX-XXXX-XXXX\n"
        "    organisation_name: Institution_1 name\n"
        "    ror: https://ror.org/XXXXXXXXX\n"
        "      # Find them in ror.org\n"
        "  - name: Author_2 Name\n"
        "    e-mail: author2@email.com\n"
        "    orcid: https://orcid.org/YYYY-YYYY-YYYY-YYYY\n"
        "    organisation_name: Institution_2 name\n"
        "    ror: https://ror.org/YYYYYYYYY\n"
        "      # Find them in ror.org\n"
        "\n"
        "Submitter:\n"
        "  name: Name\n"
        "  e-mail: submitter@email.com\n"
        "  orcid: https://orcid.org/XXXX-XXXX-XXXX-XXXX\n"
        "  organisation_name: Submitter Institution name\n"
        "  ror: https://ror.org/XXXXXXXXX\n"
        "    # Find them in ror.org\n"
    )

    compss_crate = ROCrate()

    # First, read values defined by user from ro-crate-info.yaml
    try:
        with open(info_yaml, "r", encoding="utf-8") as fp:
            try:
                yaml_content = yaml.safe_load(fp)
            except yaml.YAMLError as exc:
                print(exc)
                raise exc
    except IOError:
        with open("ro-crate-info_TEMPLATE.yaml", "w", encoding="utf-8") as ft:
            ft.write(yaml_template)
            print(
                f"PROVENANCE | ERROR: YAML file ro-crate-info.yaml not found in your working directory. A template"
                f" has been generated in file ro-crate-info_TEMPLATE.yaml"
            )
        raise

    # Generate Root entity section in the RO-Crate
    compss_wf_info, author_list = root_entity(compss_crate, yaml_content)

    # Get mainEntity from COMPSs runtime log dataprovenance.log
    compss_ver, main_entity, out_profile = get_main_entities(compss_wf_info)

    # Process set of accessed files, as reported by COMPSs runtime.
    # This must be done before adding the Workflow to the RO-Crate
    ins, outs = process_accessed_files()

    # Add application source files to the RO-Crate, that will also be physically in the crate
    add_application_source_files(
        compss_crate, compss_wf_info, compss_ver, main_entity, out_profile
    )

    # Add in and out files, not to be physically copied in the Crate by default
    # Merge lists to avoid duplication when detecting common_paths

    ins_and_outs = ins.copy() + outs.copy()
    ins_and_outs.sort()  # Put together shared paths between ins an outs

    list_common_paths = []
    part_time = time.time()
    if (
        "data_persistence" in compss_wf_info
        and compss_wf_info["data_persistence"] == True
    ):
        persistence = True
        list_common_paths = get_common_paths(ins_and_outs)
        print(f"List of common paths INS and OUTS: {list_common_paths}")
    else:
        persistence = False

    fixed_ins = []  # ins are file://host/path/file, fixed_ins are crate_path/file
    for item in ins:
        fixed_ins.append(
            add_dataset_file_to_crate(
                compss_crate, item, persistence, list_common_paths
            )
        )
    print(
        f"PROVENANCE | RO-CRATE adding input files' references TIME (add_dataset_file_to_crate): "
        f"{time.time() - part_time} s"
    )

    part_time = time.time()

    fixed_outs = []
    for item in outs:
        fixed_outs.append(
            add_dataset_file_to_crate(
                compss_crate, item, persistence, list_common_paths
            )
        )
    print(
        f"PROVENANCE | RO-CRATE adding output files' references TIME (add_dataset_file_to_crate): "
        f"{time.time() - part_time} s"
    )

    # print(f"FIXED_INS: {fixed_ins}")
    # print(f"FIXED_OUTS: {fixed_outs}")
    # Register execution details using WRROC profile
    run_uuid = wrroc_create_action(
        compss_crate, main_entity, author_list, fixed_ins, fixed_outs, yaml_content
    )  # Compliance with RO-Crate WorkflowRun Level 2 profile, aka. Workflow Run Crate

    # ro-crate-py does not deal with profiles
    # compss_crate.metadata.append_to(
    #     "conformsTo", {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"}
    # )

    #  Code from runcrate https://github.com/ResearchObject/runcrate/blob/411c70da556b60ee2373fea0928c91eb78dd9789/src/runcrate/convert.py#L270
    profiles = []
    for p in "process", "workflow":
        id_ = f"{PROFILES_BASE}/{p}/{PROFILES_VERSION}"
        profiles.append(
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    id_,
                    properties={
                        "@type": "CreativeWork",
                        "name": f"{p.title()} Run Crate",
                        "version": PROFILES_VERSION,
                    },
                )
            )
        )
    # FIXME: in the future, this could go out of sync with the wroc
    # profile added by ro-crate-py to the metadata descriptor
    wroc_profile_id = (
        f"https://w3id.org/workflowhub/workflow-ro-crate/{WROC_PROFILE_VERSION}"
    )
    profiles.append(
        compss_crate.add(
            ContextEntity(
                compss_crate,
                wroc_profile_id,
                properties={
                    "@type": "CreativeWork",
                    "name": "Workflow RO-Crate",
                    "version": WROC_PROFILE_VERSION,
                },
            )
        )
    )
    compss_crate.root_dataset["conformsTo"] = profiles

    # Debug
    # for e in compss_crate.get_entities():
    #    print(e.id, e.type)

    # Dump to file
    part_time = time.time()
    folder = "COMPSs_RO-Crate_" + run_uuid + "/"
    compss_crate.write(folder)
    print(f"PROVENANCE | COMPSs RO-Crate created successfully in subfolder {folder}")
    print(f"PROVENANCE | RO-CRATE dump TIME: {time.time() - part_time} s")
    # cleanup from workingdir
    os.remove("compss_command_line_arguments.txt")


if __name__ == "__main__":
    import sys
    import time

    exec_time = time.time()

    # Usage: python /path_to/generate_COMPSs_RO-Crate.py ro-crate-info.yaml /path_to/dataprovenance.log
    if len(sys.argv) != 3:
        print(
            "PROVENANCE | Usage: python /path_to/generate_COMPSs_RO-Crate.py "
            "ro-crate-info.yaml /path_to/dataprovenance.log"
        )
        exit()
    else:
        info_yaml = sys.argv[1]
        dp_log = sys.argv[2]
        path_dplog = Path(sys.argv[2])
        complete_graph = path_dplog.parent / "monitor/complete_graph.svg"
    main()

    print(
        f"PROVENANCE | RO-CRATE GENERATION TOTAL EXECUTION TIME: {time.time() - exec_time} s"
    )
