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
import time
import os
import json
import sys

from pathlib import Path
from hashlib import sha256
from mmap import mmap, ACCESS_READ

from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity

from provenance.processing.entities import get_manually_defined_software_requirements


LANGUAGE_EXTENSIONS = (".java", ".py", ".sh")


def add_file_to_crate(
    compss_crate: ROCrate,
    wf_info: dict,
    file_name: str,
    compss_ver: str,
    main_entity: str,
    out_profile: str,
    in_sources_dir: str,
    complete_graph: str,
    info_yaml: str,
    auxiliary_file_list: list,
) -> str:
    """
    Get details of a file, and add it physically to the Crate. The file will be an application source file, so,
    the destination directory should be 'application_sources/'

    :param compss_crate: The COMPSs RO-Crate being generated
    :param file_name: File to be added physically to the Crate, full path resolved
    :param compss_ver: COMPSs version number
    :param main_entity: COMPSs file with the main code, full path resolved
    :param out_profile: COMPSs application profile output
    :param in_sources_dir: Path to the defined sources_dir. May be passed empty, so there is no sub-folder structure
        to be respected
    :param complete_graph: Full path to the file containing the workflow diagram
    :param info_yaml: Name of the YAML file specified by the user
    :param auxiliary_file_list: list of the auxiliary file contained in the hasPart

    :returns: Path where the file has been stored in the crate
    """

    file_path = Path(file_name)

    file_properties = {
        "name": file_path.name,
        "contentSize": os.path.getsize(file_name),
    }

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
        if file_path.suffix in LANGUAGE_EXTENSIONS:
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

    # provenance-run-crate standard not ready yet
    # auxiliary_file_list.append(path_in_crate)

    if file_name != main_entity:
        # Or check if the file is an executable (.py or .java)
        # if file_name.endswith(".py") or file_name.endswith(".java")
        if __debug__:
            print(f"PROVENANCE DEBUG | Adding auxiliary source file: {file_name}")
        compss_crate.add_file(
            source=file_name, dest_path=path_in_crate, properties=file_properties
        )
    else:
        # Add software dependencies as softwareRequirements
        req_list = get_manually_defined_software_requirements(
            compss_crate, wf_info, info_yaml
        )
        if req_list:
            if len(req_list) > 1:
                file_properties["softwareRequirements"] = req_list
            else:
                file_properties["softwareRequirements"] = req_list[0]

        # We get lang_version from dataprovenance.log
        if __debug__:
            print(
                f"PROVENANCE DEBUG | Adding main source file: {file_path.name}, file_name: {file_name}"
            )
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
            file_properties = {}
            file_properties["name"] = "complete_graph.svg"
            file_properties["contentSize"] = complete_graph.stat().st_size
            file_properties["@type"] = ["File", "ImageObject", "WorkflowSketch"]
            file_properties["description"] = (
                "The graph diagram of the workflow, automatically generated by COMPSs runtime"
            )
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

            # Adding checksum for the file. sha3_256 is stronger, but slower and not installed by default in may systems
            with open(complete_graph) as file, mmap(
                file.fileno(), 0, access=ACCESS_READ
            ) as file:
                file_properties["sha256"] = sha256(file).hexdigest()

            compss_crate.add_file(complete_graph, properties=file_properties)
        else:
            print(
                "PROVENANCE | WARNING: complete_graph.svg file not found.\n"
                "\tProvenance will be generated without image property"
            )

        # out_profile
        if os.path.exists(out_profile):
            if out_profile.split("/")[-1] != "App_Profile.json":
                file_properties = {}
                file_properties["name"] = out_profile
                file_properties["contentSize"] = os.path.getsize(out_profile)
                file_properties["description"] = "COMPSs application Tasks profile"
                file_properties["encodingFormat"] = [
                    "application/json",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/817"},
                ]

                # Fix COMPSs crappy format of JSON files
                with open(out_profile, encoding="UTF-8") as op_file:
                    op_json = json.load(op_file)
                with open(out_profile, "w", encoding="UTF-8") as op_file:
                    json.dump(op_json, op_file, indent=1)

                # Add JSON as ContextEntity
                compss_crate.add(
                    ContextEntity(
                        compss_crate,
                        "https://www.nationalarchives.gov.uk/PRONOM/fmt/817",
                        {"@type": "WebSite", "name": "JSON Data Interchange Format"},
                    )
                )

                # Adding checksum for the file. sha3_256 is stronger, but slower and not installed by default in may systems
                with open(out_profile) as file, mmap(
                    file.fileno(), 0, access=ACCESS_READ
                ) as file:
                    file_properties["sha256"] = sha256(file).hexdigest()

                compss_crate.add_file(out_profile, properties=file_properties)
        else:
            print(
                "PROVENANCE | WARNING: COMPSs application profile has not been generated.\n"
                "\tMake sure you use runcompss with --output_profile=file_name\n"
                "\tProvenance will be generated without profiling information"
            )

        # compss_submission_command_line.txt. Old compss_command_line_arguments.txt
        # if os.path.exists("compss_submission_command_line.txt"):
        #     file_properties = {}
        #     file_properties["name"] = "compss_submission_command_line.txt"
        #     file_properties["contentSize"] = os.path.getsize(
        #         "compss_submission_command_line.txt"
        #     )
        #     file_properties["description"] = (
        #         "COMPSs submission command line (runcompss / enqueue_compss), including flags and parameters passed to the application"
        #     )
        #     file_properties["encodingFormat"] = "text/plain"
        #     with open("compss_submission_command_line.txt") as file, mmap(
        #         file.fileno(), 0, access=ACCESS_READ
        #     ) as file:
        #         file_properties["sha256"] = sha256(file).hexdigest()
        #     compss_crate.add_file(
        #         "compss_submission_command_line.txt", properties=file_properties
        #     )
        # else:
        #     print(
        #         "PROVENANCE | WARNING: COMPSs submission command line has not been generated.\n"
        #         "\tProvenance will be generated without submission information"
        #     )

        # ro-crate-info.yaml
        yaml_path = Path(info_yaml)
        file_properties = {}
        file_properties["name"] = yaml_path.name
        file_properties["contentSize"] = os.path.getsize(yaml_path)
        file_properties["description"] = (
            "COMPSs Workflow Provenance YAML configuration file"
        )
        file_properties["encodingFormat"] = [
            "YAML",
            {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/818"},
        ]

        # Add YAML as ContextEntity
        compss_crate.add(
            ContextEntity(
                compss_crate,
                "https://www.nationalarchives.gov.uk/PRONOM/fmt/818",
                {"@type": "WebSite", "name": "YAML"},
            )
        )

        with open(info_yaml) as file, mmap(
            file.fileno(), 0, access=ACCESS_READ
        ) as file:
            file_properties["sha256"] = sha256(file).hexdigest()

        compss_crate.add_file(yaml_path, properties=file_properties)

        return ""

    # print(f"ADDED FILE: {file_name} as {path_in_crate}")

    return path_in_crate


def add_application_source_files(
    compss_crate: ROCrate,
    compss_wf_info: dict,
    compss_ver: str,
    main_entity: str,
    out_profile: str,
    info_yaml: str,
    complete_graph: str,
    auxiliary_file_list: list,
) -> None:
    """
    Add all application source files as part of the crate. This means, to include them physically in the resulting
    bundle

    :param compss_crate: The COMPSs RO-Crate being generated
    :param compss_wf_info: YAML dict to extract info form the application, as specified by the user
    :param compss_ver: COMPSs version number
    :param main_entity: COMPSs file with the main code, full path resolved
    :param out_profile: COMPSs application profile output file
    :param info_yaml: Name of the YAML file specified by the user
    :param complete_graph: Full path to the file containing the workflow diagram
    :param auxiliary_file_list: list of the auxiliary file contained in the hasPart

    :returns: None
    """

    part_time = time.time()

    sources_list = []

    if "sources" in compss_wf_info:
        if isinstance(compss_wf_info["sources"], list):
            sources_list.extend(compss_wf_info["sources"])
        else:
            sources_list.append(compss_wf_info["sources"])
    if "files" in compss_wf_info:
        # Backward compatibility: if old "sources_dir" and "files" have been used, merge in sources_list.
        if isinstance(compss_wf_info["files"], list):
            sources_list.extend(compss_wf_info["files"])
        else:
            sources_list.append(compss_wf_info["files"])
    if "sources_dir" in compss_wf_info:
        #  Backward compatibility: if old "sources_dir" and "files" have been used, merge in sources_list.
        # sources_list = list(tuple(wf_info["files"])) + list(tuple(wf_info["sources"]))
        if isinstance(compss_wf_info["sources_dir"], list):
            sources_list.extend(compss_wf_info["sources_dir"])
        else:
            sources_list.append(compss_wf_info["sources_dir"])
    # else: Nothing defined, covered at the end

    added_files = []
    added_dirs = []

    #  TODO: before dealing with all files from all directories, update the list of sources, removing any sub-folders
    #  already included in other folders. Do it with source_list_copy, to avoid strange iterations
    #  This would avoid the issue of sources: [sources_empty/empty_dir_1/, sources_empty/] which adds empty_dir_1
    #  to the root of application_sources/.

    for source in sources_list:
        path_source = Path(source).expanduser()
        if not path_source.exists():
            print(
                f"PROVENANCE | WARNING: A file or directory defined as 'sources' in {info_yaml} does not exist "
                f"({source})"
            )
            continue
        resolved_source = str(path_source.resolve())
        if any(part.startswith(".") for part in Path(str(resolved_source)).parts):
            continue
        if os.path.isdir(resolved_source):
            # Adding files twice is not a drama, since add_file_to_crate won't add them twice, but we save traversing directories
            if resolved_source in added_dirs:
                print(
                    f"PROVENANCE | WARNING: A directory addition was attempted twice: {resolved_source}"
                )
                continue  # Do not traverse the directory again
            if any(resolved_source.startswith(dir_item) for dir_item in added_dirs):
                print(
                    f"PROVENANCE | WARNING: A sub-directory addition was attempted twice: {resolved_source}"
                )
                continue
            if any(dir_item.startswith(resolved_source) for dir_item in added_dirs):
                print(
                    f"PROVENANCE | WARNING: A parent directory of a previously added sub-directory is being added. Some "
                    f"files will be traversed twice in: {resolved_source}"
                )
                # Can't continue, we need to traverse the parent directory. Luckily, files won't be added twice
            added_dirs.append(resolved_source)
            for root, dirs, files in os.walk(
                resolved_source, topdown=True, followlinks=True
            ):
                if "__pycache__" in root:
                    continue  # We skip __pycache__ subdirectories
                for f_name in files:
                    if f_name.startswith(("*", ".")):
                        # Avoid dealing with symlinks with wildcards
                        continue
                    resolved_file = os.path.join(root, f_name)
                    if resolved_file not in added_files:
                        add_file_to_crate(
                            compss_crate,
                            compss_wf_info,
                            resolved_file,
                            compss_ver,
                            main_entity,
                            out_profile,
                            resolved_source,
                            complete_graph,
                            info_yaml,
                            auxiliary_file_list,
                        )
                        added_files.append(resolved_file)
                    else:
                        print(
                            f"PROVENANCE | WARNING: A file addition was attempted twice: "
                            f"{resolved_file} in {resolved_source}"
                        )
                for dir_name in dirs:
                    # Check if it's an empty directory, needs to be added by hand
                    full_dir_name = os.path.join(root, dir_name)
                    if not os.listdir(full_dir_name):
                        if __debug__:
                            print(
                                f"PROVENANCE DEBUG | Adding an empty directory. root ({root}), full_dir_name ({full_dir_name}), resolved_source ({resolved_source})"
                            )
                        # Workaround to add empty directories in a git repository
                        git_keep = Path(full_dir_name + "/" + ".gitkeep")
                        Path.touch(git_keep)
                        add_file_to_crate(
                            compss_crate,
                            compss_wf_info,
                            str(git_keep),
                            compss_ver,
                            main_entity,
                            out_profile,
                            resolved_source,
                            complete_graph,
                            info_yaml,
                            auxiliary_file_list,
                        )
            if not os.listdir(resolved_source):
                # The root directory itself is empty
                if __debug__:
                    print(
                        f"PROVENANCE DEBUG | Adding an empty directory. resolved_source ({resolved_source})"
                    )
                # Workaround to add empty directories in a git repository
                git_keep = Path(resolved_source + "/" + ".gitkeep")
                Path.touch(git_keep)
                add_file_to_crate(
                    compss_crate,
                    compss_wf_info,
                    str(git_keep),
                    compss_ver,
                    main_entity,
                    out_profile,
                    resolved_source,
                    complete_graph,
                    info_yaml,
                    auxiliary_file_list,
                )
        elif os.path.isfile(resolved_source):
            if resolved_source not in added_files:
                add_file_to_crate(
                    compss_crate,
                    compss_wf_info,
                    resolved_source,
                    compss_ver,
                    main_entity,
                    out_profile,
                    "",
                    complete_graph,
                    info_yaml,
                    auxiliary_file_list,
                )
                added_files.append(resolved_source)
            else:
                print(
                    f"PROVENANCE | WARNING: A file addition was attempted twice: "
                    f"{resolved_source} in {added_dirs}"
                )
        else:
            print(
                f"PROVENANCE | WARNING: A defined source is neither a directory, nor a file ({resolved_source})"
            )

    if len(sources_list) == 0:
        # No sources defined by the user, add the selected main_entity at least
        add_file_to_crate(
            compss_crate,
            compss_wf_info,
            main_entity,
            compss_ver,
            main_entity,
            out_profile,
            "",
            complete_graph,
            info_yaml,
            auxiliary_file_list,
        )
        added_files.append(main_entity)

    # Add auxiliary files as hasPart to the ComputationalWorkflow main file
    # Not working well when an application has several versions (ex: Java matmul files, objects, arrays)

    # provenance-run-crate standard not ready yet
    # for e in compss_crate.data_entities:
    #     if "ComputationalWorkflow" in e.type:
    #         for file in auxiliary_file_list:
    #             e.append_to("hasPart", {"@id": file})

    print(f"PROVENANCE | Application source files detected ({len(added_files)})")
    if __debug__:
        print(f"PROVENANCE DEBUG | Source files detected: {added_files}")

    print(
        f"PROVENANCE | RO-Crate adding source files TIME: {time.time() - part_time} s"
    )
