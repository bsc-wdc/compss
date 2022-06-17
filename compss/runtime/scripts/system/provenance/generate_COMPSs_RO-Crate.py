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

from rocrate.rocrate import ROCrate
from rocrate import rocrate_api
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


CRATE = ROCrate()


def add_file_not_in_crate(in_url: str) -> None:
    """
    When adding local files that we don't want to be physically in the Crate, they must be added with a file:// URI
    CAUTION: If the file has been already added (e.g. for INOUT files) add_file won't succeed in adding a second entity
    with the same name

    :param in_url: File added as input or output, but not in the RO-Crate

    :returns: None
    """

    url_parts = urlsplit(in_url)
    final_item_name = os.path.basename(in_url)
    file_properties = {
        "name": final_item_name,
        "sdDatePublished": iso_now(),
    }  # Register when the Data Entity was last accessible

    if url_parts.scheme == "file":  # Dealing with a local file
        file_properties["contentSize"] = os.path.getsize(url_parts.path)
        CRATE.add_file(
            in_url,
            fetch_remote=False,
            validate_url=False,  # True fails at MN4 when file URI points to a node hostname (only localhost works)
            properties=file_properties,
        )

    elif url_parts.scheme == "dir":  # DIRECTORY parameter
        # For directories, describe all files inside the directory
        hasPart_list = []
        for root, dirs, files in os.walk(
            url_parts.path, topdown=True
        ):  # Ignore references to sub-directories (they are not a specific in or out of the workflow), but not their files
            dirs.sort()
            files.sort()
            for f_name in files:
                listed_file = os.path.join(root, f_name)
                dir_f_url = "file://" + url_parts.netloc + listed_file
                hasPart_list.append({"@id": dir_f_url})
                dir_f_properties = {
                    "name": f_name,
                    "sdDatePublished": iso_now(),  # Register when the Data Entity was last accessible
                    "contentSize": os.path.getsize(listed_file),
                }
                CRATE.add_file(
                    dir_f_url,
                    fetch_remote=False,
                    validate_url=False,
                    # True fails at MN4 when file URI points to a node hostname (only localhost works)
                    properties=dir_f_properties,
                )
        file_properties["hasPart"] = hasPart_list
        CRATE.add_dataset(
            fix_dir_url(in_url), properties=file_properties
        )  # fetch_remote and validate_url false by default. add_dataset also ensures the URL ends with '/'

    else:  # Remote file, currently not supported in COMPSs. validate_url already adds contentSize and encodingFormat
        # from the remote file
        CRATE.add_file(in_url, validate_url=True, properties=file_properties)


def get_main_entities(list_of_files: list) -> typing.Tuple[str, str, str]:
    """
    Get COMPSs version and mainEntity from dataprovenance.log first lines
    3 First lines expected format: compss_version_number\n main_entity\n output_profile_file\n
    Next lines are for "accessed files" and "direction"

    :param list_of_files: List of files that form the application, as specified by the user

    :returns: COMPSs version, main COMPSs file name, COMPSs profile file name
    """

    with open(dp_log, "r") as f:
        compss_v = next(f).rstrip()  # First line, COMPSs version number
        second_line = next(
            f
        ).rstrip()  # Second, main_entity. Use better rstrip, just in case there is no '\n'
        main_entity_fn = Path(second_line)
        if main_entity_fn.suffix == ".py":  # PyCOMPSs, main_entity equals main_file.py
            main_entity = main_entity_fn.name
        else:  # COMPSs Java application, consider first file as main
            main_entity = Path(list_of_files[0]).name
        third_line = next(f).rstrip()
        out_profile_fn = Path(third_line)

    return compss_v, main_entity, out_profile_fn.name


def process_accessed_files() -> typing.Tuple[list, list]:
    """
    Process all the files the COMPSs workflow has accessed. They will be the overall inputs needed and outputs
    generated of the whole workflow.
    - If a task that is an INPUT, was previously an OUTPUT, it means it is an intermediate file, therefore we discard it
    - Works fine with COLLECTION_FILE_IN, COLLECTION_FILE_OUT and COLLECTION_FILE_INOUT

    :returns: List of Inputs and Outputs of the COMPSs workflow
    """

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

    print(f"INPUTS({len(l_ins)})")
    print(f"OUTPUTS({len(l_outs)})")

    return l_ins, l_outs


def fix_dir_url(in_url: str) -> str:
    """
    Fix dir:// URL returned by the runtime, change it to file:// and ensure it ends with '/'

    :param in_url: URL that may need to be fixed

    :returns: A file:// URL
    """

    runtime_url = urlsplit(in_url)
    if runtime_url.scheme == "dir":  # Fix dir:// to file:// and ensure it ends with a slash
        new_url = "file://" + runtime_url.netloc + runtime_url.path
        if new_url[-1] != '/':
            new_url += '/'  # Add end slash if needed
        return new_url
    else:
        return in_url  # No changes required


def add_file_to_crate(
    file_name: str,
    compss_ver: str,
    main_entity: str,
    out_profile: str,
    ins: list,
    outs: list,
) -> None:
    """
    Get details of a file, and add it physically to the Crate

    :param file_name: File to be added physically to the Crate
    :param compss_ver: COMPSs version number
    :param main_entity: COMPSs file with the main code
    :param out_profile: COMPSs application profile output
    :param ins: List of input files
    :param outs: List of output files

    :returns: None
    """

    file_path = Path(file_name)
    file_properties = dict()
    file_properties["name"] = file_path.name
    file_properties["contentSize"] = os.path.getsize(file_name)
    # Check file extension, to decide how to add it in the Crate file_path.suffix
    # if file_path.suffix == ".jar":  # We can ignore main_entity
    #     namespace = main_entity_in.rstrip().split(".")
    #     print(f"namespace: {namespace}")
    #     main_entity = namespace[0] + ".jar"  # Rebuild package name
    # else:  # main_file.py or any other file
    #     main_entity = main_entity_in
    # print(f"main_entity is: {main_entity}, file_path is: {file_path}")

    if file_path.name == main_entity:
        file_properties["description"] = "Main file of the COMPSs workflow source files"
        if file_path.suffix == ".jar":
            file_properties["encodingFormat"] = (
                [
                    "application/java-archive",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/412"},
                ],
            )
            # Add JAR as ContextEntity
            CRATE.add(
                ContextEntity(
                    CRATE,
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
            CRATE.add(
                ContextEntity(
                    CRATE,
                    "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/415",
                    {"@type": "WebSite", "name": "Java Compiled Object Code"},
                )
            )
        else:  # .py, .java, .c, .cc, .cpp
            file_properties["encodingFormat"] = "text/plain"
        if complete_graph.exists():
            file_properties["image"] = {
                "@id": "complete_graph.pdf"
            }  # Name as generated
        file_properties["input"] = []
        for item in ins:
            file_properties["input"].append({"@id": fix_dir_url(item)})
        file_properties["output"] = []
        for item in outs:
            file_properties["output"].append({"@id": fix_dir_url(item)})

    else:
        # Any other extra file needed
        file_properties["description"] = "Auxiliary File"
        if file_path.suffix == ".py" or file_path.suffix == ".java":
            file_properties["encodingFormat"] = "text/plain"
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
        elif file_path.suffix == ".jar":
            file_properties["encodingFormat"] = (
                [
                    "application/java-archive",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/412"},
                ],
            )
            # Add JAR as ContextEntity
            CRATE.add(
                ContextEntity(
                    CRATE,
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
            CRATE.add(
                ContextEntity(
                    CRATE,
                    "https://www.nationalarchives.gov.uk/PRONOM/x-fmt/415",
                    {"@type": "WebSite", "name": "Java Compiled Object Code"},
                )
            )

    if file_path.name != main_entity:
        print(f"Adding file: {file_path}")
        CRATE.add_file(file_path, properties=file_properties)
    else:
        # We get lang_version from dataprovenance.log
        print(f"Adding file: {file_path.name}, file_path: {file_path}")
        CRATE.add_workflow(
            file_path,
            file_path.name,
            main=True,
            lang="COMPSs",
            lang_version=compss_ver,
            properties=file_properties,
            gen_cwl=False,
        )

        # complete_graph.pdf
        if complete_graph.exists():
            file_properties = dict()
            file_properties["name"] = "complete_graph.pdf"
            file_properties["contentSize"] = complete_graph.stat().st_size
            file_properties["@type"] = ["File", "ImageObject", "WorkflowSketch"]
            file_properties[
                "description"
            ] = "The graph diagram of the workflow, automatically generated by COMPSs runtime"
            file_properties["encodingFormat"] = (
                [
                    "application/pdf",
                    {"@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/276"},
                ],
            )
            file_properties["about"] = {"@id": main_entity}
            # Add PDF as ContextEntity
            CRATE.add(
                ContextEntity(
                    CRATE,
                    "https://www.nationalarchives.gov.uk/PRONOM/fmt/276",
                    {
                        "@type": "WebSite",
                        "name": "Acrobat PDF 1.7 - Portable Document Format",
                    },
                )
            )
            CRATE.add_file(complete_graph, properties=file_properties)
        else:
            print(
                f"WARNING: complete_graph.pdf file not found. Provenance will be generated without image property"
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
            # Add JSON as ContextEntity
            CRATE.add(
                ContextEntity(
                    CRATE,
                    "https://www.nationalarchives.gov.uk/PRONOM/fmt/817",
                    {"@type": "WebSite", "name": "JSON Data Interchange Format"},
                )
            )
            CRATE.add_file(out_profile, properties=file_properties)
        else:
            print(
                f"WARNING: COMPSs application profile has not been generated. \
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
        CRATE.add_file("compss_command_line_arguments.txt", properties=file_properties)


def main():
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
            template = """COMPSs Workflow Information:
  name: Name of your COMPSs application
  description: Detailed description of your COMPSs application
  license: Apache-2.0 #Provide better a URL, but these strings are accepted:
                  # https://about.workflowhub.eu/Workflow-RO-Crate/#supported-licenses
  files: [main_file.py, aux_file_1.py, aux_file_2.py] # List of application files
Authors:
  - name: Author_1 Name
    e-mail: author_1@email.com
    orcid: https://orcid.org/XXXX-XXXX-XXXX-XXXX
    organisation_name: Institution_1 name
    ror: https://ror.org/XXXXXXXXX # Find them in ror.org
  - name: Author_2 Name
    e-mail: author2@email.com
    orcid: https://orcid.org/YYYY-YYYY-YYYY-YYYY
    organisation_name: Institution_2 name
    ror: https://ror.org/YYYYYYYYY # Find them in ror.org
            """
            ft.write(template)
            print(
                f"ERROR: YAML file ro-crate-info.yaml not found in your working directory. A template has been generated"
                f" in file ro-crate-info_TEMPLATE.yaml"
            )
        raise

    # Get Sections
    compss_wf_info = yaml_content["COMPSs Workflow Information"]
    authors_info = yaml_content["Authors"]  # Now a list of authors

    # COMPSs Workflow RO Crate generation

    # Root Entity
    CRATE.name = compss_wf_info["name"]
    CRATE.description = compss_wf_info["description"]
    CRATE.license = compss_wf_info["license"]  # Faltarà el detall de la llicència????
    authors_set = set()
    organisations_set = set()
    for author in authors_info:
        authors_set.add(author["orcid"])
        organisations_set.add(author["ror"])
        CRATE.add(
            Person(
                CRATE,
                author["orcid"],
                {
                    "name": author["name"],
                    "contactPoint": {"@id": "mailto:" + author["e-mail"]},
                    "affiliation": {"@id": author["ror"]},
                },
            )
        )
        CRATE.add(
            ContextEntity(
                CRATE,
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
        CRATE.add(
            ContextEntity(
                CRATE,
                author["ror"],
                {"@type": "Organization", "name": author["organisation_name"]},
            )
        )
    author_list = list()
    for creator in authors_set:
        author_list.append({"@id": creator})
    CRATE.creator = author_list
    org_list = list()
    for org in organisations_set:
        org_list.append({"@id": org})
    CRATE.publisher = org_list

    # Get mainEntity from COMPSs runtime report dataprovenance.log

    compss_ver, main_entity, out_profile = get_main_entities(compss_wf_info["files"])
    print(
        f"COMPSs version: {compss_ver}, main_entity is: {main_entity}, out_profile is: {out_profile}"
    )

    # Process set of accessed files, as reported by COMPSs runtime.
    # This must be done before adding the Workflow to the RO-Crate

    ins, outs = process_accessed_files()

    # Add files that will be physically in the crate
    for file in compss_wf_info["files"]:
        add_file_to_crate(file, compss_ver, main_entity, out_profile, ins, outs)

    # Add files not to be physically in the Crate
    for item in ins:
        add_file_not_in_crate(item)

    for item in outs:
        add_file_not_in_crate(item)

    # COMPSs RO-Crate Provenance Info can be directly hardcoded by now

    CRATE.add(
        ContextEntity(
            CRATE,
            "#history-01",
            {
                "@type": "CreateAction",
                "object": {"@id": "./"},
                "name": "COMPSs RO-Crate automatically generated for Python applications",
                "endTime": "2022-03-22",
                "agent": {"@id": "https://orcid.org/0000-0003-0606-2512"},
                "actionStatus": {"@id": "http://schema.org/CompletedActionStatus"},
            },
        )
    )
    CRATE.add(
        ContextEntity(
            CRATE,
            "#history-02",
            {
                "@type": "CreateAction",
                "object": {"@id": "./"},
                "name": "COMPSs RO-Crate automatically generated for Java applications",
                "endTime": "2022-06-13",
                "agent": {"@id": "https://orcid.org/0000-0003-0606-2512"},
                "actionStatus": {"@id": "http://schema.org/CompletedActionStatus"},
            },
        )
    )

    # Dump to file
    folder = "COMPSs_RO-Crate_" + str(uuid.uuid4()) + "/"
    CRATE.write(folder)
    print(f"COMPSs RO-Crate created successfully in subfolder {folder}")
    # cleanup from workingdir
    os.remove("compss_command_line_arguments.txt")


if __name__ == "__main__":
    import sys

    # Usage: python /path_to/generate_COMPSs_RO-Crate.py ro-crate-info.yaml /path_to/dataprovenance.log
    if len(sys.argv) != 3:
        print(
            "Usage: python /path_to/generate_COMPSs_RO-Crate.py ro-crate-info.yaml /path_to/dataprovenance.log"
        )
        exit()
    else:
        info_yaml = sys.argv[1]
        dp_log = sys.argv[2]
        path_dplog = Path(sys.argv[2])
        complete_graph = path_dplog.parent / "monitor/complete_graph.pdf"
    main()
