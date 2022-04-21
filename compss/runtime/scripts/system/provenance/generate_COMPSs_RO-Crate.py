#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

from pathlib import Path

import yaml
import os
import uuid
import typing


CRATE = ROCrate()


def add_file_not_in_crate(file_name: str) -> list:
    """
    When adding files not physically in the Crate, they must be removed
    from the hasPart clause

    CAUTION: If the file has been already added (e.g. for INOUT files)
    add_file won't succeed in adding a second entity with the same name

    :param file_name: File added as input or output, but not in the RO-Crate
    :returns: Updated hasPart clause from RO-Crate
    """

    # TODO: for directories: use .iterdir()

    fn = Path(file_name)
    file_properties = {"name": fn.name}

    if fn.parts[0] == "file:":  # Dealing with a local file
        tuple_path = fn.parts
        list_path = list(tuple_path)
        new_path = []
        for i, item in enumerate(list_path):
            if i > 1:  # Remove file: and hostname
                new_path.append(item)
        j_np = "/" + "/".join(new_path)
        new_fn = Path(j_np)
        file_properties["contentSize"] = new_fn.stat().st_size
        CRATE.add_file(file_name, validate_url=False, properties=file_properties)
    else:  # Remote file. validate_url already adds contentSize and encodingFormat from the remote file
        CRATE.add_file(file_name, validate_url=True, properties=file_properties)

    # print(f"BEFORE HACK: CRATE.root_dataset._jsonld[hasPart] is: {CRATE.root_dataset._jsonld['hasPart']}")
    # Hack: Avoid including non validated remote files in the "hasPart" section
    has_part_crate = CRATE.root_dataset._jsonld["hasPart"]  # Assign hasPart list
    if has_part_crate[-1]["@id"] == file_name:
        has_part_crate.pop(-1)  # Can't use remove, file_name is in a dictionary now
        # print(f"AFTER HACK: has_part_crate is: {has_part_crate}")
    # else:
    # print(f"HACK SKIPPED: has_part_crate is: {has_part_crate}")

    return has_part_crate


def get_main_entities() -> typing.Tuple[str, str, str]:
    """
    Get COMPSs version and mainEntity from dataprovenance.log first lines
    3 First lines expected format: compss_version_number\n main_entity\n output_profile_file\n
    Next lines are for "accessed files" and "direction"

    :returns: COMPSs version, main COMPSs file name, COMPSs profile file name
    """

    with open(dp_log, "r") as f:
        compss_v = next(f).rstrip()  # First line, COMPSs version number
        second_line = next(
            f
        ).rstrip()  # Second, main_entity. Use better rstrip, just in case there is no '\n'
        # clean_path = secondline[:-1] # Remove final "\n"
        main_entity_fn = Path(second_line)
        third_line = next(f).rstrip()
        out_profile_fn = Path(third_line)

    return compss_v, main_entity_fn.name, out_profile_fn.name


def process_accessed_files() -> typing.Tuple[list, list]:
    """
    Process all the files the COMPSs workflow has accessed. They will be the
    overall inputs needed and outputs generated of the whole workflow

    :returns: List of Inputs and Outputs of the COMPSs workflow
    """

    inputs = set()
    outputs = set()

    with open(dp_log, "r") as f:
        for line in f:
            file_record = line.rstrip().split(" ")
            if len(file_record) == 2:
                # print(f"File name: {file_record[0]}, Direction: {file_record[1]}")
                if file_record[1] == "IN":
                    inputs.add(file_record[0])
                elif file_record[1] == "OUT":
                    outputs.add(file_record[0])
                else:  # INOUT
                    inputs.add(file_record[0])
                    outputs.add(file_record[0])
            # else dismiss the line

    l_ins = list(inputs)
    l_ins.sort()
    l_outs = list(outputs)
    l_outs.sort()

    print(f"INPUTS({len(l_ins)})")
    print(f"OUTPUTS({len(l_outs)})")

    return l_ins, l_outs


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
    # Check file extension, to decide how to add it in the Crate file_path.suffix
    file_properties = dict()
    file_properties["name"] = file_path.name
    file_properties["contentSize"] = os.path.getsize(file_name)

    if file_path.name == main_entity:
        file_properties["description"] = "Main file of the COMPSs workflow source files"
        file_properties["encodingFormat"] = "text/plain"
        if complete_graph.exists():
            file_properties["image"] = {
                "@id": "complete_graph.pdf"
            }  # Name as generated
        file_properties["input"] = []
        for item in ins:
            file_properties["input"].append({"@id": item})
        file_properties["output"] = []
        for item in outs:
            file_properties["output"].append({"@id": item})

    else:
        # Any other extra file needed
        file_properties["description"] = "Auxiliary File"
        if file_path.suffix == ".py":
            file_properties["encodingFormat"] = "text/plain"
        elif file_path.suffix == ".java":
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

    if file_path.name != main_entity:
        CRATE.add_file(file_path.name, properties=file_properties)
    else:
        # We get lang_version from dataprovenance.log
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
        ] = "Parameters passed as arguments to the COMPSs application through the command line"
        file_properties["encodingFormat"] = "text/plain"
        CRATE.add_file("compss_command_line_arguments.txt", properties=file_properties)


def main():
    # First, read values defined by user from ro-crate-info.yaml
    with open(info_yaml, "r", encoding="utf-8") as fp:
        try:
            yaml_content = yaml.safe_load(fp)
        except yaml.YAMLError as exc:
            print(exc)

    # Get Sections
    compss_wf_info = yaml_content["COMPSs Workflow Information"]
    author_info = yaml_content["Author"]
    organisation_info = yaml_content["Organisation"]

    # COMPSs Workflow RO Crate generation

    # Root Entity
    CRATE.name = compss_wf_info["name"]
    # print(f"Name: {CRATE.name}")
    CRATE.description = compss_wf_info["description"]
    CRATE.license = compss_wf_info["license"]  # Faltarà el detall de la llicència????
    CRATE.publisher = {"@id": organisation_info["ror"]}
    CRATE.creator = {"@id": author_info["orcid"]}

    # Get mainEntity from COMPSs runtime report dataprovenance.log

    compss_ver, main_entity, out_profile = get_main_entities()
    print(
        f"COMPSs version: {compss_ver}, main_entity is: {main_entity}, out_profile is: {out_profile}"
    )

    # Process set of accessed files, as reported by COMPSs runtime.
    # This must be done before adding the Workflow to the RO-Crate

    ins, outs = process_accessed_files()

    # Add files that will be physically in the crate to hasPart
    for file in compss_wf_info["files"]:
        # print(f"File: {file}")
        add_file_to_crate(file, compss_ver, main_entity, out_profile, ins, outs)

    # Add files not in the Crate
    for item in ins:
        hp_crate = add_file_not_in_crate(item)
        # print(f"OUTSIDE FUNC: CRATE.root_dataset._jsonld[hasPart] is: {CRATE.root_dataset._jsonld['hasPart']}")
        CRATE.root_dataset._jsonld["hasPart"] = hp_crate
        # print(f"AFTER ASSIGN: CRATE.root_dataset._jsonld[hasPart] is: {CRATE.root_dataset._jsonld['hasPart']}")

    for item in outs:
        hp_crate = add_file_not_in_crate(item)
        CRATE.root_dataset._jsonld["hasPart"] = hp_crate

    # Contextual Entities

    CRATE.add(
        Person(
            CRATE,
            author_info["orcid"],
            {
                "name": author_info["name"],
                "contactPoint": {"@id": "mailto:" + author_info["e-mail"]},
                "affiliation": {"@id": organisation_info["ror"]},
            },
        )
    )

    CRATE.add(
        ContextEntity(
            CRATE,
            "mailto:" + author_info["e-mail"],
            {
                "@type": "ContactPoint",
                "contactType": "Author",
                "email": author_info["e-mail"],
                "identifier": author_info["e-mail"],
                "url": author_info["orcid"],
            },
        )
    )

    CRATE.add(
        ContextEntity(
            CRATE,
            organisation_info["ror"],
            {"@type": "Organization", "name": organisation_info["name"]},
        )
    )

    # COMPSs RO-Crate Provenance Info can be directly hardcoded by now

    CRATE.add(
        ContextEntity(
            CRATE,
            "#history-01",
            {
                "@type": "CreateAction",
                "object": {"@id": "./"},
                "name": "COMPSs RO-Crate automatically generated for Python applications",
                "endTime": "2021-03-22",
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