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
import os
import subprocess
import socket
from typing import Any

import yaml

from pathlib import Path
from datetime import timezone
from datetime import datetime
import pytz

from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity

from provenance.utils.url_fixes import fix_dir_url
from provenance.processing.entities import add_person_definition


def get_stats_list(dp_path: str, start_time: datetime, end_time: datetime) -> list:
    """
    Function that provide a list of the statistical data recorded

    :param dp_path: pathname of the dataprovenance.log
    :param start_time: starting time of the execution
    :param end_time: ending time of the execution

    :return data_list: list of data parsed from dataprovenance.log
    """
    data_list = []
    with open(dp_path, "r") as data_provenance:
        for idx, row in enumerate(data_provenance.readlines()):
            if idx == 1:
                application_name = row.rstrip()
                continue
            elif idx < 3:
                continue

            parameter_list = list(filter(None, row.strip().split(" ")))
            len_row = len(parameter_list)
            if len_row >= 4 and not row.startswith("Task") and not row.startswith("parameter"):
                data_list.append(parameter_list)

        try:
            start_time = start_time.timestamp()
            end_time = end_time.timestamp()
            execution_time = int((end_time - start_time) * 1000)
            data_list.append(
                ["overall", application_name, "executionTime", str(execution_time)]
            )
            print(f"Program execution time: {execution_time*0.001} s")
        except TypeError:
            print("PROVENANCE | WARNING: could not retrieve execution time")

    return data_list


def add_execution(id_name: str, value: int) -> dict:
    """
    Function that generate a new dictionary of the number of executions.

    :param id_name: id of the new item
    :param value: value of the parameter passed

    :return new_item: new item referred to the number of executions data
    """
    # If there is no execution, it means that it haven't been executed
    if value == 0:
        value = None
    new_item = {
        "id": id_name,
        "@type": "PropertyValue",
        "name": "executions",
        "propertyID": "https://w3id.org/ro/terms/compss#executions",
        "value": str(value),
    }
    return new_item


def add_time(id_name: str, name_parameter: str, value: int) -> dict:
    """
    Function that generate a new dictionary of the item referred to a time.

    :param id_name: identifier of the Data Entity that is generated
    :param name_parameter: the name of the parameter
    :param value: value of the parameter passed

    :return new_item: new item referred to a time data
    """
    new_item = {
        "id": id_name,
        "@type": "PropertyValue",
        "name": name_parameter,
        "propertyID": f"https://w3id.org/ro/terms/compss#{name_parameter}",
        "unitCode": "https://qudt.org/vocab/unit/MilliSEC",
        "value": str(value),
    }
    return new_item


def get_new_item(id_name: str, stat: str, value: int) -> dict:
    if stat == "executions":
        return add_execution(id_name, value)
    else:
        return add_time(id_name, stat, value)


def get_resource_usage_dataset(
    dp_path: str, start_time: datetime, end_time: datetime
) -> list:
    """
    Function that provides a list of the statistical data recorded

    :param dp_path: pathname of the dataprovenance.log
    :param start_time: starting time of the execution
    :param end_time: ending time of the execution

    :return data_list: list of data parsed from dataprovenance.log
    """
    stats_list = get_stats_list(dp_path, start_time, end_time)
    resource_dataset = []
    for data in stats_list:
        resource = data[0]
        implementation = data[1]
        stat = data[2]
        try:
            value = int(data[3])
        except ValueError:
            value = None
        id_name = f"#{resource}.{implementation}.{stat}"
        new_item = get_new_item(id_name, stat, value)
        resource_dataset.append(new_item)
    return resource_dataset


def wrroc_create_action(
    compss_crate: ROCrate,
    main_entity: str,
    author_list: list,
    ins: list,
    outs: list,
    yaml_content: dict,
    info_yaml: str,
    log_dir: Path,
    end_time: datetime,
    run_uuid: str,
)-> tuple[ContextEntity, dict]:
    """
    Add a CreateAction term to the ROCrate to make it compliant with WRROC.  RO-Crate WorkflowRun Level 2 profile,
    aka. Workflow Run Crate.

    :param compss_crate: The COMPSs RO-Crate being generated
    :param main_entity: The name of the source file that contains the COMPSs application main() method
    :param author_list: List of authors as described in the YAML
    :param ins: List of input files of the workflow
    :param outs: List of output files of the workflow
    :param yaml_content: Content of the YAML file specified by the user
    :param info_yaml: Name of the YAML file specified by the user
    :param log_dir: Path object to the directory where dataprovenance.log file, profiling and trace files can be found
    :param end_time: Time where the COMPSs application execution ended
    :param run_uuid: UUID generated for this run
    """

    # Compliance with RO-Crate WorkflowRun Level 2 profile, aka. Workflow Run Crate
    # marenostrum4, nord3, ... BSC_MACHINE would also work
    host_name = os.getenv("SLURM_CLUSTER_NAME")
    if host_name is None:
        host_name = os.getenv("BSC_MACHINE")
        if host_name is None:
            host_name = socket.gethostname()
    job_id = os.getenv("SLURM_JOB_ID")

    main_entity_pathobj = Path(main_entity)

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
    uname = subprocess.run(["uname", "-a"], stdout=subprocess.PIPE, check=True)
    uname_out = uname.stdout.decode("utf-8")[:-1]  # Remove final '\n'

    # SLURM interesting variables: SLURM_JOB_NAME, SLURM_JOB_QOS, SLURM_JOB_USER, SLURM_SUBMIT_DIR, SLURM_NNODES or
    # SLURM_JOB_NUM_NODES, SLURM_JOB_CPUS_PER_NODE, SLURM_MEM_PER_CPU, SLURM_JOB_NODELIST or SLURM_NODELIST.

    environment_property = []
    for name, value in os.environ.items():
        if (
            name.startswith(("SLURM_JOB", "SLURM_MEM", "SLURM_SUBMIT", "COMPSS"))
            and name != "SLURM_JOBID"
        ):
            # Changed to 'environment' term in WRROC v0.4
            env_var = {}
            env_var["@type"] = "PropertyValue"
            env_var["name"] = name
            env_var["value"] = value
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    "#" + name.lower(),
                    properties=env_var,
                )
            )
            environment_property.append({"@id": "#" + name.lower()})

    description_property = uname_out

    resolved_main_entity = main_entity
    for entity in compss_crate.get_entities():
        if "ComputationalWorkflow" in entity.type:
            resolved_main_entity = entity.id

    # Register user submitting the workflow
    agent_added = False
    if "Agent" in yaml_content:
        if isinstance(yaml_content["Agent"], list):
            print(
                f"PROVENANCE | WARNING: 'Agent' in {info_yaml} can only be a single person. First item selected "
                f"as the application submitter agent"
            )
            agent_entity = yaml_content["Agent"][0]
        else:
            agent_entity = yaml_content["Agent"]
        added_person, agent_entity = add_person_definition(
            compss_crate, "Agent", agent_entity, info_yaml
        )
        if added_person:
            agent = {"@id": agent_entity["orcid"]}
            agent_added = True
        else:
            print(f"PROVENANCE | WARNING: 'Agent' in {info_yaml} wrongly defined")

    if not agent_added and "Submitter" in yaml_content:
        # Make Submitter backwards compatible
        print(
            f"PROVENANCE | WARNING: deprecated term 'Submitter' used in {info_yaml}. Use 'Agent' instead"
        )
        if isinstance(yaml_content["Submitter"], list):
            print(
                f"PROVENANCE | WARNING: 'Submitter' in {info_yaml} can only be a single person. First item selected "
                f"as the application submitter agent"
            )
            agent_entity = yaml_content["Submitter"][0]
        else:
            agent_entity = yaml_content["Submitter"]

        added_person, agent_entity = add_person_definition(
            compss_crate, "Agent", agent_entity, info_yaml
        )
        if added_person:
            agent = {"@id": agent_entity["orcid"]}
            agent_added = True
        else:
            print(f"PROVENANCE | WARNING: 'Submitter' in {info_yaml} wrongly defined")

    if (
        "Agent" not in yaml_content and "Submitter" not in yaml_content
    ) or not agent_added:
        # Choose first author, to avoid leaving it empty. May be true most of the times
        if author_list:
            agent = author_list[0]
            print(
                f"PROVENANCE | WARNING: 'Agent' missing or not correctly specified in {info_yaml}. First author selected by default"
            )
        else:
            agent = None
            print(
                f"PROVENANCE | WARNING: No 'Authors' or 'Agent' specified in {info_yaml}"
            )

    if "Agent" in yaml_content and "Updated" in agent_entity:
        # Write updated YAML to disk
        with open("GENERATED_" + info_yaml, "w", encoding="utf-8") as f_y:
            yaml.dump(yaml_content, f_y, default_flow_style=False)

    create_action_properties = {
        "@type": "CreateAction",
        "instrument": {"@id": resolved_main_entity},  # Resolved path of the main file
        "actionStatus": {"@id": "http://schema.org/CompletedActionStatus"},
        "endTime": end_time.isoformat(),  # endTime of the application corresponds to the start of the provenance generation
        "name": name_property,
        "description": description_property,
    }
    if len(environment_property) > 0:
        create_action_properties["environment"] = environment_property

    # Take startTime and endTime from dataprovenance.log when no queuing system is involved
    # The string generated by the runtime is already in UTC
    # If times are found in dataprovenance.log, they replace the ones obtained at the beginning of provenance generation
    # and obtained with sacct
    dp_log = log_dir / "dataprovenance.log"
    with open(dp_log, "r", encoding="UTF-8") as dp_file:
        last_line = ""
        for i, line in enumerate(dp_file):
            if i == 3:
                try:
                    start_time = datetime.strptime(
                        line.strip(), "%Y-%m-%dT%H:%M:%S.%f%z"
                    )
                    create_action_properties["startTime"] = start_time.replace(
                        microsecond=0
                    ).isoformat()
                except ValueError:
                    print(
                        f"PROVENANCE | WARNING: No 'startTime' found in dataprovenance.log. SLURM's job start time "
                        f"will be used, if available"
                    )
                    if job_id:
                        # sacct may fail if the run is done from a container
                        try:
                            sacct_command = [
                                "sacct",
                                "-j",
                                str(job_id),
                                "--format=Start",
                                "--noheader",
                            ]
                            head_command = ["head", "-n", "1"]
                            sacct_process = subprocess.Popen(
                                sacct_command, stdout=subprocess.PIPE
                            )
                            head_process = subprocess.Popen(
                                head_command,
                                stdin=sacct_process.stdout,
                                stdout=subprocess.PIPE,
                            )
                            output, _ = head_process.communicate()
                            start_time_str = output.decode("utf-8").strip()
                            # Convert start time to datetime object
                            start_time = datetime.strptime(
                                start_time_str, "%Y-%m-%dT%H:%M:%S"
                            )
                            create_action_properties["startTime"] = (
                                start_time.astimezone(timezone.utc).isoformat()
                            )
                        except Exception as e:
                            print(
                                f"PROVENANCE | WARNING: 'sacct' command not available"
                            )
            else:
                last_line = line.strip()
        try:
            end_time_file = datetime.strptime(last_line, "%Y-%m-%dT%H:%M:%S.%f%z")
            # Next assignation won't be executed if strptime fails
            create_action_properties["endTime"] = end_time_file.replace(
                microsecond=0
            ).isoformat()
        except ValueError:
            print(
                f"PROVENANCE | WARNING: No 'endTime' found in dataprovenance.log. Using current time as 'endTime'"
            )

    try:
        print(f"PROVENANCE | RO-Crate adding statistical data")
        # Add the resource usage to the ROCrate object
        resource_usage_list = get_resource_usage_dataset(dp_log, start_time, end_time)
        id_name_list = []
        for resource_usage in resource_usage_list:
            resource_id = resource_usage["id"]
            del resource_usage["id"]
            compss_crate.add(
                ContextEntity(compss_crate, resource_id, properties=resource_usage)
            )
            id_name_list.append({"@id": resource_id})
        create_action_properties["resourceUsage"] = id_name_list
    except ValueError:
        print(f"PROVENANCE | WARNING: No statistical data found in dataprovenance.log ")

    if agent:
        create_action_properties["agent"] = agent

    create_action = compss_crate.add(
        ContextEntity(compss_crate, create_action_id, create_action_properties)
    )  # id can be something fancy for MN4, otherwise, whatever
    create_action.properties()

    # "subjectOf": {"@id": userportal_url}
    if userportal_url is not None:
        create_action.append_to("subjectOf", userportal_url)

    # "object": [{"@id":}],  # List of inputs
    # "result": [{"@id":}]  # List of outputs
    # Right now neither the COMPSs runtime nor this script check if a file URI is inside a dir URI. This means
    # duplicated entries can be found in the metadata (i.e. a file that is part of a directory, can be added
    # independently). However, this does not add duplicated files if data_persistence is True
    # Hint for controlling duplicates: both 'ins' and 'outs' dir URIs come first on each list
    for item in ins:
        create_action.append_to("object", {"@id": fix_dir_url(item)})
    for item in outs:
        create_action.append_to("result", {"@id": fix_dir_url(item)})
    create_action.append_to("result", {"@id": "./"})  # The generated RO-Crate

    # Add out and err logs in SLURM executions
    if job_id:
        suffix = [".out", ".err"]
        msg = ["output", "error"]
        for f_suffix, f_msg in zip(suffix, msg):
            file_properties = {}
            file_properties["name"] = "compss-" + job_id + f_suffix
            file_properties["contentSize"] = os.path.getsize(file_properties["name"])
            file_properties["description"] = (
                "COMPSs console standard " + f_msg + " log file"
            )
            file_properties["encodingFormat"] = "text/plain"
            file_properties["about"] = create_action_id
            compss_crate.add_file(file_properties["name"], properties=file_properties)

    # Add Paraver trace files if they have been generated in PRV_DIR/ folder
    compss_wf_info = yaml_content["COMPSs Workflow Information"]
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
                file_properties["about"] = create_action_id
                if prv_persist:
                    crate_path = "trace/" + file.name
                    compss_crate.add_file(
                        source=file.resolve(),
                        dest_path=crate_path,
                        properties=file_properties,
                    )
                else:
                    file_url = "file://" + socket.gethostname() + str(file.resolve())
                    compss_crate.add_file(
                        source=file_url,
                        fetch_remote=False,
                        validate_url=False,
                        properties=file_properties,
                    )
    elif prv_persist:
        print(
            f"PROVENANCE | WARNING: PARAVER trace files not found at COMPSs log dir, and trace_persistence is True at the Workflow Provenance YAML file"
        )

    return create_action, agent