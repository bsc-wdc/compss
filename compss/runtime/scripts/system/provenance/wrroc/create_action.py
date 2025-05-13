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
import time
import typing
import os
import uuid
import subprocess
import socket
import yaml
import statistics as st
from hashlib import sha256
try:
    import matplotlib.pyplot as plt
except:
    print(
        "Error: matplotlib is not installed. Please install it using 'pip install matplotlib'."
    )
    exit(1)

from pathlib import Path
from datetime import timezone
from datetime import datetime

try:
    import pandas as pd
except:
    print(
        "Error: pandas is not installed. Please install it using 'pip install pandas'."
    )
    exit(1)

from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity
from rocrate.model.entity import Entity

from provenance.utils.url_fixes import fix_dir_url
from provenance.processing.entities import add_person_definition


unit_dict = {
    "TIME_SEC": "https://qudt.org/vocab/unit/SEC",
    "DC_NODE_POWER_W": "https://qudt.org/vocab/unit/W",
    "DRAM_POWER_W": "https://qudt.org/vocab/unit/W",
    "PCK_POWER_W": "https://qudt.org/vocab/unit/W",
    "CPU-GFLOPS": "https://qudt.org/vocab/unit/GigaFLOPS",
    "AVG_CPUFREQ_KHZ": "https://qudt.org/vocab/unit/KiloHZ",
    "AVG_IMCFRQ_KHZ": "https://qudt.org/vocab/unit/KiloHZ",
    "DEF_FREQ_KHZ": "https://qudt.org/vocab/unit/Hz",
    "IO_MBS": "https://qudt.org/vocab/unit/MegaBYTES",
    "MEM_GBS": "https://qudt.org/vocab/unit/GigaBYTES",
}

LANGUAGES_EXTENSION = (".java", ".py", ".sh")


def get_description_plot(metric, node_name="unknown node"):
    description_plots = {
        "cpu": f"Plot of {node_name} showing the percentage of CPU used during the execution",
        "mem": f"Plot of {node_name} showing the amount of memory used during the execution",
        "disk_usage": f"Plot of {node_name} showing the cumulative amount of data read and written on the disk during the execution",
        "network_usage": f"Plot of {node_name} showing the cumulative amount of data sent and received during the execution",
        "cpu_nodes": "Plot of the percentage of CPU used during the execution of all nodes used",
        "mem_nodes": "Plot of the percentage of memory used during the execution of all nodes used",
        # The following plots represent bursts over time and are currently unused
        "bytes_read": "Plot of the amount of data read from the disk during the execution",
        "bytes_written": "Plot of the amount of data written from the disk during the execution",
        "bytes_sent": "Plot of the amount of data sent across the network during the execution",
        "bytes_received": "Plot of the amount of data received across the network during the execution",
    }

    return description_plots[metric]

def process_log(dp_path: str, data_list: list) -> tuple:
    """
    Reads and processes the dataprovenance.log efficiently.

    :param dp_path: pathname of the dataprovenance.log
    :param data_list: list of data to fill with data parsed from dataprovenance.log
    :return: Parsed statistical data
    """
    application_name = None

    with open(dp_path, "r") as data_provenance:
        for idx, row in enumerate(data_provenance):
            row = row.strip()
            if idx == 1:
                application_name = row
            elif idx >= 4 and row:
                parameter_list = row.split()
                if len(parameter_list) >= 4:
                    data_list.append(parameter_list)

    return application_name


def get_stats_list(dp_path: str, start_time: datetime, end_time: datetime) -> list:
    """
    Function that provide a list of the statistical data recorded

    :param dp_path: pathname of the dataprovenance.log
    :param start_time: starting time of the execution
    :param end_time: ending time of the execution

    :return data_list: list of data parsed from dataprovenance.log
    """
    data_list = []
    try:
        init_process_log = time.time()
        application_name = process_log(dp_path, data_list)
        elapsed_process_log = init_process_log - time.time()
        if __debug__:
            print(
                f"Time of reading dataprovenance.log file: {elapsed_process_log:.2f} seconds"
            )

        start_time = start_time.timestamp()
        end_time = end_time.timestamp()
        execution_time = int((end_time - start_time) * 1000)
        app_name = application_name.split(".")[0]
        data_list.append(["overall", app_name, "executionTime", str(execution_time)])
    except TypeError:
        print("PROVENANCE | WARNING: could not retrieve execution time")

    return data_list


def get_properties(id_name: str, stat: str, value: int) -> dict:
    """
    Function that generate a new dictionary of the item

    :param id_name: identifier of the Data Entity that is generated
    :param stat: the name of the parameter
    :param value: value of the parameter passed
    :return: new dictionary containing the properties
    """
    properties = {
        "id": id_name,
        "@type": "PropertyValue",
        "name": stat,
        "propertyID": f"https://w3id.org/ro/terms/compss#{stat}",
    }

    if stat == "executions":
        if value == 0:
            value = None
    else:
        properties["unitCode"] = "https://qudt.org/vocab/unit/MilliSEC"

    properties["value"] = str(value)
    return properties


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
        new_item = get_properties(id_name, stat, value)
        resource_dataset.append(new_item)
    return resource_dataset


def build_info_dict_ear(measure_name: str, value: typing.Union[float, int]) -> dict:
    """
    Build the dictionary of ear property

    :param measure_name: name of metric
    :param value: value of the metric
    :return: dictionary containing the ear property
    """
    properties_item = {
        "@type": "PropertyValue",
        "name": measure_name,
        "value": str(value),
        "propertyID": f"https://w3id.org/ro/terms/compss#{measure_name}",
    }

    if measure_name in unit_dict.keys():
        properties_item["unitCode"] = unit_dict[measure_name]
    elif "DATE" in measure_name:
        properties_item["value"] = datetime.strptime(
            value, "%Y-%m-%d %H:%M:%S"
        ).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    return properties_item


def build_info_dict_resource_usage(
    measure_name: str, value: typing.Union[float, int]
) -> dict:
    """
    Build the dictionary of resource property

    :param measure_name: name of metric
    :param value: value of the metric
    :return: dictionary containing the new resource property
    """
    properties_item = {
        "@type": "PropertyValue",
        "name": measure_name,
        "value": str(value),
        "propertyID": f"https://w3id.org/ro/terms/compss#{measure_name}",
    }

    if "byte" in measure_name:
        properties_item["unitCode"] = "https://qudt.org/vocab/unit/BYTE"
    else:
        properties_item["unitCode"] = "https://qudt.org/vocab/unit/PERCENT"

    return properties_item


def get_energy_usage_for_node(energy_file: str, info_list: list, node: str):
    """
    Build the list containing the energy usage of a node

    :param energy_file: csv file containing the energy data
    :param info_list: list where to add the data containing the energy usage of the node
    :param node: name of the node
    """
    df = pd.read_csv(energy_file, sep=";")
    df = df.rename(columns={"CPU-GFLOPS": "CPU_GFLOPS"})

    df = df[
        [
            "APPID",
            "START_DATE",
            "END_DATE",
            "AVG_CPUFREQ_KHZ",
            "AVG_IMCFREQ_KHZ",
            "DEF_FREQ_KHZ",
            "TIME_SEC",
            "CPI",
            "TPI",
            "MEM_GBS",
            "IO_MBS",
            "DC_NODE_POWER_W",
            "DRAM_POWER_W",
            "PCK_POWER_W",
            "CYCLES",
            "INSTRUCTIONS",
            "CPU_GFLOPS",
            "L1_MISSES",
            "L2_MISSES",
            "L3_MISSES",
        ]
    ]

    for row in df.itertuples(index=True):
        id = f"{getattr(row, 'APPID')}"
        is_appid = True
        for column in df.columns:
            if is_appid:
                is_appid = False
                continue
            info_list.append(
                build_info_dict_ear(node, id, column, getattr(row, column))
            )


def check_resource(path: str):
    """
    Get the list of the csv files contained in the folder

    :param path: pathname of the directory containing the csv files
    :return: list containing the filenames
    """
    list_of_files = []
    for file in os.listdir(path):
        filename = os.fsdecode(file)
        if filename.endswith(".csv"):
            list_of_files.append(filename)
    return list_of_files


def get_resource_information(resource_file: Path) -> dict:
    """
    Get the resource summary data contained in the file

    :param resource_file: csv file containing the data of the node
    :return: dictionary containing the summary data of the node
    """
    resource_df = pd.read_csv(resource_file)
    cpu_avg = round(sum(resource_df["CPU"]) / len(resource_df), 2)
    cpu_max = max(resource_df["CPU"])
    mem_avg = round(sum(resource_df["MEM"]) / len(resource_df), 2)
    mem_min = min(resource_df["MEM"])
    mem_max = max(resource_df["MEM"])
    byte_sent_sum = sum(resource_df["BYTE_SENT"])
    byte_recv_sum = sum(resource_df["BYTE_RECV"])

    resource_properties = {
        "cpuAvg": cpu_avg,
        "cpuMax": cpu_max,
        "memAvg": mem_avg,
        "memMin": mem_min,
        "memMax": mem_max,
        "byteSent": byte_sent_sum,
        "byteRecv": byte_recv_sum,
    }
    return resource_properties


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
    auxiliary_file_list: list,
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
    :param info_yaml: Name of the YAML file specified by the user
    :param log_dir: Path object to the directory where dataprovenance.log file, profiling and trace files can be found
    :param end_time: Time where the COMPSs application execution ended
    :param auxiliary_file_list: list of the auxiliary file contained in the instruments

    :returns: UUID generated for this run
    """
    # Define useful pathnames of file/directory in log directory
    energy_path = log_dir / "energy/"
    stats_path = log_dir / "stats/"
    plots_path = log_dir / "stats/plots/"
    dp_log = log_dir / "dataprovenance.log"

    # Compliance with RO-Crate WorkflowRun Level 2 profile, aka. Workflow Run Crate
    # marenostrum4, nord3, ... BSC_MACHINE would also work
    host_name = os.getenv("SLURM_CLUSTER_NAME")
    if host_name is None:
        host_name = os.getenv("BSC_MACHINE")
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
    # uname = subprocess.run(["uname", "-a"], stdout=subprocess.PIPE, check=True)
    # uname_out = uname.stdout.decode("utf-8")[:-1]  # Remove final '\n'

    description_property = ""

    if os.path.exists(".compss_submission_command_line.txt"):
        with open(".compss_submission_command_line.txt", "r") as file:
            description_property = file.read()[:-1].strip()

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
            # if "COMPSS_PROFILING_INTERVAL" == name:
            #     env_var["unitCode"] = "https://qudt.org/vocab/unit/SEC"
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    "#" + name.lower(),
                    properties=env_var,
                )
            )
            environment_property.append({"@id": "#" + name.lower()})

    resolved_main_entity = main_entity
    for entity in compss_crate.get_entities():
        if "ComputationalWorkflow" in entity.type:
            resolved_main_entity = entity.id

    # Adding profiling plots to RO-Crate
    plots_path = str(plots_path)
    if os.path.exists(plots_path):
        for root, _, files in os.walk(plots_path):
            for file in files:
                if file.endswith(".svg"):
                    full_path = os.path.join(root, file)
                    relative_path = "profiling" + full_path.split("/plots")[1]

                    # Generate a unique ID and path for the file
                    unique_id = "#" + full_path.split("plots/")[1].split(".svg")[
                        0
                    ].replace(
                        "/", "."
                    )  # Replace '/' with '_'

                    metric = relative_path.split("/")[-1].split(".svg")[0]
                    node_name = relative_path.split("/")[-2]

                    # Add the CreateAction entity
                    # action = compss_crate.add(Entity(compss_crate, unique_id, properties={
                    #     '@type': 'CreateAction',
                    #     'instrument': {
                    #         '@id': resolved_main_entity
                    #     },
                    #     'name': f'Profiling plot of {metric}',
                    # }))

                    # Add the trace file with a unique ID and file path
                    trace_file = compss_crate.add_file(
                        full_path,
                        dest_path=relative_path,
                        properties={
                            "@id": relative_path,  # Unique ID for the file
                            "@type": ["File", "ImageObject"],
                            "name": relative_path.split("/")[-1],
                            "description": get_description_plot(metric, node_name),
                            "contentSize": os.stat(full_path).st_size,
                            "encodingFormat": [
                                "image/svg+xml",
                                {
                                    "@id": "https://www.nationalarchives.gov.uk/PRONOM/fmt/91"
                                },
                            ],
                            "about": resolved_main_entity,
                        },
                    )
    else:
        print("Plots folder does not exist")

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

    # instrument_list = []
    # instrument_list.append({"@id": resolved_main_entity})

    # for aux_file in auxiliary_file_list:
    #     instrument_list.append({"@id": aux_file})

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
    with open(dp_log, "r", encoding="UTF-8") as dp_file:
        last_line = ""
        for i, line in enumerate(dp_file):
            if i == 3:
                try:
                    clean_time = line.strip().replace('Z', '+0000')[:26] + '+0000'
                    start_time = datetime.strptime(clean_time, "%Y-%m-%dT%H:%M:%S.%f%z")
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
                                f"PROVENANCE | WARNING: 'sacct' command not available. 'startTime' will be obtained from dataprovenance.log"
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
        stat_data_time = time.time()
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

        # Get profiling data
        try:
            profiling_files_list = check_resource(stats_path)
        except FileNotFoundError:
            profiling_files_list = []
        id_measure_list = []
        if len(profiling_files_list) == 0:
            print(f"PROVENANCE | WARNING: No profiling resource file found ")
        else:
            for profiling_file in profiling_files_list:
                resource_name = profiling_file.split(".")[0].split("_")[-1]
                resource_properties = get_resource_information(
                    stats_path / profiling_file
                )
                resource_id = "#" + resource_name

                for measure in resource_properties.keys():
                    measure_id = f"{resource_id}.{measure}"
                    new_properties = build_info_dict_resource_usage(
                        measure, resource_properties[measure]
                    )
                    compss_crate.add(
                        ContextEntity(
                            compss_crate, measure_id, properties=new_properties
                        )
                    )
                    id_measure_list.append({"@id": measure_id})

        id_name_list.extend(id_measure_list)
        print(f"PROVENANCE | Added resource profiling information TIME: {time.time() - stat_data_time} s")

    except ValueError:
        print(f"PROVENANCE | WARNING: No statistical data found in dataprovenance.log ")

    if os.path.isdir(energy_path):
        try:
            entry_list = [
                "AVG_CPUFREQ_KHZ",
                "AVG_IMCFREQ_KHZ",
                "CPI",
                "TPI",
                "MEM_GBS",
                "IO_MBS",
                "DC_NODE_POWER_W",
                "DRAM_POWER_W",
                "PCK_POWER_W",
                "CYCLES",
                "INSTRUCTIONS",
                "CPU_GFLOPS",
            ]

            id_measure_list = []
            for subdir, dirs, files in os.walk(energy_path):
                for file in files:
                    if file.endswith("time.csv"):
                        energy_file = Path(subdir, file)
                        node = file.split(".")[1]
                        df = pd.read_csv(energy_file, sep=";")
                        # pandas library has problems in column names containing dash
                        df = df.rename(columns={"CPU-GFLOPS": "CPU_GFLOPS"})
                        node_id = df["NODENAME"].iloc[0]
                        df = df[entry_list]

                        for measure in entry_list:
                            average_value = round(st.mean(df[measure]), 2)

                            measure_id = f"#{node_id}.{measure}"
                            new_properties = build_info_dict_ear(measure, average_value)
                            compss_crate.add(
                                ContextEntity(
                                    compss_crate, measure_id, properties=new_properties
                                )
                            )
                            id_measure_list.append({"@id": measure_id})
            id_name_list.extend(id_measure_list)
            print(f"PROVENANCE | RO-Crate adding energy data")
        except ValueError:
            print(
                f"PROVENANCE | WARNING: Error during data retrieving in directory {energy_path}"
            )
            print("PROVENANCE | EAR not used")
    else:
        print("PROVENANCE | EAR not enabled")

    create_action_properties["resourceUsage"] = id_name_list

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

    return run_uuid
