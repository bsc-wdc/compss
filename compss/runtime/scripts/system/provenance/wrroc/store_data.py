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
from pathlib import Path

# import pymongo
import json
from rocrate.rocrate import ROCrate


def write_data_db(data, name_coll):
    """
    Store the data in the database

    :param data: data to store in the database
    :param name_coll: name of application which define the collection where to store in the database
    :return:
    """
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    db = client["compss-stats"]
    collection = db[name_coll]

    try:
        collection.insert_one(data)
        print(
            "PROVENANCE | Data about the execution stored in database:'compss-stats', collection: '%s'"
            % name_coll
        )
    except:
        print(
            "PROVENANCE | ERROR Could not store the data in the database. Connection with the database not established"
        )


def write_data_local_db(data, application_name):
    """
    Write the local data in the local file which store all the past execution data of the application

    :param data: new data to store in the local file
    :param application_name: name of the application (to use in the local file)
    :return:
    """
    local_file = ""  # Pathname of local file

    try:
        with open(
            local_file,
            "r",
        ) as json_object:
            content = json.load(json_object)
    except FileNotFoundError:
        content = []

    content.append(data)

    with open(
        local_file,
        "w",
    ) as json_object:
        json.dump(content, json_object, indent=4)


def store_data(compss_path: str, stats_path: Path, crate: ROCrate):
    """
    Store the data in the stats.json file

    :param compss_path: path of the compss folder containing the new provenance generated
    :param stats_path: path of the stats folder in the log directory
    :return:
    """
    forbidden_types = [
        "Dataset",
        "CreativeWork",
        "[",
        "File",
        "WebSite",
        "CreativeWork",
    ]
    execution_stats = ["maxTime", "executions", "avgTime", "minTime", "executionTime"]
    profiling_stats = [
        "cpuMax",
        "cpuAvg",
        "memMax",
        "memAvg",
        "memMin",
        "memAvg",
        "byteSent",
        "byteRecv",
    ]

    application_name = ""
    final_dict = {}

    stat_dict = {}
    funct_dict = {}

    nodes = []

    command_launched = "not found"
    for e in crate.contextual_entities:
        if "#COMPSs_Workflow_Run" in str(e):
            command_launched = str(e.properties().get("description"))
            break

    final_dict["Command"] = command_launched

    command_launched = "not found"
    for e in crate.contextual_entities:
        if "#COMPSs_Workflow_Run" in str(e):
            command_launched = str(e.properties().get("description"))
            break

    final_dict["Command"] = command_launched

    command_launched = 'not found'
    for e in crate.contextual_entities:
        if "#COMPSs_Workflow_Run" in str(e):
            command_launched = str(e.properties().get('description'))
            break

    final_dict['Command'] = command_launched

    for e in crate.contextual_entities:
        if not (str(e.type) in forbidden_types or type(e.type) is list):
            if e.type == "ContactPoint":
                final_dict["ContactPoint"] = e.id
            elif e.type == "Organization":
                final_dict["OrganizationName"] = e.get("name")
                final_dict["OrganizationROR"] = e.id
            elif e.type == "Person":
                final_dict["AgentName"] = e.get("name")
                final_dict["AgentROR"] = e.id
            elif e.type == "PropertyValue":
                entry_name = e.get("name")
                if entry_name in profiling_stats:
                    id_stat = e.id.replace("#", "").split(".")
                    node_list = final_dict.keys()
                    for n in node_list:
                        if id_stat[0] in n:
                            if n not in final_dict:
                                final_dict[n] = {}
                            final_dict[n][id_stat[1]] = e.get("value")

                elif entry_name in execution_stats:
                    id_stat = e.id.replace("#", "").split(".")
                    node = id_stat[0]
                    nodes.append(node)
                    if len(id_stat) > 3:
                        function_name = id_stat[1] + "." + id_stat[2]
                        stat = id_stat[3]
                    else:
                        function_name = id_stat[1]
                        stat = id_stat[2]

                    if entry_name == "executionTime":
                        final_dict["AppName"] = function_name
                    elif entry_name == "maxTime":
                        # Initialize every time it reads maxTime because the stats of a new node will be read
                        stat_dict = {}
                        funct_dict = {}

                    value = e.get("value")
                    if value == "None":
                        value = None

                    stat_dict[stat] = value
                    funct_dict[function_name] = stat_dict
                    if node not in final_dict:
                        final_dict[node] = {}
                    final_dict[node].update(funct_dict)
                else:
                    final_dict[e.get("name")] = e.get("value")
            elif e.type == "CrateAction":
                final_dict["ExecutionID"] = e.id

    nodes = set(nodes)
    nodes.remove("overall")
    num_nodes = len(nodes)

    for stat in profiling_stats:
        if "avg" in stat.lower():
            sum_value = 0
            count = 0
            for n in nodes:
                if stat in final_dict.get(n, {}):  # Check if the key exists
                    sum_value += float(final_dict[n][stat])
                    count += 1
            if count > 0:  # Avoid division by zero
                final_dict["overall"][stat] = str(round(sum_value / count, 2))
            else:
                final_dict["overall"][stat] = None  # or some default value
        elif "min" in stat.lower():
            min_value = float("inf")
            for n in nodes:
                if stat in final_dict.get(n, {}):  # Check if the key exists
                    current_value = float(final_dict[n][stat])
                    min_value = (
                        current_value if current_value < min_value else min_value
                    )
            final_dict["overall"][stat] = (
                min_value if min_value != float("inf") else None
            )  # or some default value
        elif "max" in stat.lower():
            max_value = float("-inf")
            for n in nodes:
                if stat in final_dict.get(n, {}):  # Check if the key exists
                    current_value = float(final_dict[n][stat])
                    max_value = (
                        current_value if current_value > max_value else max_value
                    )
            final_dict["overall"][stat] = (
                max_value if max_value != float("-inf") else None
            )  # or some default value
        else:
            total = 0
            for n in nodes:
                if stat in final_dict.get(n, {}):  # Check if the key exists
                    total += int(final_dict[n][stat])
            final_dict["overall"][stat] = str(total)

    # NOT necessary anymore
    # os.makedirs(stats_path, exist_ok=True)

    with open(stats_path / "stats.json", "w") as out_json:
        json.dump(final_dict, out_json, indent=4)

    # write_data_local_db(final_dict, application_name)
