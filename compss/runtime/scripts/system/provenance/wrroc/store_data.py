#!/usr/bin/python
#
#  Copyright 2002-2023 Barcelona Supercomputing Center (www.bsc.es)
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
import os
from pathlib import Path

# import pymongo
import json
from rocrate.rocrate import ROCrate


def write_data_db(data, name_coll):
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
    try:
        with open(
                f"/home/ngiacomi/Documents/applications-test/ProvenanceStats/{application_name}.json",
                "r",
        ) as json_object:
            content = json.load(json_object)
    except FileNotFoundError:
        content = []

    content.append(data)

    with open(
            f"/home/ngiacomi/Documents/applications-test/ProvenanceStats/{application_name}.json",
            "w",
    ) as json_object:
        json.dump(content, json_object, indent=4)


def store_data(compss_path: str, stats_path: Path):
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

    crate = ROCrate(compss_path)

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
                            final_dict[n][id_stat[1]] = e.get("value")

                elif entry_name in execution_stats:
                    id_stat = e.id.replace("#", "").split(".")
                    node = id_stat[0]
                    nodes.append(node)
                    # function_name = id_stat[1] + "." + id_stat[2]
                    function_name = id_stat[1]
                    stat = id_stat[2]
                    # stat = id_stat[3]

                    if entry_name == "executionTime":
                        application_name = function_name.split(".")[0]
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
                    final_dict[node] = funct_dict
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
            for n in nodes:
                sum_value += float(final_dict[n][stat])
            final_dict["overall"][stat] = str(round(sum_value / num_nodes, 2))
        elif "min" in stat.lower():
            min_value = float("inf")
            for n in nodes:
                min_value = (
                    final_dict[n][stat]
                    if float(final_dict[n][stat]) < float(min_value)
                    else min_value
                )
            final_dict["overall"][stat] = min_value
        elif "max" in stat.lower():
            max_value = float("-inf")
            for n in nodes:
                max_value = (
                    final_dict[n][stat]
                    if float(final_dict[n][stat]) > float(max_value)
                    else max_value
                )
            final_dict["overall"][stat] = max_value
        else:
            total = 0
            for n in nodes:
                total += int(final_dict[n][stat])
            final_dict["overall"][stat] = str(total)

    # NOT necessary anymore
    # os.makedirs(stats_path, exist_ok=True)

    with open(stats_path / "stats.json", "w") as out_json:
        json.dump(final_dict, out_json, indent=4)

    # write_data_local_db(final_dict, application_name)
