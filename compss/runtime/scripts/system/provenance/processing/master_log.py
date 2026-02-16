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
import socket
import time
import typing
import uuid
from pathlib import Path
from provenance.models.Parameter import Parameter
from provenance.models.Task import Task
from provenance.utils.type_mapping import map_datatype
from urllib.parse import urlsplit

STATUS_PRIORITY = {
    "UNKNOWN": -1,
    "CANCELED": 0,
    "FAILED": 1,
    "RECOVERED": 2,
    "FINISHED": 3,
}


def is_future_object(name: str):
    """
    Checks whether the given string format corresponds to a COMPSs future object
    """
    name = name.rsplit("-", 1)[
        0
    ]  # Remove the last trailing character that is added to the UUID
    try:
        val = uuid.UUID(name)
        return True
    except ValueError:
        return False


def process_master_log(dp_log: Path) -> typing.Tuple[list, list, list]:
    """
    Process all the files the COMPSs workflow has accessed. They will be the overall inputs needed and outputs
    generated of the whole workflow.
    - If a task that is an INPUT, was previously an OUTPUT, it means it is an intermediate file, therefore we discard it
    - Works fine with COLLECTION_FILE_IN, COLLECTION_FILE_OUT and COLLECTION_FILE_INOUT

    :param dp_log: Path object to the dataprovenance.log file

    :returns: List of Inputs and Outputs of the COMPSs workflow
    """

    part_time = time.time()

    inputs = set()
    outputs = set()
    tasks = {"master": Task(tid="master", status="FINISHED")}

    task_params: dict[str, dict] = (
        {}
    )  # Ad-hoc. Does not follow Parameter class defined in Parameter.py

    with open(dp_log, "r", encoding="UTF-8") as dp_file:
        for line in dp_file:
            line_record = line.rstrip().split(" ")

            if line_record[0] == "parameter":
                param_name, param_type, param_value, direction = (
                    line_record[1],
                    line_record[2],
                    line_record[3],
                    line_record[4],
                )

                param_name = param_name.replace("#kwarg_", "")

                if param_name not in task_params:
                    task_params[param_name] = {
                        "type": param_type,
                        "direction": direction,
                        "value": param_value,
                        "array": False,
                    }

            if line_record[0] == "file":
                param_name, param_type, param_value, direction = (
                    line_record[1],
                    line_record[2],
                    line_record[3],
                    line_record[4],
                )

                param_name = param_name.replace("#kwarg_", "")

                # Skip individual elements of a collection starting with @
                if "@" in param_name:
                    continue

                if param_name not in task_params:
                    task_params[param_name] = {
                        "type": param_type,
                        "direction": direction,
                        "value": param_value,
                        "array": False,
                    }
                else:
                    existing = task_params[param_name]["value"]
                    if isinstance(existing, list):
                        existing.append(param_value)
                    else:
                        task_params[param_name]["value"] = [existing, param_value]
                    # Force to add "multipleValues" property to COMPSs 'COLLECTION_T' type
                    task_params[param_name]["array"] = True

                # Handle inputs/outputs classification
                if param_type in ["FILE_T", "DIRECTORY_T"] and not is_future_object(
                    param_value
                ):
                    if (
                        line_record[4] == "IN" or line_record[4] == "IN_DELETE"
                    ):  # Can we have an IN_DELETE that was not previously an OUTPUT?
                        if (
                            line_record[3] not in outputs
                        ):  # A true INPUT, not an intermediate file
                            inputs.add(line_record[3])
                        #  Else, it is an intermediate file, not a true INPUT or OUTPUT. Not adding it as an input may
                        # be enough in most cases, since removing it as an output may be a bit radical
                        #     outputs.remove(line_record[0])
                    elif line_record[4] == "OUT":
                        outputs.add(line_record[3])
                    else:  # INOUT, COMMUTATIVE, CONCURRENT
                        if (
                            line_record[3] not in outputs
                        ):  # Not previously generated by another task (even a task using that same file), a true INPUT
                            inputs.add(line_record[3])
                        # else, we can't know for sure if it is an intermediate file, previous call using the INOUT may
                        # have inserted it at outputs, thus don't remove it from outputs
                        outputs.add(line_record[3])

            # -------------------- TASK DEFINITION -------------------- #
            # Here we add all the parameters to the task. Their types and values are not yet final,
            # since we only have the serialized version and the COMPSs type at this point
            if line_record[0] == "task":
                task_id = line_record[1]

                if line_record[2] == "status" and task_id in tasks:
                    new_status = line_record[3]

                    # Priority checking is needed because resubmitted tasks appear with more statuses
                    if (
                        STATUS_PRIORITY[new_status]
                        > STATUS_PRIORITY[tasks[task_id].status]
                    ):
                        tasks[task_id].status = new_status

                    continue

                signature = line_record[2]  # Signature = filename + methodname
                file_name, method_name = line_record[2].rsplit(".", 1)

                current_task = Task(
                    tid=task_id,
                    signature=signature,
                    method=method_name,
                    sourcefile=file_name,
                )

                raw_params = line_record[3].split("::") if line_record[3] else []

                for p in raw_params:
                    pname, ptype, direction = p.split(".")
                    pname = pname.replace("#kwarg_", "")
                    if pname not in task_params:
                        task_params[pname] = {
                            "type": ptype,
                            "direction": direction,
                            "value": "",
                            "array": False,
                        }

                for pname, p_dict in task_params.items():
                    ptype = p_dict["type"]
                    ptype = map_datatype(ptype)
                    direction = p_dict["direction"]
                    pvalue = p_dict["value"]
                    parray = p_dict["array"]

                    if direction in {
                        "IN",
                        "INOUT",
                        "IN_DELETE",
                        "CONCURRENT",
                        "COMMUTATIVE",
                    }:
                        current_task.in_params[pname] = Parameter(
                            name=pname,
                            method=method_name,
                            direction="IN",
                            dtype=ptype,
                            value=pvalue,
                            is_array=parray,
                        )

                    if direction in {"OUT", "INOUT", "CONCURRENT", "COMMUTATIVE"}:
                        current_task.out_params[pname] = Parameter(
                            name=pname,
                            method=method_name,
                            direction="OUT",
                            dtype=ptype,
                            value=pvalue,
                            is_array=parray,
                        )

                tasks[task_id] = current_task

                # Reset for the next task scope
                task_params.clear()

            # -------------------- WORKFLOW STATUS -------------------- #
            if line_record[0] == "master":
                tasks["master"].status = line_record[2]

    l_ins = list(inputs)
    l_ins.sort()  # Put directories first
    l_outs = list(outputs)
    l_outs.sort()  # Put directories first

    # Fix dir:// references, they don't end with slash '/' at dataprovenance.log
    for data_list in [l_ins, l_outs]:
        for i, item in enumerate(data_list):
            url_parts = urlsplit(item)
            if url_parts.scheme == "dir":
                data_list[i] = "dir://" + socket.gethostname() + url_parts.path + "/"
            else:
                break  # File has been reached, all directories have been treated
        data_list.sort()

    print(f"PROVENANCE | COMPSs runtime detected inputs ({len(l_ins)})")
    print(f"PROVENANCE | COMPSs runtime detected outputs ({len(l_outs)})")
    print(
        f"PROVENANCE | dataprovenance.log processing TIME: "
        f"{time.time() - part_time} s"
    )

    return l_ins, l_outs, tasks
