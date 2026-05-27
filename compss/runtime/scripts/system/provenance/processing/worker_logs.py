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
import re
from datetime import datetime

from models.Task import Task
from utils.type_mapping import map_datatype, map_direction


def parse_log_line(line):
    """
    Parses a log line into a dictionary of key-value pairs.

    Each key is expected to be uppercase (with underscores allowed), and
    each value is captured until the next key or the end of the line.

    Example:
        parse_log_line("TASK=1 STATUS=SUCCESS MESSAGE=All done")
        {'TASK': '1', 'STATUS': 'SUCCESS', 'MESSAGE': 'All done'}
    """
    pattern = r"([A-Z_]+)=(.*?)(?=\s[A-Z_]+=|$)"
    matches = re.findall(pattern, line)
    return {k: v.strip() for k, v in matches}


def update_tasks_from_worker_logs(log_files: list[str], tasks: dict[int, Task]) -> None:
    """
    Updates the parameter values and other task information (start time, end time, log files)
    from the workers logs (static_binding_dp.log).

    :param log_files: List of the static_binding_dp.out files.
    :param tasks:     Dictionary of tasks that is the result of the dataprovenance.log processing.

    :return: None. The tasks parameter dict will be automatically updated.
    """
    for log_file in log_files:
        if __debug__:
            print(f"PROVENANCE DEBUG | Reading log file: {log_file}")
        with open(log_file, "r", encoding="UTF-8") as dp_file:
            for line in dp_file:
                if line.strip() == "Empty file":
                    print(
                        f"PROVENANCE | WARNING: empty log file {log_file}"
                    )
                    return
                try:
                    log_dict = parse_log_line(line)
                    task_id = log_dict["TASK"]
                    current_task = tasks.get(task_id)

                    if not current_task:
                        print(f"PROVENANCE | WARNING: No task found for ID {task_id}")
                        continue

                    if "PARAMETER" in log_dict:
                        # Note: - We only extract the parameter content from here, because the rest of the data
                        #         (type, accessMode, ...) are not necessarily accurate
                        #       - The content should not be updated in case of real files
                        #         since these already contain the normalized path

                        hostname = log_dict["HOST"]
                        param_name = log_dict["PARAMETER"].replace("#kwarg_", "")
                        param_direction = map_direction(log_dict["DIRECTION"])
                        deserialized_value = log_dict["CONTENT"]
                        ptypes = log_dict["BASICTYPE"].strip(",").split(",")

                        if not current_task.host:
                            current_task.host = hostname

                        if param_direction == "IN":
                            param = current_task.in_params.get(param_name, {})
                        else:
                            param = current_task.out_params.get(param_name, {})

                        if not param:
                            continue

                        if "IS_ARRAY" in log_dict:
                            param.is_array = (
                                False if log_dict["IS_ARRAY"] == "False" else True
                            )
                        if "DESCRIPTION" in log_dict:
                            param.description = log_dict["DESCRIPTION"]

                        # If it's a file, we have two options:
                        #   1. it's either a real file, in which case the type is already updated
                        #   2. it's a serialized object, in which case we update the type and the value

                        if (
                            "str" not in log_dict["BASICTYPE"]
                            and "String" not in log_dict["BASICTYPE"]
                        ) or not any(x in param.dtype for x in ("File", "Dataset")):
                            schema_type = map_datatype(ptypes[0])
                            param.dtype = [schema_type] + ptypes
                            param.value = deserialized_value

                    elif "STARTTIME" in log_dict:
                        current_task.starttime = datetime.strptime(
                            log_dict["STARTTIME"].strip(), "%Y-%m-%dT%H:%M:%S.%f%z"
                        ).isoformat()
                    elif "ENDTIME" in log_dict:
                        current_task.endtime = datetime.strptime(
                            log_dict["ENDTIME"].strip(), "%Y-%m-%dT%H:%M:%S.%f%z"
                        ).isoformat()
                    elif "LOGFILE" in log_dict:
                        filename = log_dict["LOGFILE"].strip().split("/")[-1]
                        current_task.logs.append(filename)

                except Exception as e:
                    print(
                        f"PROVENANCE | ERROR: Failed to process line in {log_file}. {e}"
                    )
