import re

from models.Task import Task
from utils.type_mapping import map_datatype, map_direction


def parse_log_line(line):
    # Match KEY=VALUE where KEY is ALL CAPS and VALUE ends just before the next KEY=
    pattern = r'([A-Z_]+)=(.*?)(?=\s[A-Z_]+=|$)'

    matches = re.findall(pattern, line)
    parsed = {k: v.strip() for k, v in matches}

    return parsed


def update_parameter_contents(
        log_files: [str],
        tasks: dict[int, Task]
) -> None:
    """
    Updates the values of the task parameters with the deserialized version found in the input file.

    :param log_file: Path to the static_binding_dp.out file.
    :param tasks: Dictionary of tasks that is the result of the dataprovenance.log processing.

    :return: None. The tasks parameter dict will be automatically updated.
    """
    buffer = ""
    for log_file in log_files:
        print(f"Reading log file: {log_file}")
        with (open(log_file, "r", encoding="UTF-8") as dp_file):
            for line in dp_file:
                log_dict = parse_log_line(line)
                if "PARAMETER" in log_dict:
                    task_id = log_dict["TASK"]
                    hostname = log_dict["HOST"]
                    param_name = log_dict["PARAMETER"]
                    param_direction = map_direction(log_dict["DIRECTION"])
                    deserialized_value = log_dict["CONTENT"]

                    # Note: we only extract the content from here, because the rest of the data (type, direction, ...)
                    #       are not necessarily accurate

                    current_task = tasks.get(task_id)
                    if current_task:
                        if not current_task.host:
                            current_task.host = hostname
                        for param in current_task.params:
                            if param.name == param_name and param.direction == param_direction:
                                if "IS_ARRAY" in log_dict:
                                    param.isArray = log_dict["IS_ARRAY"]
                                if "DESCRIPTION" in log_dict:
                                    param.description = log_dict["DESCRIPTION"]

                                ptypes = log_dict["PYTHONTYPE"].strip(",").split(",")

                                # The content should not be updated in case of real files
                                # since these already contain the normalized path
                                if "FILE_T" in param.dtype:
                                    # If it's a file, we have two options:
                                    #   it's either a real file, in which case we only update the type
                                    if "str" in log_dict["PYTHONTYPE"]:
                                        # TODO: find a better way to check if it's a file or not, this will not work in case of string lists ?
                                        param.dtype = map_datatype(param.dtype)
                                    #   or it's a serialized object, in which case we update the value and type
                                    else:
                                        param.value = deserialized_value
                                        param.dtype = [map_datatype(ptypes[0])]
                                        param.dtype.extend(ptypes)
                                elif "COLLECTION_T" in param.dtype:
                                    param.value = deserialized_value
                                    param.dtype = [map_datatype(ptypes[0])]
                                    param.dtype.extend(ptypes)
                                else:
                                    param.value = deserialized_value
                                    # param.dtype = [map_datatype(param.dtype)]
                                    param.dtype = [map_datatype(ptypes[0])]
                                    param.dtype.extend(ptypes)

                                # TODO: collapse similar branches
                    else:
                        print("No Task found with id=", task_id)
