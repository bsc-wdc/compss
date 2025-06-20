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

"""
The generate_COMPSs_RO-Crate.py module generates the resulting RO-Crate metadata from a COMPSs application run
following the Workflow Run Crate profile specification. Takes as parameters the ro-crate-info.yaml, and the
dataprovenance.log generated from the run.
"""
import numpy as np
import numpy as np
import sys
import sys
import time
import time
import yaml
import yaml
from datetime import datetime
from pathlib import Path
from provenance.file_adding.datasets import (
    add_dataset_file_to_crate,
    add_manual_datasets,
)
from provenance.file_adding.source_code import add_application_source_files
from provenance.processing.entities import root_entity, get_main_entities
from provenance.processing.files import process_accessed_files
from provenance.processing.parameters import update_parameter_contents
from provenance.utils.common_paths import get_common_paths
from provenance.utils.url_fixes import fix_in_files_at_out_dirs
from provenance.utils.yaml_template import get_yaml_template
from provenance.wrroc.profile import set_profile_details
from provenance.wrroc.provenance_run.prospective import *
from provenance.wrroc.provenance_run.retrospective import *
from provenance.wrroc.workflow_run.create_action import wrroc_create_action
from rocrate.utils import iso_now


def main():
    """
    Generate an RO-Crate from a COMPSs execution dataprovenance.log file.

    :param None

    :returns: None
    """

    exec_time = time.time()
    yaml_template = get_yaml_template()
    compss_crate = ROCrate()
    end_time = iso_now()

    # First, read values defined by user from ro-crate-info.yaml
    run_uuid = str(uuid.uuid4())
    try:
        with open(INFO_YAML, "r", encoding="utf-8") as f_p:
            try:
                yaml_content = yaml.safe_load(f_p)
            except yaml.YAMLError as exc:
                print(exc)
                raise exc
    except IOError:
        with open("ro-crate-info_TEMPLATE.yaml", "w", encoding="utf-8") as f_t:
            f_t.write(yaml_template)
            print(
                f"PROVENANCE | WARNING: YAML file {INFO_YAML} not found in your working directory. A template"
                " has been generated in file ro-crate-info_TEMPLATE.yaml so you can provide more details on the experiment. "
                "Your run will be recorded with a generated experiment name"
            )
            yaml_content = {
                "COMPSs Workflow Information": {"name": "COMPSs experiment " + run_uuid}
            }

    # Generate Root entity section in the RO-Crate
    # Can update author details from online search
    yaml_content, author_list = root_entity(compss_crate, yaml_content, INFO_YAML)
    if "Updated" in yaml_content:
        # Write updated YAML to disk
        with open("GENERATED_" + INFO_YAML, "w", encoding="utf-8") as f_y:
            del yaml_content["Updated"]
            yaml.dump(yaml_content, f_y, default_flow_style=False)

    compss_wf_info = yaml_content["COMPSs Workflow Information"]

    # Get mainEntity from COMPSs runtime log dataprovenance.log
    compss_ver, main_entity, out_profile, compss_wf_info = get_main_entities(
        compss_wf_info, INFO_YAML, DP_LOG
    )

    # Process set of accessed files, as reported by COMPSs runtime.
    # This must be done before adding the Workflow to the RO-Crate
    ins, outs, tasks_dict = process_accessed_files(DP_LOG)

    # Add application source files to the RO-Crate, that will also be physically in the crate
    add_application_source_files(
        compss_crate,
        compss_wf_info,
        compss_ver,
        main_entity,
        out_profile,
        INFO_YAML,
        COMPLETE_GRAPH,
    )

    # Add in and out files, not to be physically copied in the Crate by default (data_persistence = False)
    # First, add to the lists any inputs or outputs defined by the user, in case they exist
    if "inputs" in compss_wf_info:
        ins = add_manual_datasets("inputs", compss_wf_info, ins, INFO_YAML)
    if "outputs" in compss_wf_info:
        outs = add_manual_datasets("outputs", compss_wf_info, outs, INFO_YAML)

    ins, outs = fix_in_files_at_out_dirs(ins, outs)

    # Merge lists to avoid duplication when detecting common_paths
    ins_and_outs = ins.copy() + outs.copy()
    ins_and_outs.sort()  # Put together shared paths between ins an
    if __debug__:
        print(f"PROVENANCE DEBUG | List of ins and outs: {ins_and_outs}")

    # The list has at this point detected ins and outs, but also added any ins an outs defined by the user
    list_common_paths = []
    part_time = time.time()
    if (
        "data_persistence" in compss_wf_info
        and compss_wf_info["data_persistence"] is True
    ):
        persistence = True
        list_common_paths = get_common_paths(ins_and_outs)
    else:
        persistence = False

    fixed_ins = []  # ins are file://host/path/file, fixed_ins are crate_path/file
    for item in ins:
        fixed_ins.append(
            add_dataset_file_to_crate(
                compss_crate, item, persistence, list_common_paths
            )
        )
    print(
        f"PROVENANCE | RO-Crate adding input files TIME (Persistence: {persistence}): "
        f"{time.time() - part_time} s"
    )

    part_time = time.time()

    fixed_outs = []
    for item in outs:
        fixed_outs.append(
            add_dataset_file_to_crate(
                compss_crate, item, persistence, list_common_paths
            )
        )
    print(
        f"PROVENANCE | RO-Crate adding output files TIME (Persistence: {persistence}): "
        f"{time.time() - part_time} s"
    )
    # print(f"FIXED_INS: {fixed_ins}")
    # print(f"FIXED_OUTS: {fixed_outs}")

    # Register execution details using WRROC profile
    # Compliance with RO-Crate WorkflowRun Level 2 profile, aka. Workflow Run Crate
    # Can update Agent details from online search
    part_time = time.time()
    main_create_action, agent = wrroc_create_action(
        compss_crate,
        main_entity,
        author_list,
        fixed_ins,
        fixed_outs,
        yaml_content,
        INFO_YAML,
        path_log,
        datetime.fromisoformat(end_time),
        run_uuid,
    )
    print(
        f"PROVENANCE | RO-Crate adding CreateAction TIME: "
        f"{time.time() - part_time} s"
    )

    PROVENANCE_RUN_ENABLED = True  # Provenance Run Crate profile is enabled by default
    if "provenance_run" in compss_wf_info:
        if isinstance(compss_wf_info["provenance_run"], bool):
            PROVENANCE_RUN_ENABLED = compss_wf_info["provenance_run"]
        else:
            print(f"PROVENANCE | WARNING: 'provenance_run' in {INFO_YAML} wrongly defined. Reverting to default: True")

    if PROVENANCE_RUN_ENABLED:
        part_time = time.time()
        defined_tools = {}
        steps = []
        step_control_actions = []

        update_parameter_contents(TASK_LOGS, tasks_dict)

        paramTimes = []
        stepTimes = []
        added_formal_params = {}
        work_example_dict = {}

        tempTime = time.time()

        # Process each task
        for task in tasks_dict.values():
            tempTime2 = time.time()

            # Process each parameter of the current task
            for param in task.params:
                # It not already added, add the formal definition of the parameter first (FormalParameter)
                if (task.tid, param.name) in added_formal_params:
                    param.formal_instance = added_formal_params[(task.tid, param.name)]
                else:
                    param.formal_instance = add_parameter_definition(compss_crate, param)
                    added_formal_params[(task.tid, param.name)] = param.formal_instance

                # Then add the actual parameter instance (PropertyValue)
                # Note: Files have already been processed and added previously
                if param.dtype == "File":
                    param.actual_instance = {"@id": param.value}
                else:
                    param.actual_instance = add_parameter_value(compss_crate, param)

                # Collect the FormalParameter - PropertyValue/File cross references for later update
                if param.formal_instance not in work_example_dict:
                    work_example_dict[param.formal_instance] = set()
                work_example_dict[param.formal_instance].add(param.actual_instance["@id"])

            if task.signature not in defined_tools:
                defined_tools[task.signature] = add_software_tool_for_task(compss_crate, task)

            paramTimes.append(time.time() - tempTime2)
            tempTime2 = time.time()

            # Add the information related to the task: HowToStep, CreateAction, ControlAction
            step = add_how_to_step(compss_crate, task, defined_tools[task.signature])
            create_action = add_create_action_for_task(compss_crate, task, defined_tools[task.signature])
            control_action = add_control_action_for_step(compss_crate, step, create_action)
            steps.append(step)
            step_control_actions.append(control_action)

            stepTimes.append(time.time() - tempTime2)

        # Cross reference FormalParameter with PropertyValue:
        # Set the `workExample` key of the FormalParameter with the list of actual parameter instances
        for formal_param, actual_params in work_example_dict.items():
            formal_instance = compss_crate.get(formal_param["@id"])
            work_examples = []
            for actual_param_id in actual_params:
                work_examples.append({"@id": actual_param_id})
            formal_instance.append_to("workExample", work_examples)

        print(
            f"Task processing time: {time.time() - tempTime} s (average: {(time.time() - tempTime) / len(tasks_dict)} s)")
        print(f"Average param processing time: {np.sum(np.array(paramTimes))}")
        print(f"Average step processing time: {np.sum(np.array(stepTimes))}")

        tempTime = time.time()
        update_main_entity_with_software_tools(compss_crate, list(defined_tools.values()))
        update_main_entity_with_steps(compss_crate, steps)
        print(f"Updating main entity time: {time.time() - tempTime} s")

        compss_runtime = add_workflow_engine(compss_crate, compss_ver)
        add_organize_action(
            compss_crate=compss_crate,
            objects=step_control_actions,
            result=main_create_action,
            workflow_engine=compss_runtime,
            agent=agent
        )
        print(
            f"PROVENANCE | RO-Crate adding Provenance Run TIME: "
            f"{time.time() - part_time} s"
        )

    # Set RO-Crate conformance to profiles
    set_profile_details(compss_crate)

    # Debug
    # for e in compss_crate.get_entities():
    #    print(e.id, e.type)

    # Dump to file
    part_time = time.time()
    # folder = "COMPSs_RO-Crate_" + run_uuid + "/"
    sys.stdout.flush()  # All pending stdout to the log file
    compss_crate.write(DEST_FOLDER)
    print(f"PROVENANCE | RO-Crate writing to disk TIME: {time.time() - part_time} s")
    print(
        f"PROVENANCE | Workflow Provenance generation TOTAL EXECUTION TIME: {time.time() - exec_time} s"
    )
    print(
        f"PROVENANCE | COMPSs Workflow Provenance successfully generated in sub-folder:\n\t{DEST_FOLDER}"
    )


def find_all_static_binding_logs(workers_dir: Path):
    # Search for all static_binding_dp.out files including possible suffixes like .1, .2
    return sorted(workers_dir.glob("*/Log/static_binding_dp.out*"))


if __name__ == "__main__":

    # Usage: python /path_to/generate_COMPSs_RO-Crate.py ro-crate-info.yaml /path_to/dataprovenance.log /dest/folder/
    if len(sys.argv) != 4:
        print(
            "PROVENANCE | Usage: python /path_to/generate_COMPSs_RO-Crate.py "
            "/path_to/your_info.yaml /path_to/log_dir/dataprovenance.log /path_to/result_folder/"
        )
        sys.exit()
    else:
        INFO_YAML = sys.argv[1]
        path_log = Path(sys.argv[2])
        DEST_FOLDER = sys.argv[3]
        DP_LOG = path_log / "dataprovenance.log"
        COMPLETE_GRAPH = path_log / "monitor/complete_graph.svg"
        # TASK_LOG = path_log / "workers/localhost/Log" / "static_binding_dp.out"

        # Look for all static_binding_dp.out files across worker folders
        workers_dir = path_log / "workers"
        TASK_LOGS = find_all_static_binding_logs(workers_dir)

        # You can now process each TASK_LOGS file in a loop
        for task_log in TASK_LOGS:
            print(f"Found task log: {task_log}")

    main()
