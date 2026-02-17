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

"""
The generate_COMPSs_RO-Crate.py module generates the resulting RO-Crate metadata from a COMPSs application run
following the Workflow Run Crate profile specification. Takes as parameters the ro-crate-info.yaml, and the
dataprovenance.log generated from the run.
"""
import datetime as dt
import os.path
import uuid
from datetime import timezone
from pathlib import Path

import sys
import time
import yaml
from itertools import chain
from provenance.file_adding.datasets import (
    add_dataset_file_to_crate,
    add_file_to_crate,
    add_manual_datasets,
)
from provenance.file_adding.source_code import add_application_source_files
from provenance.processing.entities import root_entity, get_main_entities
from provenance.processing.master_log import process_master_log
from provenance.processing.worker_logs import update_tasks_from_worker_logs
from provenance.utils.common_paths import get_common_paths, has_files
from provenance.utils.url_fixes import fix_in_files_at_out_dirs
from provenance.utils.yaml_template import get_yaml_template
from provenance.wrroc.create_action import wrroc_create_action
from provenance.wrroc.profile import set_profile_details
from provenance.wrroc.provenance_run.prospective import *
from provenance.wrroc.provenance_run.retrospective import *
from provenance.wrroc.store_data import store_data
from provenance.wrroc.profiling_plots import generate_plots

from rocrate.utils import iso_now

PROVENANCE_RUN_ENABLED = True  # Provenance Run Crate profile is enabled by default
PARAM_SIZE_LIMIT = 200  # Default character limit of parameter values


def main():
    """
    Generate an RO-Crate from a COMPSs execution dataprovenance.log file.

    :param None

    :returns: None
    """
    global PROVENANCE_RUN_ENABLED, PARAM_SIZE_LIMIT
    exec_time = time.time()
    yaml_template = get_yaml_template()
    compss_crate = ROCrate()
    end_time = iso_now()

    generate_plots(STATS_PATH)

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
    compss_ver, main_entity, compss_wf_info = get_main_entities(
        compss_wf_info, INFO_YAML, DP_LOG
    )

    # Process set of accessed files, as reported by COMPSs runtime.
    # This must be done before adding the Workflow to the RO-Crate
    ins, outs, tasks_dict = process_master_log(DP_LOG)

    auxiliary_file_list = []
    # Add application source files to the RO-Crate, that will also be physically in the crate
    add_application_source_files(
        compss_crate,
        compss_wf_info,
        compss_ver,
        main_entity,
        INFO_YAML,
        COMPLETE_GRAPH,
        auxiliary_file_list,
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
    if (
        "data_persistence" in compss_wf_info
        and compss_wf_info["data_persistence"] is True
    ):
        persistence = True
        list_common_paths = get_common_paths(ins_and_outs)
    else:
        persistence = False

    if "provenance_run" in compss_wf_info:
        if isinstance(compss_wf_info["provenance_run"], bool):
            PROVENANCE_RUN_ENABLED = compss_wf_info["provenance_run"]
        else:
            print(
                f"PROVENANCE | WARNING: 'provenance_run' in {INFO_YAML} wrongly defined. "
                "Reverting to default: {PROVENANCE_RUN_ENABLED}"
            )

    if PROVENANCE_RUN_ENABLED and not WORKER_LOGS:
        PROVENANCE_RUN_ENABLED = False
        print(
            "PROVENANCE | WARNING: Missing worker log files. Cannot generate metadata with the Provenance Run Crate"
            "Profile (Level 3). Reverting back to Workflow Run Crate (Level 2)."
        )

    if "param_size_limit" in compss_wf_info:
        if (
            isinstance(compss_wf_info["param_size_limit"], int)
            and int(compss_wf_info["param_size_limit"]) > 0
        ):
            PARAM_SIZE_LIMIT = compss_wf_info["param_size_limit"]
        else:
            print(
                f"PROVENANCE | WARNING: 'param_size_limit' in {INFO_YAML} wrongly defined. "
                f"Reverting to default: {PARAM_SIZE_LIMIT}"
            )

    added_logs = set()
    job_logs_available = has_files(os.path.join(sys.argv[2], "jobs"))
    successful_execution = (
        int(os.environ.get("COMPSS_EXIT_CODE", "0")) == 0
    )  # COMPSS_EXIT_CODE set in compss_setup.sh

    # Process general ins and outs of the workflow
    part_time = time.time()
    fixed_ins = []  # ins are file://host/path/file, fixed_ins are crate_path/file
    for item in ins:
        in_url = add_dataset_file_to_crate(
            compss_crate, item, persistence, list_common_paths
        )
        if in_url:
            fixed_ins.append(in_url)
    print(
        f"PROVENANCE | RO-Crate adding input files TIME (Persistence: {persistence}): "
        f"{time.time() - part_time} s"
    )

    part_time = time.time()
    fixed_outs = []
    for item in outs:
        out_url = add_dataset_file_to_crate(
            compss_crate, item, persistence, list_common_paths
        )
        if out_url:
            fixed_outs.append(out_url)
    print(
        f"PROVENANCE | RO-Crate adding output files TIME (Persistence: {persistence}): "
        f"{time.time() - part_time} s"
    )

    # Compliance with RO-Crate WorkflowRun Level 3 profile, aka. Provenance Run Crate
    if PROVENANCE_RUN_ENABLED:
        pr_part_time1 = time.time()

        update_tasks_from_worker_logs(WORKER_LOGS, tasks_dict)

        steps = []
        step_control_actions = []
        added_formal_params = {}
        defined_tools = {}
        actual_to_formals = {}

        # Process each task
        for task in tasks_dict.values():
            successful_execution &= task.status in {
                "FINISHED",
                "RECOVERED",
            }  # If tasks have been recovered from a checkpoint, the execution worked
            if task.tid == "master":
                continue

            # -------------------- PARAMETER-related ENTITIES -------------------- #

            # Process each parameter of the current task
            params = list(chain(task.in_params.values(), task.out_params.values()))
            for param in params:
                # Add the formal definition of the parameter (FormalParameter); check for duplicates
                if (task.tid, param.name) in added_formal_params:
                    param.formal_instance = added_formal_params[(task.tid, param.name)]
                else:
                    param.formal_instance = add_parameter_definition(
                        compss_crate, param
                    )
                    added_formal_params[(task.tid, param.name)] = param.formal_instance

                # Do not print output for failed tasks
                if not task.status == "FINISHED" and param.direction == "OUT":
                    continue

                # Add the actual parameter value (File/PropertyValue)
                # TODO: Don't try to add the parameter phisically to the crate every time we find it. Keep a separated hash param_in_crate['param_log_id'] and check it first

                # for files:
                if (
                    "File" in param.dtype or "Dataset" in param.dtype
                ):  # and param.is_array == False:
                    # TODO: This bit needs to be rethought, since ALL intermediate files and Datasets for the whole workflow run are added
                    # Right now it added EVERY parameter found, which adds multiple times Files and Datasets to the RO-Crate
                    # The workflow's needed ins and outs have been already added before (Datasets and Files)

                    # Right now only COLLECTION_T COMPSs type maps to Dataset. This may change in the future.
                    # COLLECTION_FILE_XXX maps to [Array, File], not Dataset
                    if "Dataset" in param.dtype:
                        # Ensure that the directory URL ends with '/'
                        if not param.value.endswith("/"):
                            param.value += "/"
                        added_value = add_dataset_file_to_crate(
                            compss_crate, param.value, persistence, list_common_paths
                        )
                    elif "File" in param.dtype and not param.is_array:
                        added_value = add_dataset_file_to_crate(
                            compss_crate, param.value, persistence, list_common_paths
                        )
                    elif "File" in param.dtype and param.is_array:
                        # added_value will be a list for each added file
                        added_value = []
                        for collection_file in param.value:
                            added_value.append(
                                add_dataset_file_to_crate(
                                    compss_crate,
                                    collection_file,
                                    persistence,
                                    list_common_paths,
                                )
                            )
                    else:
                        added_value = None

                    if not added_value:
                        continue
                    param.value = added_value
                    if param.is_array:
                        # Construct the @id of the entity with the new values obtained, for arrays of files
                        param.actual_instance = add_parameter_value(
                            compss_crate, param, PARAM_SIZE_LIMIT
                        )
                    else:
                        param.actual_instance = {"@id": param.value}

                # for regular parameters:
                else:
                    param.actual_instance = add_parameter_value(
                        compss_crate, param, PARAM_SIZE_LIMIT
                    )

                # Collect the FormalParameter - ActualValue relationships
                if param.formal_instance and param.actual_instance:
                    actual_to_formals.setdefault(
                        param.actual_instance["@id"], set()
                    ).add(param.formal_instance["@id"])

            # -------------------- TASK-related ENTITIES -------------------- #

            # Add a SoftwareSourceCode entity representing the method that has been decorated with @task
            if task.signature not in defined_tools:
                defined_tools[task.signature] = add_formal_method_of_task(
                    compss_crate, task
                )

            # Add the information related to the task: HowToStep, CreateAction, ControlAction
            step = add_how_to_step(compss_crate, task, defined_tools[task.signature])
            create_action = add_create_action_for_task(
                compss_crate, task, defined_tools[task.signature]
            )
            control_action = add_control_action_for_step(
                compss_crate, step, create_action
            )

            steps.append(step)
            step_control_actions.append(control_action)

            # If task logs are available, we add them here and give details on which task they belong to
            if job_logs_available and task.logs:
                for filename in task.logs:
                    source = Path(PATH_LOG) / "jobs" / filename
                    add_file_to_crate(compss_crate, source, "logs", task, create_action)
                    added_logs.add(filename)

        # Enforce symmetry between FormalParameter.workExample and ActualValue.exampleOfWork
        for actual_id, formal_ids in actual_to_formals.items():
            actual_entity = compss_crate.get(actual_id)

            for formal_id in formal_ids:
                formal_entity = compss_crate.get(formal_id)

                if actual_entity:
                    actual_entity.append_to("exampleOfWork", {"@id": formal_id})
                if formal_entity:
                    formal_entity.append_to("workExample", {"@id": actual_id})

        pr_part_time1 = time.time() - pr_part_time1

    # Check for the presence of job log files in any case:
    # - Their presence indicates either: failure or debug mode enabled
    # - If debug mode was not enabled and log files were generated, we can assume a failure
    # - Double check that some log files have not been previously added together with their task (if the info was available)

    if job_logs_available:
        logs = (PATH_LOG / "jobs").glob("*")
        for file in logs:
            if file.name not in added_logs:
                add_file_to_crate(crate=compss_crate, source=file, destination="logs")
                added_logs.add(file.name)

    # -------------------- MAIN ENTITY -------------------- #

    # Register execution details using WRROC profile
    # Compliance with RO-Crate WorkflowRun Level 2 profile, aka. Workflow Run Crate
    # Can update Agent details from online search

    part_time = time.time()
    fixed_ins = sorted(fixed_ins)  # Avoid duplicating memory footprint
    fixed_outs = sorted(fixed_outs)  # Avoid duplicating memory footprint
    main_create_action, agent = wrroc_create_action(
        compss_crate,
        main_entity,
        author_list,
        fixed_ins,
        fixed_outs,
        yaml_content,
        INFO_YAML,
        PATH_LOG,
        dt.datetime.fromisoformat(end_time),
        run_uuid,
        auxiliary_file_list,
        successful_execution,
        PROVENANCE_RUN_ENABLED,
    )
    print(
        f"PROVENANCE | RO-Crate adding CreateAction TIME: "
        f"{time.time() - part_time} s"
    )

    if PROVENANCE_RUN_ENABLED:
        pr_part_time2 = time.time()
        update_main_entity_with_formal_methods(
            compss_crate, list(defined_tools.values())
        )
        update_main_entity_with_steps(compss_crate, steps)

        compss_runtime = add_workflow_engine(compss_crate, compss_ver)
        add_organize_action(
            compss_crate=compss_crate,
            objects=step_control_actions,
            result=main_create_action,
            workflow_engine=compss_runtime,
            agent=agent,
        )
        pr_part_time2 = time.time() - pr_part_time2
        print(
            f"PROVENANCE | RO-Crate Provenance Run Crate profile total TIME: "
            f"{pr_part_time1 + pr_part_time2} s"
        )

    # Set RO-Crate conformance to profiles
    set_profile_details(compss_crate, 3 if PROVENANCE_RUN_ENABLED else 2)

    # Debug
    # for e in compss_crate.get_entities():
    #    print(e.id, e.type)

    # Dump to file
    part_time = time.time()
    # folder = "COMPSs_RO-Crate_" + run_uuid + "/"
    sys.stdout.flush()  # All pending stdout to the log file
    if ZIP_PROVENANCE:
        compss_crate.write_zip(f"{DEST_FOLDER.rstrip('/')}.zip")
    else:
        compss_crate.write(DEST_FOLDER)
    store_data(DEST_FOLDER, STATS_PATH, compss_crate)

    print(f"PROVENANCE | RO-Crate writing to disk TIME: {time.time() - part_time} s")
    print(
        f"PROVENANCE | Workflow Provenance generation TOTAL EXECUTION TIME: {time.time() - exec_time} s"
    )
    if ZIP_PROVENANCE:
        print(
            f"PROVENANCE | COMPSs Workflow Provenance successfully generated in ZIP file:\n\t{DEST_FOLDER.rstrip('/')}.zip"
        )
    else:
        print(
            f"PROVENANCE | COMPSs Workflow Provenance successfully generated in sub-folder:\n\t{DEST_FOLDER}"
        )


if __name__ == "__main__":

    # Usage: python /path_to/generate_COMPSs_RO-Crate.py ro-crate-info.yaml /path_to/dataprovenance.log /dest/folder/ zip_bool
    if len(sys.argv) != 5:
        print(
            "PROVENANCE | Usage: python /path_to/generate_COMPSs_RO-Crate.py "
            "/path_to/your_info.yaml /path_to/log_dir/dataprovenance.log /path_to/result_folder/ zip_bool"
        )
        sys.exit()
    else:
        INFO_YAML = sys.argv[1]
        PATH_LOG = Path(sys.argv[2])
        DEST_FOLDER = sys.argv[3]
        ZIP_PROVENANCE = True if sys.argv[4] == "true" else False

        DP_LOG = PATH_LOG / "dataprovenance.log"
        if not DP_LOG.exists() or DP_LOG.stat().st_size == 0:
            print(
                f"PROVENANCE | ERROR: the dataprovenance.log file is empty. Provenance information has not been correctly generated from the COMPSs runtime"
            )
            sys.exit()

        COMPLETE_GRAPH = PATH_LOG / "monitor/complete_graph.svg"
        ENERGY_PATH = PATH_LOG / "energy/"
        STATS_PATH = PATH_LOG / "stats/"
        PLOTS_PATH = PATH_LOG / "stats/plots/"
        # Find all static_binding_dp.out and static_worker_dp.out files in the workers folder recursively:
        WORKER_LOGS = sorted((PATH_LOG / "workers").glob("*/Log/static_*_dp.out*"))

    main()
