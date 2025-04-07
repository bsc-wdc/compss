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
from datetime import datetime
from pathlib import Path

import yaml
import time
import sys

from rocrate.rocrate import ROCrate
from rocrate.utils import iso_now

from provenance.utils.url_fixes import fix_in_files_at_out_dirs
from provenance.utils.common_paths import get_common_paths
from provenance.utils.yaml_template import get_yaml_template
from provenance.processing.entities import root_entity, get_main_entities
from provenance.processing.files import process_accessed_files
from provenance.file_adding.source_code import add_application_source_files
from provenance.file_adding.datasets import (
    add_dataset_file_to_crate,
    add_manual_datasets,
)
from provenance.wrroc.create_action import wrroc_create_action
from provenance.wrroc.profile import set_profile_details
from provenance.wrroc.store_data import store_data
from provenance.wrroc.profiling_plots import generate_plots


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

    generate_plots(STATS_PATH)

    # First, read values defined by user from ro-crate-info.yaml
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
                f"PROVENANCE | ERROR: YAML file {INFO_YAML} not found in your working directory. A template"
                " has been generated in file ro-crate-info_TEMPLATE.yaml"
            )
        raise

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
    ins, outs = process_accessed_files(DP_LOG)

    auxiliary_file_list = []
    # Add application source files to the RO-Crate, that will also be physically in the crate
    add_application_source_files(
        compss_crate,
        compss_wf_info,
        compss_ver,
        main_entity,
        out_profile,
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
    run_uuid = wrroc_create_action(
        compss_crate,
        main_entity,
        author_list,
        fixed_ins,
        fixed_outs,
        yaml_content,
        INFO_YAML,
        path_log,
        datetime.fromisoformat(end_time),
        auxiliary_file_list,
    )
    print(
        f"PROVENANCE | RO-Crate adding CreateAction TIME: "
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
    store_data(DEST_FOLDER, STATS_PATH, compss_crate)

    print(f"PROVENANCE | RO-Crate writing to disk TIME: {time.time() - part_time} s")
    print(
        f"PROVENANCE | Workflow Provenance generation TOTAL EXECUTION TIME: {time.time() - exec_time} s"
    )
    print(
        f"PROVENANCE | COMPSs Workflow Provenance successfully generated in sub-folder:\n\t{DEST_FOLDER}"
    )


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
        ENERGY_PATH = path_log / "energy/"
        STATS_PATH = path_log / "stats/"
        PLOTS_PATH = path_log / "stats/plots/"
    main()
