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

from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity


def set_profile_details(compss_crate: ROCrate) -> None:
    """
    Set all the details of the profiles used inside the RO-Crate

    :param compss_crate: The COMPSs RO-Crate being generated

    :returns: None
    """

    PROFILES_BASE = "https://w3id.org/ro/wfrun"
    WRROC_PROFILES_VERSION = "0.5"
    WF_PROFILE_VERSION = "1.0"

    # ro-crate-py does not deal with profiles
    # compss_crate.metadata.append_to(
    #     "conformsTo", {"@id": "https://w3id.org/workflowhub/workflow-ro-crate/1.0"}
    # )

    #  Code from runcrate https://github.com/ResearchObject/runcrate/blob/411c70da556b60ee2373fea0928c91eb78dd9789/src/runcrate/convert.py#L270
    profiles = []

    # In the future, this could go out of sync with the wroc
    # profile added by ro-crate-py to the metadata descriptor
    wroc_profile_id = (
        f"https://w3id.org/workflowhub/workflow-ro-crate/{WF_PROFILE_VERSION}"
    )
    profiles.append(
        compss_crate.add(
            ContextEntity(
                compss_crate,
                wroc_profile_id,
                properties={
                    "@type": "CreativeWork",
                    "name": "Workflow RO-Crate",
                    "version": WF_PROFILE_VERSION,
                },
            )
        )
    )

    # provenance-run not enabled yet
    # for proc in "process", "workflow", "provenance":
    for proc in "process", "workflow":
        id_ = f"{PROFILES_BASE}/{proc}/{WRROC_PROFILES_VERSION}"
        profiles.append(
            compss_crate.add(
                ContextEntity(
                    compss_crate,
                    id_,
                    properties={
                        "@type": "CreativeWork",
                        "name": f"{proc.title()} Run Crate",
                        "version": WRROC_PROFILES_VERSION,
                    },
                )
            )
        )

    compss_crate.root_dataset["conformsTo"] = profiles

    # Add Checksum algorithm and "environment" to context
    compss_crate.metadata.extra_contexts.append(
        "https://w3id.org/ro/terms/workflow-run"
    )
