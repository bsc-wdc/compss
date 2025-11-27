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
import hashlib
import os
import socket

from provenance.models.Parameter import Parameter
from provenance.models.Task import Task
from rocrate.model import ContextEntity, Entity
from rocrate.rocrate import ROCrate

action_status_dict = {
    True: "http://schema.org/CompletedActionStatus",
    False: "http://schema.org/FailedActionStatus"
}


def add_workflow_engine(
        compss_crate: ROCrate,
        version: str
) -> ContextEntity:
    """
    Adds a `SoftwareApplication` entity, representing the COMPSs runtime, to the COMPSs RO-Crate.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param version: The COMPSs version used.

    :return: The created SoftwareApplication instance.
    """
    return compss_crate.add(ContextEntity(
        compss_crate,
        "#COMPSs_runtime",
        {
            "@type": "SoftwareApplication",
            "name": "COMPSs Runtime Environment",
            "softwareVersion": version,
            "url": "http://compss.bsc.es/"
        }
    ))


def add_create_action_for_task(
        compss_crate: ROCrate,
        task: Task,
        tool: Entity
) -> ContextEntity:
    """
    Adds a `CreateAction` instance to the COMPSs RO-Crate for the corresponding `HowToStep`.
    Each `HowToStep` instance will have a corresponding `ControlAction` and `CreateAction` instance.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param task: A dictionary representing the task retrieved from `dataprovenance.log`.
    :param tool: The `SoftwareSourceCode` instance for which the action is being created.

    :return: The created `CreateAction` instance.
    """

    create_action_id = f"#Task_{task.tid}_execution_details"

    l_object = [param.actual_instance for param in task.in_params.values() if param.actual_instance]

    if task.succeeded:
        l_result = [param.actual_instance for param in task.out_params.values() if param.actual_instance]
    else:
        # If the task failed, we include its logs instead of the OUT parameters as results
        l_result = [{"@id": "logs/" + filename} for filename in task.logs if filename]

    properties = {
        "@type": "CreateAction",
        "instrument": tool,
        "actionStatus": {
            "@id": action_status_dict[task.succeeded]  ## MAY (optional)
        }
    }

    if task.starttime: properties["startTime"] = task.starttime
    if task.endtime: properties["endTime"] = task.endtime
    if l_object: properties["object"] = l_object
    if l_result: properties["result"] = l_result
    if task.host:
        properties["name"] = f"Run of Task {task.tid} at host {task.host}"
    elif not task.succeeded:
        properties["name"] = f"Failed execution of Task {task.tid}"

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=create_action_id,
        properties=properties
    ))


def add_control_action_for_step(
        compss_crate: ROCrate,
        step: ContextEntity,
        create_action: ContextEntity
) -> ContextEntity:
    """
    Adds a `ControlAction` instance to the COMPSs RO-Crate, representing the orchestrator of a task.
    Each `HowToStep` (task) instance will have a corresponding `ControlAction` and `CreateAction` instance.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param step: The `HowToStep` instance for which the `ControlAction` is being generated.
    :param create_action: The `CreateAction` instance that describes the execution of this step.

    :return: The created `ControlAction` instance.
    """
    control_action_id = f"{step['@id']}_execution_record"

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=control_action_id,
        properties={
            "@type": "ControlAction",
            "name": f"Orchestration of {step['@id']}",
            "instrument": step,
            "object": create_action,
            "actionStatus": {
                "@id": create_action.get("actionStatus")  ## MAY (optional)
            }
        }
    ))


def add_organize_action(
        compss_crate: ROCrate,
        objects: list[Entity],
        result: ContextEntity,
        workflow_engine: ContextEntity,
        agent: dict
) -> ContextEntity:
    """
    Adds an `OrganizeAction` instance to the COMPSs RO-Crate.
    This action represents the orchestration of workflow steps, linking the COMPSs runtime and the `ControlAction` instances to the main `CreateAction` of the workflow.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param objects: A list of all `ControlAction` instances that orchestrate the workflow steps.
    :param result: The main `CreateAction` instance of the workflow.
    :param workflow_engine: The `SoftwareApplication` instance representing the COMPSs runtime.

    :return: The created `OrganizeAction` instance.
    """
    # Get host info
    hostname = os.getenv("SLURM_CLUSTER_NAME")
    if hostname is None:
        hostname = os.getenv("BSC_MACHINE")
        if hostname is None:
            hostname = socket.gethostname()

    organize_action_id = "#COMPSs_runtime_invocation_at_" + hostname

    properties = {
        "@type": "OrganizeAction",
        "instrument": workflow_engine,
        "object": objects,
        "result": result,
        "name": f"Run of COMPSs runtime version {workflow_engine['softwareVersion']}",
        "actionStatus": {
            "@id": result.get("actionStatus")
        }
    }

    if agent: properties["agent"] = agent

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=organize_action_id,
        properties=properties
    ))


def add_parameter_value(compss_crate: ROCrate, param: Parameter, character_limit: int) -> ContextEntity:
    """
    Adds a `PropertyValue` entity to the crate, representing the actual parameter that the method was invoked with.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param param: Object containing all relevant information about the parameter being inserted.
    :param character_limit: Maximum number of characters for printing the parameter value.

    :return: The created PropertyValue instance.
    """
    if param.is_array and len(param.value) > character_limit:
        # If the value is too long, we hash it and use the hash value in the ID and shorten the value itself
        half_limit = character_limit // 2
        hashcode = hashlib.shake_256(param.value.encode()).hexdigest(5)
        property_value_id = f"#{param.method}::{param.name}-{hashcode}"
        param.value = f"{param.value[:half_limit]} ... {param.value[-half_limit:]}"
    else:
        property_value_id = f"#{param.method}::{param.name}={param.value}"

    properties = {
        "@type": "PropertyValue",
        "exampleOfWork": param.formal_instance,
        "value": param.value
    }

    if param.description: properties["description"] = param.description

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=property_value_id,
        properties=properties
    ))


def set_entity_success(compss_crate, entity, status):
    return compss_crate.update_jsonld({
        "@id": entity.id,
        "actionStatus": {"@id": action_status_dict[status]}
    })
