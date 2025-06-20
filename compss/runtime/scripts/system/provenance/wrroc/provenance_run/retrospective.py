import hashlib
import os
import socket
import subprocess

from rocrate.model import ContextEntity, Entity
from rocrate.rocrate import ROCrate

from provenance.models.Parameter import Parameter
from provenance.models.Task import Task


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
    :param tool:The `SoftwareSourceCode` instance for which the action is being created.

    :return: The created `CreateAction` instance.
    """

    create_action_id = f"#Task_{task.tid}_execution_details_at_{task.host}"

    # TODO: add startTime and endTime too
    l_object = []
    l_result = []
    for param in task.params:
        if "OUT" in param.direction:
            l_result.append(param.actual_instance)
        if "IN" in param.direction:
            l_object.append(param.actual_instance)

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=create_action_id,
        properties={
            "@type": "CreateAction",
            "instrument": tool,
            "name": f"Run of Task {task.tid} at host {task.host}",
            "object": l_object,
            "result": l_result
        }
    ))


def add_control_action_for_step(
        compss_crate: ROCrate,
        step: ContextEntity,
        create_action: ContextEntity
) -> ContextEntity:
    """
    Adds a `ControlAction` instance to the COMPSs RO-Crate as part of the WorkflowRun Level 3 profile (Provenance Run Crate).
    Each `HowToStep` instance will have a corresponding `ControlAction` and `CreateAction` instance.

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
            "name": f"Orchestrator of {step['@id']}",
            "instrument": step,
            "object": create_action,
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

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=organize_action_id,
        properties={
            "@type": "OrganizeAction",
            "instrument": workflow_engine,
            "agent": agent,
            "object": objects,
            "result": result,
            "name": f"Run of COMPSs runtime version {workflow_engine['softwareVersion']}",
            "actionStatus": {
                "@id": "http://schema.org/CompletedActionStatus"  ## MAY (optional)
            }
        }
    ))


def add_parameter_value(compss_crate: ROCrate, param: Parameter) -> ContextEntity:
    """
    Adds a `PropertyValue` entity to the crate, representing the actual parameter that the method was invoked with.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param param: Object containing all relevant information about the parameter being inserted.

    :return: The created PropertyValue instance.
    """
    if param.isArray and len(param.value) > 200:
        # If the value is too long, we hash it and use the hash value in the ID and shorten the value itself
        hashcode = hashlib.shake_256(param.value.encode()).hexdigest(5)
        property_value_id = f"#{param.method}::{param.name}-{hashcode}"
        param.value = f"{param.value[:100]} ... {param.value[-100:]}"
    else:
        property_value_id = f"#{param.method}::{param.name}={param.value}"

    properties = {
        "@type": "PropertyValue",
        "exampleOfWork": param.formal_instance,
        "value": param.value
    }

    if param.description:
        properties["description"] = param.description

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=property_value_id,
        properties=properties
    ))
