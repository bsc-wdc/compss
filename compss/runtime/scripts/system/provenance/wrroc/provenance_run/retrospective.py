import subprocess

from rocrate.model import ContextEntity
from rocrate.rocrate import ROCrate

from provenance.models.Parameter import Parameter
from provenance.models.Task import Task


def add_create_action_for_task(
        compss_crate: ROCrate,
        task: Task,
        tool: tuple
) -> tuple:
    """
    Adds a `CreateAction` instance to the COMPSs RO-Crate for the corresponding `HowToStep`.
    Each `HowToStep` instance will have a corresponding `ControlAction` and `CreateAction` instance.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param task: A dictionary representing the task retrieved from `dataprovenance.log`.
    :param tool:The `SoftwareSourceCode` instance for which the action is being created.

    :return: The created `CreateAction` instance.
    """
    # Get host info
    hostname = subprocess.run(["hostname"], stdout=subprocess.PIPE, check=True)
    hostname_out = hostname.stdout.decode("utf-8").strip()

    create_action_id = f"#Task_{task.tid}_execution_details_{hostname_out}"

    # TODO: add startTime and endTime too
    l_object = []
    l_result = []
    for param in task.params:
        if "OUT" in param.direction:
            l_result.append(param.actual_instance)
        if "IN" in param.direction or param.direction in ["CONCURRENT", "COMMUTATIVE"]:
            l_object.append(param.actual_instance)

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=create_action_id,
        properties={
            "@type": "CreateAction",
            "instrument": tool,
            "name": f"Run of Task {task.tid} at host {hostname_out}",
            "object": l_object,
            "result": l_result
        }
    ))


def add_control_action_for_step(
        compss_crate: ROCrate,
        step: tuple,
        create_action: tuple
) -> tuple:
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
            "instrument": step,
            "object": create_action,
        }
    ))


def add_organize_action(
        compss_crate: ROCrate,
        objects: list,
        result: tuple,
        workflow_engine: tuple
):
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
    hostname = subprocess.run(["hostname"], stdout=subprocess.PIPE, check=True)
    hostname_out = hostname.stdout.decode("utf-8").strip()

    organize_action_id = "#COMPSs_runtime_invocation_at_" + hostname_out

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=organize_action_id,
        properties={
            "@type": "OrganizeAction",
            "instrument": workflow_engine,
            "object": objects,
            "result": result,
            "name": f"Run of COMPSs runtime version {workflow_engine['softwareVersion']}",
            "actionStatus": {
                "@id": "http://schema.org/CompletedActionStatus"  ## MAY (optional)
            }
        }
    ))


def add_how_to_step(
        compss_crate: ROCrate,
        task: Task,
        tool: tuple
) -> tuple:
    """
    Adds a `HowToStep` instance to the COMPSs RO-Crate.
    This step represents an executed task.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param task: The executed task that this step represents.
    :param tool: The `SoftwareSourceCode` instance that this step serves as an example of.

    :return: The created `HowToStep` instance.
    """
    how_to_step_id = f"#Task_{task.tid}"

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=how_to_step_id,
        properties={
            "@type": "HowToStep",
            "workExample": tool,
            "position": task.tid
        }
    ))


def update_main_entity_with_steps(
        compss_crate: ROCrate,
        steps: list
):
    """
    Updates the main entity with a `step` section that includes the executed `HowToStep`s.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param steps: A list of the executed `HowToStep` instances

    :return: The updated mainEntity with the steps.
    """
    main_entity_id = compss_crate.mainEntity.get("@id")
    old_types = compss_crate.mainEntity.get("@type")

    compss_crate.update_jsonld({
        "@id": main_entity_id,
        "@type": old_types.append("HowTo"),
    })
    compss_crate.mainEntity["step"] = steps

    return compss_crate.mainEntity


def add_parameter_value(compss_crate: ROCrate, param: Parameter):
    """
    Adds a `PropertyValue` entity to the crate, representing the actual parameter that the method was invoked with.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param param: Object containing all relevant information about the parameter being inserted.

    :return: The created PropertyValue instance.
    """
    property_value_id = f"#{param.method}#{param.name}#{param.value}"

    return compss_crate.add(ContextEntity(
        crate=compss_crate,
        identifier=property_value_id,
        properties={
            "@type": "PropertyValue",
            "exampleOfWork": param.formal_instance,
            "value": param.value
        }
    ))


def update_main_create_action(
        create_action: tuple,
        objects: list,
        results: list
) -> tuple:
    """
    Appends to the `object` and `result` section of the provided `CreateAction` instance.

    :param create_action: The main CreateAction instance of the RO-Crate.
    :param objects: List of instances of the actual input parameters.
    :param results: List of instances of the return values.
    :return:
    """
    create_action.append_to("object", objects)
    create_action.append_to("result", results)
    return create_action