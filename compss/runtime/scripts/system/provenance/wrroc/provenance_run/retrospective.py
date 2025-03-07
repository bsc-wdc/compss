import subprocess

from rocrate.model import ContextEntity
from rocrate.rocrate import ROCrate


def add_create_action_for_task(
        compss_crate: ROCrate,
        task: dict,
        tool: tuple
) -> tuple:
    """
    Adds a `CreateAction` instance to the COMPSs RO-Crate for the corresponding `HowToStep`.

    @param compss_crate: The COMPSs RO-Crate being generated.
    @param task: A dictionary representing the task retrieved from `dataprovenance.log`.
    @param tool:The `SoftwareSourceCode` instance for which the action is being created.

    @return The created `CreateAction` instance.
    """
    # Get host info
    hostname = subprocess.run(["hostname"], stdout=subprocess.PIPE, check=True)
    hostname_out = hostname.stdout.decode("utf-8").strip()

    create_action_id = f"#Task_{task['id']}_execution_{hostname_out}"

    # TODO: add startTime and endTime too
    l_object = []
    for input in task["ins"]:
        l_object.append({"@id": input})
    l_result = []
    for output in task["outs"]:
        l_result.append({"@id": output})

    return compss_crate.add(ContextEntity(
        compss_crate,
        create_action_id,
        {
            "@type": "CreateAction",
            "instrument": tool,
            "name": f"Run of Task {task['id']} at host {hostname_out}",
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

    Each `HowToStep` instance will have a corresponding `ControlAction` instance.

    @param compss_crate: The COMPSs RO-Crate being generated.
    @param step: The `HowToStep` instance for which the `ControlAction` is being generated.
    @param create_action: The `CreateAction` instance that describes the execution of this step.

    @return The created `ControlAction` instance.
    """
    # TODO: generate meaningful ID
    control_action_id = f"#ControlAction/{step['@id']}"

    return compss_crate.add(ContextEntity(
        compss_crate,
        control_action_id,
        {
            "@type": "ControlAction",
            "instrument": step,
            "object": create_action,
        }
    ))


def add_organize_action(
        compss_crate: ROCrate,
        ins: list,
        outs: tuple,
        workflow_engine: tuple
):
    """
    Adds an `OrganizeAction` instance to the COMPSs RO-Crate.

    This action represents the orchestration of workflow steps, linking the COMPSs runtime and the `ControlAction` instances to the main `CreateAction` of the workflow.

    @param compss_crate: The COMPSs RO-Crate being generated.
    @param ins: A list of all `ControlAction` instances that orchestrate the workflow steps.
    @param outs: The main `CreateAction` instance of the workflow.
    @param workflow_engine: The `SoftwareApplication` instance representing the COMPSs runtime.

    @return: The created `OrganizeAction` instance.
    """
    # Get runtime info
    uname = subprocess.run(["uname", "-a"], stdout=subprocess.PIPE, check=True)
    uname_out = uname.stdout.decode("utf-8").strip()

    organize_action_id = "#COMPSs_runtime_invokation_at_" + uname_out

    return compss_crate.add(ContextEntity(
        compss_crate,
        organize_action_id,
        {
            "@type": "OrganizeAction",
            "instrument": workflow_engine,
            "object": ins,
            "result": outs,
            "name": f"Run of COMPSs runtime version {workflow_engine['softwareVersion']}",
            "actionStatus": {
                "@id": "http://schema.org/CompletedActionStatus"  ## MAY (optional)
            }
        }
    ))


def add_how_to_step(
        compss_crate: ROCrate,
        task: tuple,
        tool: tuple
) -> tuple:
    """
    Adds a `HowToStep` instance to the COMPSs RO-Crate.

    This step represents an executed task.

    @param compss_crate: The COMPSs RO-Crate being generated.
    @param task: The executed task that this step represents.
    @param tool: The `SoftwareSourceCode` instance that this step serves as an example of.

    @return The created `HowToStep` instance.
    """
    how_to_step_id = f"#{task['name']}.task{task['id']}"

    return compss_crate.add(ContextEntity(
        compss_crate,
        how_to_step_id,
        {
            "@type": "HowToStep",
            "workExample": tool,
            "position": task["id"]
        }
    ))


def update_main_entity_with_steps(
        compss_crate: ROCrate,
        steps: list
):
    """
    Updates the main entity with a `step` section that includes the executed `HowToStep`s.

    @param compss_crate: The COMPSs RO-Crate being generated.
    @param steps: A list of the executed `HowToStep` instances

    @return: The updated mainEntity with the steps.
    """
    main_entity_id = compss_crate.mainEntity.get("@id")
    old_types = compss_crate.mainEntity.get("@type")

    compss_crate.update_jsonld({
        "@id": main_entity_id,
        "@type": old_types.append("HowTo"),
    })

    compss_crate.mainEntity["step"] = steps

    return compss_crate.mainEntity
