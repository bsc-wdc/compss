from rocrate.model import ContextEntity, Entity
from rocrate.rocrate import ROCrate

from provenance.models.Parameter import Parameter
from provenance.models.Task import Task


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


def add_how_to_step(
        compss_crate: ROCrate,
        task: Task,
        tool: Entity
) -> ContextEntity:
    """
    Adds a `HowToStep` instance to the COMPSs RO-Crate.
    This represents the task declaration.

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
            "name": f"Task {task.tid}",
            "workExample": tool,
            "position": task.tid
        }
    ))


def update_main_entity_with_steps(
        compss_crate: ROCrate,
        steps: list[ContextEntity]
) -> Entity:
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


def add_software_tool_for_task(
        compss_crate: ROCrate,
        task: Task
) -> ContextEntity:
    """
    Adds a `SoftwareSourceCode` entity, representing the software tool (a.k.a. the task definition), to the COMPSs RO-Crate.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param task: A Task object that implements the software tool.

    :return: The created SoftwareSourceCode instance.
    """
    tool_id = f"#{task.signature}"

    input_params = []
    output_params = []
    for param in task.params:
        if "OUT" in param.direction:
            output_params.append(param.formal_instance)
        if "IN" in param.direction:
            input_params.append(param.formal_instance)

    return compss_crate.add(ContextEntity(
        compss_crate,
        tool_id,
        {
            "@type": "SoftwareSourceCode",
            "name": task.method,
            "description": f"{task.method} method inside {task.file}",
            "input": input_params,
            "output": output_params
        }
    ))


def update_main_entity_with_software_tools(
        compss_crate: ROCrate,
        tools: list[Entity]
) -> ContextEntity:
    """
    Updates the main entity's `hasPart` section to include the given tools (task definitions).

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param tools: A list of software tools, where each tool corresponds to a method decorated with `@task`.

    :return: The updated mainEntity with the added tools.
    """
    return compss_crate.mainEntity.append_to("hasPart", tools)


def add_parameter_definition(
        compss_crate: ROCrate,
        param: Parameter
) -> ContextEntity:
    """
    Adds a formal parameter definition to a COMPSs RO-Crate.

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param param: A Parameter object to be added to the crate.

    :return: The created FormalParameter instance
    """
    formal_parameter_id = f"#{param.method}::{param.name}"
    formal_parameter_properties = {
        "@type": "FormalParameter",
        "additionalType": param.dtype,
        "name": param.name,
        "workExample": []
    }

    if param.isArray == "True":
        formal_parameter_properties["multipleValues"] = True

    return compss_crate.add(ContextEntity(
        compss_crate,
        formal_parameter_id,
        formal_parameter_properties
    ))
