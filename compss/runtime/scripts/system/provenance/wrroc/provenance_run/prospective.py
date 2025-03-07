from rocrate.model import ContextEntity
from rocrate.rocrate import ROCrate


def add_workflow_engine(
        compss_crate: ROCrate,
        version: str
) -> tuple:
    """
    Adds a SoftwareApplication entity, representing the COMPSs runtime, to the COMPSs RO-Crate.

    @param compss_crate: The COMPSs RO-Crate being generated.
    @param version: The COMPSs version used.

    @return: The created SoftwareApplication instance.
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


def add_software_tool(
        compss_crate: ROCrate,
        task: dict
):
    """
    Adds a `SoftwareSourceCode` entity, representing the software tool (a.k.a. the task definition), to the COMPSs RO-Crate.

    @param compss_crate: The COMPSs RO-Crate being generated.
    @param task: A dictionary representing the task that implements the software tool.

    @return: The created SoftwareSourceCode instance.
    """
    tool_id = f"#{task['name']}"

    # TODO: THINK ABOUT THIS. The splitting logic should be kept in the files.py, where we handle the dataprovenance.log
    file_name, method_name = task["name"].split(".")

    input_params = []
    output_params = []
    for param in task["params"]:
        param_name, param_direction = param.split(".")
        returned_param = add_parameter_definition(compss_crate, method_name, param_name)
        if "OUT" in param_direction:
            output_params.append(returned_param)
        if "IN" in param_direction:
            input_params.append(returned_param)
        else:
            input_params.append(returned_param)

    return compss_crate.add(ContextEntity(
        compss_crate,
        tool_id,
        {
            "@type": "SoftwareSourceCode",
            "name": method_name,
            "description": f"{method_name} inside {file_name}",
            "input": input_params,
            "output": output_params
        }
    ))


def update_main_entity_with_software_tools(
        compss_crate: ROCrate,
        tools: list
):
    """
    Updates the main entity's `hasPart` section to include the given tools (task definitions).

    @param compss_crate: The COMPSs RO-Crate being generated.
    @param tools: A list of software tools, where each tool corresponds to a method decorated with `@task`.

    @return: The updated mainEntity with the added tools.
    """
    return compss_crate.mainEntity.append_to("hasPart", tools)


def add_parameter_definition(
        compss_crate: ROCrate,
        method_name: str,
        param_name: str,
        type="TODO"
):
    """
    Adds a formal parameter definition to a COMPSs RO-Crate.

    @param compss_crate: The COMPSs RO-Crate being generated.
    @param method_name: The name of the method to which this parameter belongs.
    @param param_name: The name of the parameter as it appears in the method definition.
    @param type: The type of the parameter.

    @return: The created FormalParameter instance
    """
    formal_parameter_id = f"#{method_name}#{param_name}"
    return compss_crate.add(ContextEntity(
        compss_crate,
        formal_parameter_id,
        {
            "@type": "FormalParameter",
            "additionalType": type,
            "name": param_name
        }
    ))
