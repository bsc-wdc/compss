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
from provenance.models.Parameter import Parameter
from provenance.models.Task import Task
from rocrate.model import ContextEntity, Entity
from rocrate.rocrate import ROCrate


def add_how_to_step(
        compss_crate: ROCrate,
        task: Task,
        tool: Entity
) -> ContextEntity:
    """
    Adds a `HowToStep` instance to the COMPSs RO-Crate.
    This represents the task definition.

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


def add_formal_method_of_task(
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
    input_params = [param.formal_instance for param in task.in_params.values()]
    output_params = [param.formal_instance for param in task.out_params.values()]

    return compss_crate.add(ContextEntity(
        compss_crate,
        tool_id,
        {
            "@type": "SoftwareSourceCode",
            "name": task.method,
            "description": f"{task.method} method inside {task.sourcefile}",
            "input": input_params,
            "output": output_params
        }
    ))


def update_main_entity_with_formal_methods(
        compss_crate: ROCrate,
        methods: list[ContextEntity]
) -> ContextEntity:
    """
    Updates the main entity's `hasPart` section to include the given tools (task definitions).

    :param compss_crate: The COMPSs RO-Crate being generated.
    :param methods: A list of `SoftwareSourceCode` entities, representing the formal definitions of methods decorated with @task.

    :return: The updated mainEntity with the added methods.
    """
    return compss_crate.mainEntity.append_to("hasPart", methods)


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
    }

    if param.is_array == True:
        formal_parameter_properties["multipleValues"] = "True"

    return compss_crate.add(ContextEntity(
        compss_crate,
        formal_parameter_id,
        formal_parameter_properties
    ))
