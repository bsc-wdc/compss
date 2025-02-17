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
def get_yaml_template():
    """
    Create a YAML template for the ro-crate-info.yaml file, when not provided by the user

    :param None

    :returns: The string with the content of the template
    """

    return (
        "COMPSs Workflow Information:\n"
        "  name: Name of your COMPSs application\n"
        "  description: Detailed description of your COMPSs application\n"
        "  license: Apache-2.0\n"
        "    # URL preferred, but these strings are accepted: https://spdx.org/licenses/\n"
        "  sources: [/absolute_path_to/dir_1/, relative_path_to/dir_2/, main_file.py, relative_path/aux_file_1.py, /abs_path/aux_file_2.py]\n"
        "    # List of application source files and directories. Relative or absolute paths can be used.\n"
        "  sources_main_file: my_main_file.py\n"
        "    # Optional: Manually specify the name of the main file of the application, located in one of the 'sources' defined.\n"
        "    # Relative paths from a 'sources' entry, or absolute paths can be used.\n"
        "  data_persistence: False\n"
        "    # True to include all input and output files of the application in the resulting crate.\n"
        "    # If False, input and output files of the application won't be included, just referenced. False by default or if not set.\n"
        "  trace_persistence: False\n"
        "    # True to include all Paraver trace files of the application in the resulting crate.\n"
        "    # If False, Paraver trace files of the application won't be included, just referenced. False by default or if not set.\n"
        "  inputs: [/abs_path_to/dir_1, rel_path_to/dir_2, file_1, rel_path/file_2]\n"
        "    # Optional: Manually specify the inputs of the workflow. Relative or absolute paths can be used.\n"
        "  outputs: [/abs_path_to/dir_1, rel_path_to/dir_2, file_1, rel_path/file_2]\n"
        "    # Optional: Manually specify the outputs of the workflow. Relative or absolute paths can be used.\n"
        "  software:\n"
        "    # Optional: Manually specify the software dependencies of the application\n"
        "    - name: Software_1\n"
        "      version: Software_1 version description string\n"
        "      url: https://software_1.com/\n"
        "    - name: Software_2\n"
        "      version: Software_2 version description string\n"
        "      url: https://software_2.com/\n"
        "\n"
        "Authors:\n"
        "  - name: Author_1 Name\n"
        "    e-mail: author_1@email.com\n"
        "    orcid: https://orcid.org/XXXX-XXXX-XXXX-XXXX\n"
        "    organisation_name: Institution_1 name\n"
        "    ror: https://ror.org/XXXXXXXXX\n"
        "      # Find them in ror.org\n"
        "  - name: Author_2 Name\n"
        "    e-mail: author2@email.com\n"
        "    orcid: https://orcid.org/YYYY-YYYY-YYYY-YYYY\n"
        "    organisation_name: Institution_2 name\n"
        "    ror: https://ror.org/YYYYYYYYY\n"
        "      # Find them in ror.org\n"
        "\n"
        "Agent:\n"
        "  name: Name\n"
        "  e-mail: agent@email.com\n"
        "  orcid: https://orcid.org/XXXX-XXXX-XXXX-XXXX\n"
        "  organisation_name: Agent Institution name\n"
        "  ror: https://ror.org/XXXXXXXXX\n"
        "    # Find them in ror.org\n"
        "\n"
    )
