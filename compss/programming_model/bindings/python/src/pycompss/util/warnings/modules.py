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

# -*- coding: utf-8 -*-

"""
PyCOMPSs Utils - Warnings - Optional Modules.

This file contains a list, alongside with some functions, of the
optional modules that are required in order to be able to use some
PyCOMPSs features.
"""

from pycompss.util.objects.properties import is_module_available

OPTIONAL_MODULES = {
    "dill": "Dill is a pickle extension which is capable to serialize a "
    "wider variety of objects."
}


def get_optional_module_warning(
    module_name: str, module_description: str
) -> str:
    """Get optional modules warning message.

    :param module_name: Module name.
    :param module_description: Module description.
    :return: String with the optional warning message.
    """
    ret = [
        f"\n[ WARNING ]:\t{module_name} module is not installed.",
        "\t\t%s" % module_description.replace("\n", "\n\t\t"),
        f"\t\tPyCOMPSs can work without {module_name}, "
        f"but it is recommended to have it installed.",
        f"\t\tYou can install it via pip typing pip install {module_name}, "
        f"or (probably) with your package manager.\n",
    ]
    return "\n".join(ret)


def show_optional_module_warnings() -> None:
    """Display the optional modules warnings if not available.

    :return: None.
    """
    for name, description in OPTIONAL_MODULES.items():
        if not is_module_available(name):
            print(get_optional_module_warning(name, description))
