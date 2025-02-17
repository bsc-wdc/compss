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

"""
PyCOMPSs Worker - Container - PYTHONPATH Fixer.

Fixes the PYTHONPATH for Container environments.
"""

import sys


def fix_pythonpath() -> None:
    """Reset the PYTHONPATH for clean container environments.

    :return: None.
    """
    sys_version = sys.version_info[0:2]
    version = f"{sys_version[0]}.{sys_version[1]}"
    # Default Python installation in Docker containers
    default_container_pythonpath = [
        f"/usr/lib/python{version}",
        f"/usr/lib/python{version}/plat-x86_64-linux-gnu",
        f"/usr/lib/python{version}/lib-tk",
        f"/usr/lib/python{version}/lib-old",
        f"/usr/lib/python{version}/lib-dynload",
        f"/usr/local/lib/python{version}/dist-packages",
        f"/usr/lib/python{version}/dist-packages",
    ]

    # Build new PYTHONPATH
    new_pythonpath = []

    # Add entries not inherited by user's system default
    # (application pythonpath only)
    for path in sys.path:
        if path.startswith("/apps/COMPSs/") or not (
            path.startswith("/apps/") or path.startswith("/gpfs/apps/")
        ):
            new_pythonpath.append(path)

    # Add default entries (at the end)
    new_pythonpath.extend(default_container_pythonpath)

    # Reset PYTHONPATH
    sys.path = new_pythonpath
