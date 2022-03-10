#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs PYTHONPATH Fixer
=========================
    Fixes the PYTHONPATH for Container environments.
"""

import sys


def fix_pythonpath() -> None:
    """Resets the PYTHONPATH for clean container environments

    :return: None
    """
    version = "%s.%s" % sys.version_info[0:2]
    # Default Python installation in Docker containers
    default_container_pythonpath = [
        "/usr/lib/python%s" % version,
        "/usr/lib/python%s/plat-x86_64-linux-gnu" % version,
        "/usr/lib/python%s/lib-tk" % version,
        "/usr/lib/python%s/lib-old" % version,
        "/usr/lib/python%s/lib-dynload" % version,
        "/usr/local/lib/python%s/dist-packages" % version,
        "/usr/lib/python%s/dist-packages" % version,
    ]

    # Build new PYTHONPATH
    new_pythonpath = []

    # Add entries not inherited by user's system default
    # (application pythonpath only)
    for pp in sys.path:
        if pp.startswith("/apps/COMPSs/") or not (
            pp.startswith("/apps/") or pp.startswith("/gpfs/apps/")
        ):
            new_pythonpath.append(pp)

    # Add default entries (at the end)
    new_pythonpath.extend(default_container_pythonpath)

    # Reset PYTHONPATH
    sys.path = new_pythonpath


# Fix on this module import
fix_pythonpath()
