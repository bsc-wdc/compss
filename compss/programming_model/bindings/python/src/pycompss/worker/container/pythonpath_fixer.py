#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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


def fix_pythonpath():
    # type: (...) -> None
    """ Resets the PYTHONPATH for clean container environments

    :return: None
    """
    # Default Python installation in Docker containers
    default_container_pythonpath = ['/usr/lib/python2.7', '/usr/lib/python2.7/plat-x86_64-linux-gnu',
                                    '/usr/lib/python2.7/lib-tk', '/usr/lib/python2.7/lib-old',
                                    '/usr/lib/python2.7/lib-dynload', '/usr/local/lib/python2.7/dist-packages',
                                    '/usr/lib/python2.7/dist-packages']

    # Build new PYTHONPATH
    new_pythonpath = []

    # Add entries not inherited by user's system default (application pythonpath only)
    for pp in sys.path:
        if pp.startswith("/apps/COMPSs/") \
                or not (pp.startswith("/apps/") or pp.startswith("/gpfs/apps/")):
            new_pythonpath.append(pp)

    # Add default entries (at the end)
    new_pythonpath.extend(default_container_pythonpath)

    # Reset PYTHONPATH
    sys.path = new_pythonpath


# Fix on this module import
fix_pythonpath()
