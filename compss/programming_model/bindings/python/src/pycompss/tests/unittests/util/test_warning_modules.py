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

import os
import sys

from pycompss.util.exceptions import PyCOMPSsException


def test_get_optional_module_warning():
    from pycompss.util.warnings.modules import get_optional_module_warning

    warning = get_optional_module_warning(
        "UNITTEST_NAME", "UNITTEST_DESCRIPTION"
    )
    assert isinstance(
        warning, str
    ), "Optional module warning does NOT return a string"
    assert warning != "", "Optional module warning can not be empty"
    assert (
        "UNITTEST_NAME" in warning
    ), "Module name not in optional module warning"
    assert (
        "UNITTEST_DESCRIPTION" in warning
    ), "Module description not in optional module warning"


def test_show_optional_module_warning():
    import pycompss.util.warnings.modules as warn

    # Hack - Add non existing package
    warn.OPTIONAL_MODULES["non_existing_package"] = "this is the description"
    stdout_backup = sys.stdout
    out_file = "warning.out"
    fd = open(out_file, "w")
    sys.stdout = fd
    warn.show_optional_module_warnings()
    # Cleanup
    sys.stdout = stdout_backup
    fd.close()
    del warn.OPTIONAL_MODULES["non_existing_package"]
    # Result check
    if os.path.exists(out_file) and os.path.getsize(out_file) > 0:
        # Non empty file exists - this is ok.
        os.remove(out_file)
    else:
        raise PyCOMPSsException("The warning has not been shown")
