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

"""PyCOMPSs Testbench synthetic functions."""

import os
import shutil
import tempfile

from pycompss.api.api import compss_open
from pycompss.api.api import compss_wait_on_file
from pycompss.api.binary import binary
from pycompss.api.mpi import mpi
from pycompss.api.ompss import ompss
from pycompss.api.parameter import FILE_OUT_STDOUT
from pycompss.api.parameter import Type
from pycompss.api.task import task

TEMPORARY_DIRECTORY = tempfile.mkdtemp()


@binary(binary="date", working_dir=TEMPORARY_DIRECTORY)
@task(result={Type: FILE_OUT_STDOUT})
def check_binary(result):  # noqa
    pass


@mpi(binary="date", working_dir=TEMPORARY_DIRECTORY, runner="mpirun")
@task(result={Type: FILE_OUT_STDOUT})
def check_mpi(result):  # noqa
    pass


@ompss(binary="date", working_dir=TEMPORARY_DIRECTORY)
@task(result={Type: FILE_OUT_STDOUT})
def check_ompss(result):  # noqa
    pass


def check_decorators():
    """Check the binary, mpi and ompss decorators.

    :returns: None.
    """
    binary_result = "binary_result.out"
    mpi_result = "mpi_result.out"
    ompss_result = "ompss_result.out"
    check_binary(binary_result)
    check_mpi(mpi_result)
    check_ompss(ompss_result)
    compss_wait_on_file(binary_result)
    compss_wait_on_file(mpi_result)
    compss_wait_on_file(ompss_result)

    binary_result_fd = compss_open(binary_result)
    mpi_result_fd = compss_open(mpi_result)
    ompss_result_fd = compss_open(ompss_result)

    binary_content = binary_result_fd.readlines()  # noqa
    mpi_content = mpi_result_fd.readlines()  # noqa
    ompss_content = ompss_result_fd.readlines()  # noqa

    binary_result_fd.close()  # noqa
    mpi_result_fd.close()  # noqa
    ompss_result_fd.close()  # noqa

    os.remove(binary_result)
    os.remove(mpi_result)
    os.remove(ompss_result)

    shutil.rmtree(TEMPORARY_DIRECTORY)

    assert len(binary_content) == 1
    assert len(mpi_content) == 1
    assert len(ompss_content) == 1

    print(binary_content)
    print(mpi_content)
    print(ompss_content)


def main():
    """Execute all synthetic functionalities.

    :returns: None.
    """
    check_decorators()
    # add more to be tested


# Uncomment for command line check:
# if __name__ == "__main__":
#     main()
