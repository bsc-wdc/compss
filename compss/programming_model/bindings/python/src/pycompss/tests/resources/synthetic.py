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

import os
from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.binary import binary
from pycompss.api.ompss import ompss
from pycompss.api.mpi import mpi
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_open
from pycompss.api.api import compss_wait_on_file


@binary(binary="date", working_dir="/tmp")
@task(result={Type: FILE_OUT_STDOUT})
def check_binary(result):
    pass


@mpi(binary="date", working_dir="/tmp", runner="mpirun")
@task(result={Type: FILE_OUT_STDOUT})
def check_mpi(result):
    pass


@ompss(binary="date", working_dir="/tmp")
@task(result={Type: FILE_OUT_STDOUT})
def check_ompss(result):
    pass


def check_decorators():
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

    binary_content = binary_result_fd.readlines()
    mpi_content = mpi_result_fd.readlines()
    ompss_content = ompss_result_fd.readlines()

    binary_result_fd.close()
    mpi_result_fd.close()
    ompss_result_fd.close()

    os.remove(binary_result)
    os.remove(mpi_result)
    os.remove(ompss_result)

    assert len(binary_content) == 1
    assert len(mpi_content) == 1
    assert len(ompss_content) == 1

    print(binary_content)
    print(mpi_content)
    print(ompss_content)


def main():
    check_decorators()
    # add more to be tested


# if __name__ == "__main__":
#     main()
