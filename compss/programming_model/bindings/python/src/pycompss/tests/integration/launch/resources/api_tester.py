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

"""PyCOMPSs Testbench for the API."""

import os
import shutil
import time

# from pycompss.api.api import compss_get_number_of_resources
# from pycompss.api.api import compss_request_resources
# from pycompss.api.api import compss_free_resources
from pycompss.api.api import TaskGroup
from pycompss.api.api import compss_barrier
from pycompss.api.api import compss_barrier_group
from pycompss.api.api import compss_delete_file
from pycompss.api.api import compss_delete_object
from pycompss.api.api import compss_file_exists
from pycompss.api.api import compss_open
from pycompss.api.api import compss_wait_on
from pycompss.api.api import compss_wait_on_directory
from pycompss.api.api import compss_wait_on_file
from pycompss.api.parameter import FILE
from pycompss.api.parameter import FILE_INOUT
from pycompss.api.parameter import FILE_OUT
from pycompss.api.parameter import DIRECTORY_INOUT
from pycompss.api.parameter import DIRECTORY_IN
from pycompss.api.parameter import DIRECTORY_OUT
from pycompss.api.task import task


@task(fin=FILE, returns=str)
def file_in(fin):
    """Read the given file.

    :param fin: Input file path.
    :returns: The input file contents.
    """
    print("TEST FILE IN")
    # Open the file and read the content
    fin_d = open(fin, "r")
    content = fin_d.read()
    print("- In file content:\n", content)
    # Close and return the content
    fin_d.close()
    return content


@task(finout=FILE_INOUT, returns=str)
def file_inout(finout):
    """Read and modify the given file.

    CAUTION! Modifies finout.

    :param finout: Input output file path.
    :returns: The given file contents.
    """
    print("TEST FILE INOUT")
    # Open the file and read the content
    finout_d = open(finout, "r+")
    content = finout_d.read()
    print("- Inout file content:\n", content)
    # Add some content
    content += "\n===> INOUT FILE ADDED CONTENT"
    finout_d.write("\n===> INOUT FILE ADDED CONTENT")
    print("- Inout file content after modification:\n", content)
    # Close and return with the modification
    finout_d.close()
    return content


@task(fout=FILE_OUT, returns=str)
def file_out(fout, content):
    """Read the given file and returns as OUT and in return.

    :param fout: Input file path.
    :param content: Output content.
    :returns: The given file contents.
    """
    print("TEST FILE OUT")
    # Open the file for writing and write some content
    with open(fout, "w") as fout_d:
        fout_d.write(content)
    print("- Out file content added:\n", content)
    return content


def file_checker(filename, direction):
    """Check if file exists.

    :param filename: File to be checked.
    :param direction: File direction.
    :returns: None.
    """
    must_exist = compss_file_exists(filename)
    compss_delete_file(filename)
    must_not_exist = compss_file_exists(filename)
    assert must_exist is True, "File %s that must exist not found." % direction
    assert must_not_exist is False, (
        "File %s that must NOT exist is found." % direction
    )


def multiple_file_checker(filenames, directions):
    """Check if multiple files exist.

    :param filenames: Files to be checked.
    :param directions: Files direction.
    :returns: None.
    """
    must_exist = compss_file_exists(*filenames)
    compss_delete_file(*filenames)
    must_not_exist = compss_file_exists(*filenames)
    assert all(
        must_exist
    ), "Multiple files %s that must exist not found." % str(directions)
    assert not any(
        must_not_exist
    ), "Multiple files %s that must NOT exist is found." % str(directions)


def files():
    """Test files functionality.

    :returns: None.
    """
    # Test FILE_IN
    fin = "infile"
    content = "IN FILE CONTENT"
    with open(fin, "w") as f:
        f.write(content)
    res = file_in(fin)
    res = compss_wait_on(res)
    assert res == content, "strings are not equal: {}, {}".format(res, content)

    # Check if file exists:
    file_checker(fin, "IN")
    # Remove object
    compss_delete_object(res)

    # Test Multiple FILE_IN
    fin_1 = "infile_1"
    fin_2 = "infile_2"
    fin_3 = "infile_3"
    fins = [fin_1, fin_2, fin_3]
    content = "IN FILE CONTENT"
    results = []
    for fin in fins:
        with open(fin, "w") as f:
            f.write(content)
        results.append(file_in(fin))
    results = compss_wait_on(results)
    for res in results:
        assert res == content, "strings are not equal: {}, {}".format(
            res, content
        )

    # Check if file exists:
    multiple_file_checker(fins, "IN")
    # Remove objects
    compss_delete_object(*results)

    # Test FILE_INOUT
    finout = "inoutfile"
    content = "INOUT FILE CONTENT"
    with open(finout, "w") as f:
        f.write(content)
    res = file_inout(finout)
    res = compss_wait_on(res)
    compss_wait_on_file(finout)
    with compss_open(finout, "r") as finout_r:
        content_r = finout_r.read()
    content += "\n===> INOUT FILE ADDED CONTENT"
    assert res == content, "strings are not equal: {}, {}".format(res, content)
    assert content_r == content, "strings are not equal: {}, {}".format(
        content_r, content
    )

    # Check if file exists:
    file_checker(finout, "INOUT")
    # Remove object
    compss_delete_object(res)

    # Test Multiple FILE_INOUT
    finout_1 = "inoutfile_1"
    finout_2 = "inoutfile_2"
    finout_3 = "inoutfile_3"
    finouts = [finout_1, finout_2, finout_3]
    content = "INOUT FILE CONTENT"
    results = []
    for finout in finouts:
        with open(finout, "w") as f:
            f.write(content)
        results.append(file_inout(finout))
    results = compss_wait_on(results)
    compss_wait_on_file(*finouts)
    i = 0
    content += "\n===> INOUT FILE ADDED CONTENT"
    for finout in finouts:
        with compss_open(finout, "r") as finout_r:
            content_r = finout_r.read()
        assert results[i] == content, "strings are not equal: {}, {}".format(
            results[i], content
        )
        assert content_r == content, "strings are not equal: {}, {}".format(
            content_r, content
        )
        i += 1

    # Check if file exists:
    multiple_file_checker(finouts, "INOUT")
    # Remove objects
    compss_delete_object(*results)

    # Test FILE_OUT
    fout = "outfile"
    content = "OUT FILE CONTENT"
    res = file_out(fout, content)
    res = compss_wait_on(res)
    compss_wait_on_file(fout)
    with compss_open(fout, "r") as fout_r:
        content_r = fout_r.read()
    # The final file is only stored after the execution.
    # During the execution, you have to use the compss_open, which will
    # provide the real file where the output file is.
    # fileInFolder = os.path.exists(fout)
    # assert fileInFolder is True, "FILE_OUT is not in the final location"
    assert res == content, "strings are not equal: {}, {}".format(res, content)
    assert content_r == content, "strings are not equal: {}, {}".format(
        content_r, content
    )

    # Check if file exists:
    file_checker(fout, "OUT")
    # Remove object
    compss_delete_object(res)

    # Test Multiple FILE_OUT
    fout_1 = "outfile_1"
    fout_2 = "outfile_2"
    fout_3 = "outfile_3"
    fouts = [fout_1, fout_2, fout_3]
    content = "OUT FILE CONTENT"
    results = []
    for fout in fouts:
        results.append(file_out(fout, content))
    results = compss_wait_on(results)
    compss_wait_on_file(*fouts)
    i = 0
    for fout in fouts:
        with compss_open(fout, "r") as fout_r:
            content_r = fout_r.read()
        # The final file is only stored after the execution.
        # During the execution, you have to use the compss_open, which will
        # provide the real file where the output file is.
        # fileInFolder = os.path.exists(fout)
        # assert fileInFolder is True, "FILE_OUT is not in the final location"
        assert results[i] == content, "strings are not equal: {}, {}".format(
            results[i], content
        )
        assert content_r == content, "strings are not equal: {}, {}".format(
            content_r, content
        )
        i += 1

    # Check if file exists:
    file_checker(fout, "OUT")
    # Remove object
    compss_delete_object(*results)


@task(dir_inout=DIRECTORY_INOUT, returns=list)
def dir_inout_task(dir_inout, i):
    """Write to an INOUT directory.

    :param dir_inout: Directory with INOUT direction.
    :param i: Value to be written in the file within the directory.
    :returns: None.
    """
    res = list()
    for _ in os.listdir(dir_inout):
        with open("{}{}{}".format(dir_inout, os.sep, _), "r") as fd:
            res.append(fd.read())
    f_inout = "{}{}{}".format(dir_inout, os.sep, i)
    with open(f_inout, "w") as fd:
        fd.write("written by inout task #" + str(i))
    return res


@task(dir_in=DIRECTORY_IN)
def dir_in_task(dir_in):
    """Read all files within the given directory.

    :param dir_in: Source directory.
    :returns: List with the contents per file.
    """
    res = list()
    for _ in os.listdir(dir_in):
        _fp = dir_in + os.sep + _
        with open(_fp, "r") as fd:
            res.append(fd.read())
    return res


@task(dir_out=DIRECTORY_OUT)
def dir_out_task(dir_out, i):
    """Write to an OUT directory.

    :param dir_out: Directory with OUT direction.
    :param i: Value to be written in the file within the directory.
    :returns: None.
    """
    if os.path.exists(dir_out):
        shutil.rmtree(dir_out)
    os.mkdir(dir_out)
    f_out = "{}{}{}".format(dir_out, os.sep, i)
    with open(f_out, "w") as fd:
        fd.write("written in dir out #{}".format(i))


def directories():
    """Check directories functionalities.

    :returns: None.
    """
    cur_path = "{}{}".format(os.getcwd(), os.sep)
    dir_t = "{}{}".format(cur_path, "some_dir_t")
    dir_t_1 = "{}{}".format(cur_path, "some_dir_t_1")
    dir_t_2 = "{}{}".format(cur_path, "some_dir_t_2")
    dir_t_3 = "{}{}".format(cur_path, "some_dir_t_3")
    dir_ts = [dir_t_1, dir_t_2, dir_t_3]
    if os.path.exists(dir_t):
        shutil.rmtree(dir_t)
    os.mkdir(dir_t)
    for dir_t_elem in dir_ts:
        if os.path.exists(dir_t_elem):
            shutil.rmtree(dir_t_elem)
        os.mkdir(dir_t_elem)

    # len(phase[i] = i)
    res_phase_0 = []
    for i in range(0, 5, 1):
        res_phase_0.append(dir_inout_task(dir_t, i))

    res_multiple_dirs = []
    value = 11
    for dir_t_elem in dir_ts:
        res_multiple_dirs.append(dir_inout_task(dir_t_elem, value))
        value += 11

    # len(phase[i] = 5)
    res_phase_1 = []
    for i in range(0, 5, 1):
        res_phase_1.append(dir_in_task(dir_t))

    # len(phase[i] = i + 5)
    res_phase_2 = []
    for i in range(5, 10, 1):
        res_phase_2.append(dir_inout_task(dir_t, i))

    # len(phase[i] = 10)
    res_phase_3 = []
    for i in range(0, 5, 1):
        res_phase_3.append(dir_in_task(dir_t))

    # dir out should contain only the last file
    for i in range(0, 15, 1):
        dir_out_task(dir_t, i)

    res_phase_0 = compss_wait_on(res_phase_0)
    res_phase_1 = compss_wait_on(res_phase_1)
    res_phase_2 = compss_wait_on(res_phase_2)
    res_phase_3 = compss_wait_on(res_phase_3)
    compss_wait_on_directory(dir_t)
    res_multiple_dirs = compss_wait_on(res_multiple_dirs)
    compss_wait_on_directory(*dir_ts)

    for i, res in enumerate(res_phase_0):
        assert len(res) == i, "ERROR in task #{} of phase 0: {} != {}".format(
            i, len(res), i
        )

    for res in res_multiple_dirs:
        assert (
            len(res) == 0
        ), "ERROR in task of phase 0 multiple: {} != {}".format(len(res), 0)

    for i, res in enumerate(res_phase_1):
        assert len(res) == 5, "ERROR in task #{} of phase 1: {} != 5".format(
            i, len(res)
        )

    for i, res in enumerate(res_phase_2):
        assert (
            len(res) == i + 5
        ), "ERROR in task #{} of phase 2: {} != {}".format(i, len(res), i + 5)

    for i, res in enumerate(res_phase_3):
        assert len(res) == 10, "ERROR in task #{} of phase 3: {} != 10".format(
            i, len(res)
        )

    time.sleep(3)  # TODO: Why it is needed a sleep to find the directory?
    assert 1 == len(
        os.listdir(dir_t)
    ), "Directory has fewer or more files than 1: {}".format(
        len(os.listdir(dir_t))
    )
    shutil.rmtree(dir_t)
    compressed_dir = "some_dir_t.zip"
    if os.path.exists(compressed_dir):
        os.remove(compressed_dir)

    i = 1
    for dir_t in dir_ts:
        assert 1 == len(
            os.listdir(dir_t)
        ), "Directory has fewer or more files than 1: {}".format(
            len(os.listdir(dir_t))
        )
        shutil.rmtree(dir_t)
        compressed_dir = "some_dir_t_" + str(i) + ".zip"
        if os.path.exists(compressed_dir):
            os.remove(compressed_dir)
        i += 1


@task(returns=1)
def increment(value):
    """Increment the given value with 1.

    :param value: Integer value.
    :returns: value incremented with 1.
    """
    return value + 1


def test_task_groups():
    """Check task groups functionalities.

    :returns: None.
    """
    num_tasks = 3
    num_groups = 3
    results = []
    with TaskGroup("bigGroup", True):
        # Inside a big group, more groups are created
        for i in range(num_groups):
            with TaskGroup("group" + str(i), False):
                for j in range(num_tasks):
                    results.append(increment(i))

    # Barrier for groups
    for i in range(num_groups):
        compss_barrier_group("group" + str(i))


def main():
    """Check all API functionalities.

    :returns: None.
    """
    files()
    compss_barrier()
    directories()
    compss_barrier()
    test_task_groups()
    compss_barrier()


# Uncomment for command line check:
# if __name__ == "__main__":
#     main()
