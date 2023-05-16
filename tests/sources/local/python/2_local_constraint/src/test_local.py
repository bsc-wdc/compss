#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
==================
    This file represents PyCOMPSs Testbench.
    It implements all functionalities in order to evaluate the PyCOMPSs features.
"""

# Imports
import os, shutil

from time import sleep

from pycompss.api.api import compss_barrier
from pycompss.api.task import task
from pycompss.api.constraint import constraint
from pycompss.api.parameter import DIRECTORY_INOUT
from pycompss.api.api import compss_wait_on_directory as cwod


@task(returns=1)
def increment_out(value):
    sleep(1)
    return value + "1"


@constraint(is_local=True)
@task(returns=1)
def increment_local(value):
    sleep(1)
    return value + "1"


@task(dir_inout=DIRECTORY_INOUT)
def dir_inout_task(dir_inout):
    for _ in os.listdir(dir_inout):
        _fp = dir_inout + os.sep + _
        with(open(_fp, 'a')) as nm:
            nm.write(" #inout_task# ")


def main():
    sleep(3)
    for i in range(0,10):
        result = increment_out("1234")
    compss_barrier()
    for i in range(0,10):
        result = increment_local("1234")

    test_dir_inout()


def test_dir_inout():
    """
    Test DIRECTORY_INOUT
    """
    cur_path = "{}{}".format(os.getcwd(), os.sep)
    dir_inout = "{}{}".format(cur_path, "some_dir_inout")
    os.mkdir(dir_inout)

    _content = "this is some text to test directory_inout".split(" ")

    for i, word in enumerate(_content):
        temp = "{}{}{}".format(dir_inout, os.sep, str(i))
        with open(temp, 'w') as f:
            f.write(word)

        dir_inout_task(dir_inout)
        cwod(dir_inout)

        res = ""
        for _f in os.listdir(dir_inout):
            temp = "{}{}{}".format(dir_inout, os.sep, _f)
            with(open(temp, 'r')) as _:
                res += _.read() + " "
    try:
        for word in _content:
            if word not in res:
                raise Exception("DIRECTORY_INOUT Test failed.")
    finally:
        shutil.rmtree(dir_inout)


if __name__ == "__main__":
    main()
