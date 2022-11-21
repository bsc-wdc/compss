#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
import unittest
import os

from pycompss.api.task import task
from pycompss.api.parameter import *
from pycompss.api.api import compss_barrier, compss_wait_on as cwo
from pycompss.api.software import software
from pycompss.api.data_transformation import *

from modules.dt_functions import *


@dt("A", appender)
@software(config_file=os.getcwd() + "/src/config/software.json")
def dt_software(A):
    A.append("from_software_task")
    return A


@dt("A", appender)
@task()
def dt_basic(A):
    A.append("from_task")
    return A


@dt()
@task()
def dto_basic(A):
    A.append("from_task")
    return A


@dt("A", appender_w_param, tba="dt_no_workflow")
@dt("A", app_task, is_workflow=True)
@dt("B", app_task, is_workflow=True)
@dt("B", appender_w_param, tba="dt_no_workflow")
@task()
def _appender(A, B):
    A.append("task itself")
    B.append("task itself")
    return A, B


@dt(target="data", function=fto, type=FILE_TO_OBJECT)
@task()
def file_to_object(data):
    return data


@dt(target="data", function=otf, type=OBJECT_TO_FILE, destination="dt_file_out")
@task(data=FILE_IN)
def obj_to_file(data):
    with open(data, "r") as nm:
        ret = nm.read()
    return ret


@dt(target="data", function=ftc, type=FILE_TO_COLLECTION, size=3)
@task(data=COLLECTION_IN)
def file_to_col(data):
    ret = " " + str(len(data))
    for elem in data:
        ret += elem
    return ret


@dt(target="data", function=cto, type=COLLECTION_TO_OBJECT)
@task(returns=object)
def col_to_obj(data):
    return data


@dt(target="data", function=ctf, type=COLLECTION_TO_FILE)
@task(data=FILE_IN)
def col_to_file(data):
    with open(data, "r") as nm:
        ret = nm.read()
    return ret


class TestDtDecorator(unittest.TestCase):

    def testFTO(self):
        in_file = "src/infile"
        ret = cwo(file_to_object(in_file))
        self.assertEqual(ret, "hello dt")

    def testCTO(self):
        in_col = ["1", "2", "3", "4", "5"]
        ret = cwo(col_to_obj(in_col))
        self.assertEqual(ret, "1 2 3 4 5")

    def testFTC(self):
        in_file = "src/infile_2"
        ret = cwo(file_to_col(in_file))
        self.assertEqual(len(ret), 5)

    def testOTF(self):
        in_obj = "a_string"
        ret = cwo(obj_to_file(in_obj))
        self.assertEqual(ret, "a_string serialized")

    def testCTF(self):
        in_col = ["1", "2", "3", "4", "5"]
        ret = cwo(col_to_file(in_col))
        self.assertEqual(ret, "12345 serialized")

    def testSoftware(self):
        A = [0]
        A = cwo(dt_software(A))
        self.assertTrue("from_dt" in A)
        self.assertTrue("from_software_task" in A)

    def testMultiple(self):
        A = [0, 1, 2]
        B = [3, 4, 5]

        A, B = _appender(A, B)
        A = cwo(A)
        B = cwo(B)
        self.assertTrue(all(x in A for x in ['dt_no_workflow', 'from_bb',
                                             'from_aa', 'task itself']))
        self.assertTrue(all(x in B for x in ['dt_no_workflow', 'from_bb',
                                             'from_aa', 'task itself']))

    def testBasicDtObject(self):
        A = [0]
        dt_1 = dto("A", appender)
        A = cwo(dto_basic(A, dt=dt_1))
        self.assertEqual(len(A), 3)

    def testBasic(self):
        A = [0]
        A = cwo(dt_basic(A))
        self.assertEqual(len(A), 3)

