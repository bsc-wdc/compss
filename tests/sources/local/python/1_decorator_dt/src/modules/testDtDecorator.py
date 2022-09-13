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
from pycompss.api.data_transformation import dt
from pycompss.api.data_transformation import dto


@task(returns=object)
def bb(A):
    A.append("from_bb")
    return A


@task(returns=object)
def aa(A):
    A.append("from_aa")
    return A


def app_task(A):
    return aa(bb(A))


def appender(A):
    A.append("from_dt")
    return A


def appender_w_param(a_list, tba):
    a_list.append(tba)
    return a_list


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


class TestDtDecorator(unittest.TestCase):

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

