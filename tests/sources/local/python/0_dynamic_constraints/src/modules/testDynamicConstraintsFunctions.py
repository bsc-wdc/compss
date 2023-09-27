#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
import unittest
import time
from pycompss.api.api import compss_wait_on
from pycompss.api.constraint import constraint
from pycompss.api.task import task

CU = 1
MS = 1
SS = 1

@constraint(computing_units="comp")
@task(returns=int)
def singleintconstTask(comp):
    print("computing_units=", comp)
    time.sleep(1)
    return 1


@constraint(computing_units="comp", memory_size="mems")
@task(returns=int)
def multintconstTask(comp, mems):
    print("computing_units=", comp)
    print("memory_size=", mems)
    time.sleep(1)
    return 1


@constraint(computing_units="comp+comp")
@task(returns=int)
def singleexpconstTask(comp):
    print("computing_units=", comp + comp)
    time.sleep(1)
    return 1


@constraint(computing_units="comp+comp", memory_size="comp + comp*2 + 4")
@task(returns=int)
def multexpconstTask(comp):
    print("computing_units=", comp + comp)
    print("memory_size=", comp + comp*2 + 4)
    time.sleep(1)
    return 1


@constraint(computing_units="CU")
@task(returns=int)
def singleglobalconstTask():
    global CU
    print("computing_units=", CU)
    time.sleep(1)
    return 1


@constraint(computing_units="CU", memory_size="MS")
@task(returns=int)
def multglobalconstTask():
    global CU
    global MS
    print("computing_units=", CU)
    print("memory_size=", MS)
    time.sleep(1)
    return 1


@constraint(memory_type="memt")
@task(returns=int)
def singlestrconstTask(memt):
    print("memory_type=", memt)
    time.sleep(1)
    return 1


@constraint(memory_type="memt", storage_type="stot")
@task(returns=int)
def multstrconstTask(memt, stot):
    print("memory_type=", memt)
    print("storage_type=", stot)
    time.sleep(1)
    return 1


@constraint(computing_units="comp", memory_type="memt", memory_size="comp + comp/1", storage_size="SS")
@task(returns=int)
def multintstrexpglobalconstTask(comp, memt):
    global SS
    print("computing_units=", comp)
    print("memory_type=", memt)
    print("memory_size=", comp + comp/1)
    print("storage_size=", SS)
    time.sleep(1)
    return 1


@constraint(computing_units="comp", memory_size="1")
@task(returns=int)
def dynamicstaticintconstTask(comp):
    print("computing_units=", comp)
    print("memory_size=", 1)
    time.sleep(1)
    return 1


@constraint(computing_units="1", memory_size="mems*2")
@task(returns=int)
def dynamicstaticexpconstTask(mems):
    print("computing_units=", 1)
    print("memory_size=", mems*2)
    time.sleep(1)
    return 1


@constraint(computing_units="1", memory_size="MS")
@task(returns=int)
def dynamicstaticglobalconstTask():
    global MS
    print("computing_units=", 1)
    print("memory_size=", MS)
    time.sleep(1)
    return 1



@constraint(computing_units="1", memory_type="memt")
@task(returns=int)
def dynamicstaticstrconstTask(memt):
    print("computing_units=", memt)
    print("memory_type=DRAM")
    time.sleep(1)
    return 1


class testDynamicConstraintsFunctions(unittest.TestCase):

    '''
    CONSTRAINTS WITH INTS
    '''

    # we have one int dynamic constraint and different ways to pass the argument
    def testsingleintconstTask1(self):
        comp = 1
        pending1 = singleintconstTask(comp)
        pending2 = singleintconstTask(1)
        pending3 = singleintconstTask(comp=1)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    def testsingleintconstTask2(self):
        comp = 1
        pending1 = singlestrconstTask(comp)
        comp = 2
        pending2 = singlestrconstTask(comp)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (1, 1))

    # we have multiple int dynamic constraints and different ways to pass the argument
    def testmultintconstTask1(self):
        comp = 1
        mems = 1
        pending1 = multintconstTask(comp, mems)
        comp = 2
        mems = 3
        pending2 = multintconstTask(comp, mems)
        pending3 = multintconstTask(comp=1, mems=1)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    '''
    CONSTRAINTS WITH EXPRESSION
    '''

    # we have one expression dynamic constraint and different ways to pass the argument
    def testsingleexpconstTask1(self):
        comp = 1
        pending1 = singleexpconstTask(comp)
        pending2 = singleexpconstTask(1)
        pending3 = singleexpconstTask(comp=1)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    def testsingleexpconstTask2(self):
        comp = 1
        pending1 = singleexpconstTask(comp)
        comp = 2
        pending2 = singleexpconstTask(comp)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (1, 1))

    # we have multiple expression dynamic constraints and different ways to pass the argument
    def testmultexpconstTask1(self):
        comp = 1
        pending1 = multexpconstTask(comp)
        comp = 2
        pending2 = multexpconstTask(comp)
        pending3 = multexpconstTask(comp=1)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    '''
    CONSTRAINTS WITH GLOBALS
    '''

    # we have one global dynamic constraint
    def testsingleglobalconstTask1(self):
        global CU
        pending1 = singleglobalconstTask()
        CU = 2
        pending2 = singleglobalconstTask()
        CU = 3
        pending3 = singleglobalconstTask()
        CU = 2
        pending4 = singleglobalconstTask()

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        result4 = compss_wait_on(pending4)
        self.assertEqual((result1, result2, result3, result4), (1, 1, 1, 1))

    # we have multiple global dynamic constraints
    def testmultglobalconstTask1(self):
        global CU
        global MS
        pending1 = multglobalconstTask()
        CU = 2
        MS = 3
        pending2 = multglobalconstTask()
        CU = 1
        MS = 1
        pending3 = multglobalconstTask()

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    '''
    CONSTRAINTS WITH STRINGS
    '''

    # we have one string dynamic constraint and different ways to pass the argument
    def testsinglestrconstTask1(self):
        memt = "DRAM"
        pending1 = singlestrconstTask(memt)
        pending2 = singlestrconstTask("DRAM")
        pending3 = singlestrconstTask(memt="DRAM")

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    def testsinglestrconstTask2(self):
        memt = "DRAM"
        pending1 = singlestrconstTask(memt)
        memt = "SRAM"
        pending2 = singlestrconstTask(memt)
        pending3 = singlestrconstTask(memt="DRAM")

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    # we have multiple string dynamic constraints and different ways to pass the argument
    def testmultstrconstTask1(self):
        memt = "DRAM"
        stot = "HDD"
        pending1 = multstrconstTask(memt, stot)
        memt = "SRAM"
        stot = "SSD"
        pending2 = multstrconstTask(memt, stot)
        pending3 = multstrconstTask(memt="DRAM", stot="HDD")

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    '''
    CONSTRAINTS WITH INT + STRING + EXPRESSION + GLOBAL
    '''

    # we have multiple int, string and expression dynamic constraints and different ways to pass the argument
    def testmultintstrexpglobalconstTask1(self):
        global SS
        comp = 1
        memt = "DRAM"
        pending1 = multintstrexpglobalconstTask(comp, memt)
        SS = 2
        comp = 2
        memt = "SRAM"
        pending2 = multintstrexpglobalconstTask(comp, memt)
        SS = 1
        pending3 = multintstrexpglobalconstTask(comp=1, memt="DRAM")

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    '''
    STATIC + DYNAMIC CONSTRAINTS
    '''

    # we have dynamic int constraint and static int constraint
    def testdynamicstaticintconstTask1(self):
        comp = 1
        pending1 = dynamicstaticintconstTask(comp)
        comp = 2
        pending2 = dynamicstaticintconstTask(comp)
        comp = 1
        pending3 = dynamicstaticintconstTask(comp)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    # we have dynamic expression constraint and static int constraint
    def testdynamicstaticexpconstTask1(self):
        mems = 1
        pending1 = dynamicstaticexpconstTask(mems)
        mems = 2
        pending2 = dynamicstaticexpconstTask(mems)
        mems = 1
        pending3 = dynamicstaticexpconstTask(mems)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    # we have dynamic global constraint and static int constraint
    def testdynamicstaticglobalconstTask1(self):
        global MS
        pending1 = dynamicstaticglobalconstTask()
        MS = 2
        pending2 = dynamicstaticglobalconstTask()
        MS = 1
        pending3 = dynamicstaticglobalconstTask()

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    # we have dynamic string constraint and static int constraint
    def testdynamicstaticstrconstTask1(self):
        memt = "DRAM"
        pending1 = dynamicstaticstrconstTask(memt)
        memt = "SRAM"
        pending2 = dynamicstaticstrconstTask(memt)
        memt = "DRAM"
        pending3 = dynamicstaticstrconstTask(memt)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))


