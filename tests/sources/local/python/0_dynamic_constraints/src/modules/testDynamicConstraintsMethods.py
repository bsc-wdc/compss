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


class testDynamicCostraintsClass:
    @constraint(computing_units="comp")
    @task(returns=int)
    def singleintconstTask(self, comp):
        print("computing_units=", comp)
        time.sleep(1)
        return 1

    @constraint(computing_units="comp", memory_size="mems")
    @task(returns=int)
    def multintconstTask(self, comp, mems):
        print("computing_units=", comp)
        print("memory_size=", mems)
        time.sleep(1)
        return 1

    @constraint(memory_type="memt")
    @task(returns=int)
    def singlestrconstTask(self, memt):
        print("memory_type=", memt)
        time.sleep(1)
        return 1

    @constraint(memory_type="memt", storage_type="stot")
    @task(returns=int)
    def multstrconstTask(self, memt, stot):
        print("memory_type=", memt)
        print("storage_type=", stot)
        time.sleep(1)
        return 1

    @constraint(computing_units="comp", memory_type="memt")
    @task(returns=int)
    def multintstrconstTask(self, comp, memt):
        print("computing_units=", comp)
        print("memory_type=", memt)
        time.sleep(1)
        return 1

    @constraint(computing_units="comp", memory_size="1")
    @task(returns=int)
    def dynamicstaticintconstTask(self, comp):
        print("computing_units=", comp)
        print("memory_size=", 1)
        time.sleep(1)
        return 1

    @constraint(computing_units="1", memory_type="memt")
    @task(returns=int)
    def dynamicstaticstrconstTask(self, memt):
        print("computing_units=", memt)
        print("memory_type=DRAM")
        time.sleep(1)
        return 1

    '''
    CONSTRAINTS WITH INTS
    '''

class testDynamicCostraintsMethods(unittest.TestCase):

    # we have one int dynamic constraint and different ways to pass the argument
    def testsingleintconstTask1(self):
        comp = 1
        tdcc = testDynamicCostraintsClass()
        pending1 = tdcc.singleintconstTask(comp)
        pending2 = tdcc.singleintconstTask(1)
        pending3 = tdcc.singleintconstTask(comp=1)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    def testsingleintconstTask2(self):
        tdcc = testDynamicCostraintsClass()
        comp = 1
        pending1 = tdcc.singlestrconstTask(comp)
        comp = 2
        pending2 = tdcc.singlestrconstTask(comp)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        self.assertEqual((result1, result2), (1, 1))

    # we have multiple int dynamic constraints and different ways to pass the argument
    def testmultintconstTask1(self):
        tdcc = testDynamicCostraintsClass()
        comp = 1
        mems = 1
        pending1 = tdcc.multintconstTask(comp, mems)
        comp = 2
        mems = 3
        pending2 = tdcc.multintconstTask(comp, mems)
        pending3 = tdcc.multintconstTask(comp=1, mems=1)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    '''
    CONSTRAINTS WITH STRINGS
    '''

    # we have one string dynamic constraint and different ways to pass the argument
    def testsinglestrconstTask1(self):
        tdcc = testDynamicCostraintsClass()
        memt = "DRAM"
        pending1 = tdcc.singlestrconstTask(memt)
        pending2 = tdcc.singlestrconstTask("DRAM")
        pending3 = tdcc.singlestrconstTask(memt="DRAM")

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    def testsinglestrconstTask2(self):
        tdcc = testDynamicCostraintsClass()
        memt = "DRAM"
        pending1 = tdcc.singlestrconstTask(memt)
        memt = "SRAM"
        pending2 = tdcc.singlestrconstTask(memt)
        pending3 = tdcc.singlestrconstTask(memt="DRAM")

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    # we have multiple string dynamic constraints and different ways to pass the argument
    def testmultstrconstTask1(self):
        tdcc = testDynamicCostraintsClass()
        memt = "DRAM"
        stot = "HDD"
        pending1 = tdcc.multstrconstTask(memt, stot)
        memt = "SRAM"
        stot = "SSD"
        pending2 = tdcc.multstrconstTask(memt, stot)
        pending3 = tdcc.multstrconstTask(memt="DRAM", stot="HDD")

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    '''
    CONSTRAINTS WITH INT + STRING
    '''

    # we have multiple int and string dynamic constraints and different ways to pass the argument
    def testmultintstrconstTask1(self):
        tdcc = testDynamicCostraintsClass()
        comp = 1
        memt = "DRAM"
        pending1 = tdcc.multintstrconstTask(comp, memt)
        comp = 2
        memt = "SRAM"
        pending2 = tdcc.multintstrconstTask(comp, memt)
        pending3 = tdcc.multintstrconstTask(comp=1, memt="DRAM")

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    '''
    STATIC + DYNAMIC CONSTRAINTS
    '''

    # we have dynamic int constraint and static int constraint
    def testdynamicstaticintconstTask1(self):
        tdcc = testDynamicCostraintsClass()
        comp = 1
        pending1 = tdcc.dynamicstaticintconstTask(comp)
        comp = 2
        pending2 = tdcc.dynamicstaticintconstTask(comp)
        comp = 1
        pending3 = tdcc.dynamicstaticintconstTask(comp)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))

    # we have dynamic string constraint and static int constraint
    def testdynamicstaticstrconstTask1(self):
        tdcc = testDynamicCostraintsClass()
        memt = "DRAM"
        pending1 = tdcc.dynamicstaticstrconstTask(memt)
        memt = "SRAM"
        pending2 = tdcc.dynamicstaticstrconstTask(memt)
        memt = "DRAM"
        pending3 = tdcc.dynamicstaticstrconstTask(memt)

        result1 = compss_wait_on(pending1)
        result2 = compss_wait_on(pending2)
        result3 = compss_wait_on(pending3)
        self.assertEqual((result1, result2, result3), (1, 1, 1))
