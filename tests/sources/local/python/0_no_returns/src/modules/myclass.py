#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench Tasks
========================
"""

# Imports
from pycompss.api.task import task


class myClass(object):

    @task()
    def noReturnNoReturns(self, v):
        print("V: ", v)

    @task()
    def PrimitiveReturnNoReturns(self, v):
        print("V: ", v)
        return v + 1

    @task()
    def ObjectReturnNoReturns(self, v):
        print("V: ", v)
        v[0] += 1
        return v

    @task()
    def MultiPrimitiveReturnNoReturns(self, v):
        print("V: ", v)
        return v + 1, v + 2

    @task()
    def MultiObjectReturnNoReturns(self, v, w):
        print("V: ", v)
        print("W: ", w)
        v[0] += 1
        w.append(v[0])
        return v, w

    @task()
    def MultiReturnNoReturns(self, v):
        print("V: ", v)
        v[0] += 1
        return v, v[1] + 2

    @classmethod
    @task()
    def MultiReturnNoReturnsClassMethod(cls, v):
        print("V: ", v)
        v[0] += 1
        return v, v[1] + 2
