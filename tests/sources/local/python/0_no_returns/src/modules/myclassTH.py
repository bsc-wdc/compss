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
    def noReturnNoReturns(self, v: int):
        print("V: ", v)

    @task()
    def PrimitiveReturnNoReturns(self, v: int) -> int:
        print("V: ", v)
        return v + 1

    @task()
    def ObjectReturnNoReturns(self, v: list) -> list:
        print("V: ", v)
        v[0] += 1
        return v

    @task()
    def MultiPrimitiveReturnNoReturns(self, v: int) -> (int, int):
        print("V: ", v)
        return v + 1, v + 2

    @task()
    def MultiObjectReturnNoReturns(self, v: list, w: list) -> (list, list):
        print("V: ", v)
        print("W: ", w)
        v[0] += 1
        w.append(v[0])
        return v, w

    @task()
    def MultiReturnNoReturns(self, v: list) -> (list, list):
        print("V: ", v)
        v[0] += 1
        return v, v[1] + 2

    @classmethod
    @task()
    def MultiReturnNoReturnsClassMethod(cls, v: list) -> (list, list):
        print("V: ", v)
        v[0] += 1
        return v, v[1] + 2
