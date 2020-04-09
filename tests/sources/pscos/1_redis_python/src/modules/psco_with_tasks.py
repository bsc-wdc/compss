#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from storage.api import StorageObject
from pycompss.api.task import task

from pycompss.api.parameter import *


class PSCOWithTasks(StorageObject):
    def __init__(self, content="Content"):
        super(PSCOWithTasks, self).__init__()
        self.content = content

    @task(returns=object)
    def get_content(self):
        return self.content

    @task()
    def set_content(self, content):
        self.content = content

    @task(target_direction=INOUT)
    def persist_isModifier(self):
        self.make_persistent()

    @task(target_direction=IN)
    def persist_notIsModifier(self):
        self.make_persistent()
