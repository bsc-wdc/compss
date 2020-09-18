#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench
========================
"""

# Imports
from storage.api import StorageObject
# Redis connector does not provide INOUT natively
from storage.api import delete_persistent
from storage.api import make_persistent


class PSCO(StorageObject):
    def __init__(self, content="Content"):
        super(PSCO, self).__init__()
        self.content = content

    def get_content(self):
        return self.content

    def set_content(self, content):
        self.content = content

    def increase_content(self, value, update=True):
        self.content += value
        if update:
            # Redis connector does not provide INOUT natively
            id = str(self.getID())
            delete_persistent(self)
            make_persistent(self, id)
