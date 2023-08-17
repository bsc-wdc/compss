#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench PSCO Models
========================
"""

# Imports
import time

from pycompss.api.task import task
from pycompss.api.parameter import *

from storage.Object import SCO
from pycompss.api.task import task
from pycompss.api.parameter import INOUT
from pycompss.tests.outlog import create_logger

LOGGER = create_logger()


def updateFile(obj):
    if obj.getID() is not None:
        import socket
        storage_path = '/tmp/PSCO/' + str(socket.gethostname()) + '/'
        from pycompss.util.serialization.serializer import serialize_to_file
        serialize_to_file(obj, storage_path + obj.getID() + ".PSCO", LOGGER)

class PersistentObject(SCO):

    contents=0
    def __init(self):
        contents=0

    @task(target_direction=COMMUTATIVE)
    def write_three(self):
        # Write value
        self.contents=self.contents+3
        self.updatePersistent()
        time.sleep(2)

    @task(target_direction=INOUT)
    def write_four(self):
        # Write value
        self.contents = self.contents + 4
        self.updatePersistent()
        time.sleep(2)

    def get_count(self):
        return self.contents

    def put(self, contents):
        self.contents = contents
        updateFile(self)
