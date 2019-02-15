#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench PSCO Models
========================
"""

# Imports
from pycompss.api.task import task
from pycompss.api.parameter import *
from storage.Object import SCO
import time

class MyFile(SCO):

    path=''
    def __init__(self, path):
        self.path=path

    @task(targetDirection=CONCURRENT)
    def writeThree(self):
        # Write value
        with open(self.path, 'a') as file:
            file.write("3")
        time.sleep(2)

    @task(targetDirection=INOUT)
    def writeFour(self):
        # Write value
        with open(self.path, 'a') as file:
            file.write("4")
        time.sleep(2)

    def countThrees(self):
        # Read final value
        with open(self.path) as file:
            final_value = file.read()

        total = final_value.count('3')
        return total


    def countFours(self):
        # Read final value
        with open(self.path) as file:
            final_value = file.read()

        total = final_value.count('4')
        return total

    def get(self):
        return self.path

def updateFile(obj):
    if obj.getID() is not None:
        import socket
        storage_path = '/tmp/PSCO/' + str(socket.gethostname()) + '/'
        from pycompss.util.serializer import serialize_to_file
        serialize_to_file(obj, storage_path + obj.getID() + ".PSCO")
