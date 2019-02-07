#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench PSCO Models
========================
"""

# Imports
from storage.Object import SCO
from pycompss.api.api import compss_open

class MyFile(SCO):

    path=''
    # The class "constructor" - It's actually an initializer
    def __init__(self, path):
        self.path=path

    def getPath(self):
        return self.path

    def blankFile(self):
        open(self.path, 'w').close()

    def writeOne(self):
        # Write value
        with open(self.path, 'a') as file:
            new_value = "1"
            file.write("1")

    def countOnes(self):
        # Read final value
        with compss_open(self.path) as file:
            final_value = file.read()
        total = final_value.count('1')
        return total

    def get(self):
        return self.path

def updateFile(obj):
    if obj.getID() is not None:
        import socket
        storage_path = '/tmp/PSCO/' + str(socket.gethostname()) + '/'
        from pycompss.util.serializer import serialize_to_file
        serialize_to_file(obj, storage_path + obj.getID() + ".PSCO")


