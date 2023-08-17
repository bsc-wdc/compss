#!/usr/bin/python

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench PSCO Models
========================
"""

# Imports
from storage.Object import SCO
from pycompss.api.task import task
from pycompss.api.parameter import INOUT


def updateFile(obj):
    if obj.getID() is not None:
        import socket
        storage_path = '/tmp/PSCO/' + str(socket.gethostname()) + '/'
        from pycompss.util.serialization.serializer import serialize_to_file
        from pycompss.tests.outlog import create_logger
        logger = create_logger()
        serialize_to_file(obj, storage_path + obj.getID() + ".PSCO", logger)


# For simple PSCO test

class mySO(SCO):
    value = 0

    def __init__(self, v):
        self.value = v

    def get(self):
        return self.value

    def put(self, v):
        self.value = v
        updateFile(self)

    @task(target_direction=INOUT)
    def increment(self):
        self.value += 1
        self.updatePersistent()


# For Wordcount Test

class Words(SCO):
    '''
    @ClassField wordinfo dict <<position:int>,wordinfo:str>
    '''
    text = ''

    def __init__(self, t):
        self.text = t

    def get(self):
        return self.text


class Result(SCO):
    '''
    @ClassField instances dict <<word:str>,instances:atomicint>
    '''
    myd = {}

    def __init__(self):
        pass

    def get(self):
        return self.myd

    def set(self, d):
        self.myd = d
        updateFile(self)


# For Tiramisu mockup test

class InputData(SCO):
    '''
    @ClassField images dict <<image_id:str>, value:list>
    '''
    images = {}

    def __init__(self):
        pass

    def get(self):
        return self.images

    def set(self, i):
        self.images = i
        updateFile(self)
