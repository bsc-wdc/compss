#!/usr/bin/python

# -*- coding: utf-8 -*-

# Imports
from storage.Object import SCO


def updateFile(obj):
    if obj.getID() is not None:
        import socket
        storage_path = '/tmp/PSCO/' + str(socket.gethostname()) + '/'
        from pycompss.util.serialization.serializer import serialize_to_file
        serialize_to_file(obj, storage_path + obj.getID() + ".PSCO")


# For simple PSCO test

class Person(SCO):
    name = ""
    age = 0

    def __init__(self, name, age):
        self.name = name
        self.age = age

    def get(self):
        return self.age

    def put(self, age):
        self.age = age
        updateFile(self)
