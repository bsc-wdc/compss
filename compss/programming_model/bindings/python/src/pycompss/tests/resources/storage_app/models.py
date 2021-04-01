#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

# -*- coding: utf-8 -*-

"""
PyCOMPSs Testbench PSCO Models
========================
"""

import socket
from pycompss.tests.resources.storage.Object import SCO
from pycompss.api.task import task
from pycompss.api.parameter import INOUT
from pycompss.util.serialization.serializer import serialize_to_file


def update_file(obj):
    if obj.getID() is not None:
        storage_path = '/tmp/PSCO/' + str(socket.gethostname()) + '/'  # NOSONAR
        serialize_to_file(obj, storage_path + obj.getID() + ".PSCO")


class MySO(SCO):
    """
    @ClassField value int
    """
    # For simple PSCO test
    value = 0

    def __init__(self, v):  # noqa
        self.value = v

    def get(self):
        return self.value

    def put(self, v):
        self.value = v
        update_file(self)

    @task(target_direction=INOUT)
    def increment(self):
        self.value += 1
        self.updatePersistent()


class Words(SCO):
    """
    @ClassField text dict <<position:int>, word_info:str>
    """
    # For Wordcount Test
    text = ''

    def __init__(self, t):  # noqa
        self.text = t

    def get(self):
        return self.text


class Result(SCO):
    """
    @ClassField myd dict <<word:str>,instances:atomicint>
    """
    myd = {}

    def get(self):
        return self.myd

    def set(self, d):
        self.myd = d
        update_file(self)


# For Tiramisu mockup test

class InputData(SCO):
    """
    @ClassField images dict <<image_id:str>, value:list>
    """
    images = {}

    def get(self):
        return self.images

    def set(self, i):
        self.images = i
        update_file(self)
