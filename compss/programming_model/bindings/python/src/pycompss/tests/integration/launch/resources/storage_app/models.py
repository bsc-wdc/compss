#!/usr/bin/env python3
#
#  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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

"""PyCOMPSs Testbench PSCO Models."""

import socket

from pycompss.api.parameter import INOUT
from pycompss.api.task import task
from pycompss.tests.integration.launch.resources.storage.Object import SCO
from pycompss.tests.outlog import get_logger
from pycompss.util.serialization.serializer import serialize_to_file

LOGGER = get_logger()


def update_file(obj):
    """Update the object in its file (serialized).

    :param obj: Object to update.
    :returns: None
    """
    if obj.getID() is not None:
        storage_path = (
            "/tmp/PSCO/" + str(socket.gethostname()) + "/"
        )  # NOSONAR
        serialize_to_file(
            obj, storage_path + obj.getID() + ".PSCO", logger=LOGGER
        )


class MySO(SCO):
    """Storage object.

    @ClassField value int
    """

    # For simple PSCO test
    value = 0

    def __init__(self, v):  # noqa
        """Create a MySO object containing an integer value.

        :param v: Integer value to be contained.
        :returns: None.
        """
        self.value = v

    def get(self):
        """Retrieve the inner value.

        :returns: The inner value.
        """
        return self.value

    def put(self, v):
        """Put an inner value.

        :param v: Integer value to be contained.
        :returns: None.
        """
        self.value = v
        update_file(self)

    @task(target_direction=INOUT)
    def increment(self):
        """Increment the inner value.

        :returns: None.
        """
        self.value += 1
        self.updatePersistent()


class Words(SCO):
    """Words storage object.

    @ClassField text dict <<position:int>, word_info:str>
    """

    # For Wordcount Test
    text = ""

    def __init__(self, t):  # noqa
        """Create a Words object containing the given text.

        :param t: Text to be contained.
        :returns: None.
        """
        self.text = t

    def get(self):
        """Retrieve the inner text.

        :returns: The inner text.
        """
        return self.text


class Result(SCO):
    """Result storage object.

    @ClassField myd dict <<word:str>,instances:atomicint>
    """

    myd = {}

    def get(self):
        """Retrieve the inner result.

        :returns: The inner result.
        """
        return self.myd

    def set(self, d):
        """Set the inner result.

        :param d: Result to be contained.
        :returns: None.
        """
        self.myd = d
        update_file(self)


# For Tiramisu mockup test


class InputData(SCO):
    """Input data storage object.

    @ClassField images dict <<image_id:str>, value:list>
    """

    images = {}

    def get(self):
        """Retrieve the inner input data.

        :returns: The inner input data.
        """
        return self.images

    def set(self, i):
        """Set the inner input data.

        :param i: Data to be contained.
        :returns: None.
        """
        self.images = i
        update_file(self)
