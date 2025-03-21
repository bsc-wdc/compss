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

from pycompss.streams.distro_stream import DistroStreamClientHandler
from pycompss.util.process.manager import create_process


def test_client_handler():
    """
    Tests the client handler with two different processes.

    :return:
    """

    def runner():
        print("Starting process")
        print("Init Client Handler")
        DistroStreamClientHandler.init_and_start("localhost", "49049")
        print("Stop Client Handler")
        DistroStreamClientHandler.set_stop()
        print("End process")

    p1 = create_process(target=runner)
    p2 = create_process(target=runner)

    p1.start()
    p2.start()

    p1.join()
    p2.join()
