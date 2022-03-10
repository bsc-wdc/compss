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

import os
import subprocess


def test_launch_streaming_application():
    current_path = os.path.dirname(os.path.abspath(__file__))

    stream_type = "OBJECTS"
    server_name = "localhost"
    server_port = "49049"

    # Start streaming server
    start_script = os.path.join(
        current_path, "..", "resources", "streaming", "start_server.sh"
    )
    subprocess.check_call([start_script, stream_type, server_name, server_port])

    from pycompss.runtime.launch import launch_pycompss_application

    app = os.path.join(current_path, "..", "resources", "stream_objects.py")
    launch_pycompss_application(
        app,
        "main",
        debug=True,
        app_name="stream_objects",
        streaming_backend=stream_type,
        streaming_master_name=server_name,
        streaming_master_port=server_port,
    )

    # Stop the streaming server
    stop_script = os.path.join(
        current_path, "..", "resources", "streaming", "stop_server.sh"
    )
    subprocess.check_call([stop_script, stream_type])
