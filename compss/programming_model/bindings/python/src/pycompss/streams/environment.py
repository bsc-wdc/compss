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
PyCOMPSs Binding - Streams - Environment
========================================
This file contains the methods to setup the environment when using streaming.
It is called from interactive.py and launch.py scripts.
"""

from pycompss.streams.components.distro_stream_client import (
    DistroStreamClientHandler,
)  # noqa: E501

if __debug__:
    import logging

    logger = logging.getLogger(__name__)


def init_streaming(streaming_backend, streaming_master_name, streaming_master_port):
    # type: (str, str, str) -> bool
    """Initialize the streaming client.

    :param streaming_backend: Streaming backend.
    :param streaming_master_name: Streaming backend master node name.
    :param streaming_master_port: Streaming backend master port.
    :return: True if initialized successfully, False otherwise.
    """
    # Fix options if necessary
    if (
        streaming_master_name == ""
        or not streaming_master_name
        or streaming_master_name == "null"
    ):
        streaming_master_name = "localhost"
    if (
        streaming_master_port == ""
        or not streaming_master_port
        or streaming_master_port == "null"
    ):
        streaming_master_port = "49049"

    # Check if the stream backend is enabled
    if (
        streaming_backend != ""
        and streaming_backend != "null"
        and streaming_backend != "None"
        and streaming_backend != "NONE"
    ):
        streaming_enabled = True
    else:
        streaming_enabled = False

    # Init stream backend if needed
    if streaming_enabled:
        if __debug__:
            logger.debug("Starting DistroStream library")
        DistroStreamClientHandler.init_and_start(
            master_ip=streaming_master_name,  # noqa: E501
            master_port=streaming_master_port,
        )  # noqa: E501

    # Return whether the streaming backend is enabled or not
    return streaming_enabled


def stop_streaming():
    # type: () -> None
    """Stop the streaming backend.

    :return: None
    """
    if __debug__:
        logger.debug("Stopping DistroStream library")
    DistroStreamClientHandler.set_stop()
