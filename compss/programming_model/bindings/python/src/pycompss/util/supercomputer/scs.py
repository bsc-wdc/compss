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

"""
PyCOMPSs Utils - Supercomputer - Helpers.

This file contains the methods used within Supercomputers.
They are used when running a notebook in interactive mode within a SC.
"""

import os

from pycompss.util.typing_helper import typing


def get_master_node() -> str:
    """Get the master node.

    TIP: The environment variable COMPSS_MASTER_NODE is defined in the
         launch_compss script.

    :return: Master node name.
    """
    return os.environ["COMPSS_MASTER_NODE"]


def get_master_port() -> str:
    """Get the master port.

    TIP: The environment variable COMPSS_MASTER_PORT is defined in the
         launch_compss script.

    :return: Master port.
    """
    return os.environ["COMPSS_MASTER_PORT"]


def get_worker_nodes() -> str:
    """Get the worker nodes.

    TIP: The environment variable COMPSS_WORKER_NODES is defined in the
         launch_compss script.

    :return: List of worker nodes.
    """
    return os.environ["COMPSS_WORKER_NODES"]


def get_xmls() -> typing.Tuple[str, str]:
    """Get the project and resources.

    They are taken from the environment variable exported from the
    submit_jupyter_job.sh.

    :return: the project and resources paths.
    """
    project = os.environ["COMPSS_PROJECT_XML"]
    resources = os.environ["COMPSS_RESOURCES_XML"]
    return project, resources


def get_uuid() -> str:
    """Get UUID.

    TIP: The environment variable COMPSS_UUID is defined in the
         launch_compss script.

    :return: UUID as string.
    """
    return os.environ["COMPSS_UUID"]


def get_log_dir() -> str:
    """Get log dir.

    TIP: The environment variable COMPSS_LOG_DIR is defined in the
         launch_compss script.

    :return: Log directory.
    """
    return os.environ["COMPSS_LOG_DIR"]


def get_master_working_dir() -> str:
    """Get master working directory.

    TIP: The environment variable COMPSS_MASTER_WORKING_DIR is defined in the
         launch_compss script.

    :return: Master working directory.
    """
    return os.environ["COMPSS_MASTER_WORKING_DIR"]


def get_log_level() -> str:
    """Get log level.

    TIP: The environment variable COMPSS_LOG_LEVEL is defined in the
         launch_compss script.

    :return: Log level.
    """
    return os.environ["COMPSS_LOG_LEVEL"]


def get_tracing() -> bool:
    """Get tracing boolean.

    TIP: The environment variable COMPSS_TRACING is defined in the
         launch_compss script.

    :return: Tracing boolean.
    """
    return "true" == os.environ["COMPSS_TRACING"]


def get_storage_conf() -> str:
    """Get storage configuration file.

    TIP: The environment variable COMPSS_STORAGE_CONF is defined in the
         launch_compss script.

    :return: Storage configuration file path.
    """
    return os.environ["COMPSS_STORAGE_CONF"]
