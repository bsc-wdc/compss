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

import os


def test_get_master_node():
    from pycompss.util.supercomputer.scs import get_master_node

    master_node = "my_master_node"
    os.environ["COMPSS_MASTER_NODE"] = master_node
    result = get_master_node()
    del os.environ["COMPSS_MASTER_NODE"]
    assert result == master_node, "ERROR: Wrong master node."


def test_get_master_port():
    from pycompss.util.supercomputer.scs import get_master_port

    master_port = "1234"
    os.environ["COMPSS_MASTER_PORT"] = master_port
    result = get_master_port()
    del os.environ["COMPSS_MASTER_PORT"]
    assert result == master_port, "ERROR: Wrong master port."


def test_get_worker_nodes():
    from pycompss.util.supercomputer.scs import get_worker_nodes

    worker_nodes = "my_worker_nodes"
    os.environ["COMPSS_WORKER_NODES"] = worker_nodes
    result = get_worker_nodes()
    del os.environ["COMPSS_WORKER_NODES"]
    assert result == worker_nodes, "ERROR: Wrong worker nodes."


def test_get_xmls():
    from pycompss.util.supercomputer.scs import get_xmls

    project = "my_project.xml"
    resources = "my_resources.xml"
    os.environ["COMPSS_PROJECT_XML"] = project
    os.environ["COMPSS_RESOURCES_XML"] = resources
    result_project, result_resources = get_xmls()
    del os.environ["COMPSS_PROJECT_XML"]
    del os.environ["COMPSS_RESOURCES_XML"]
    assert result_project == project, "ERROR: Wrong project XML."
    assert result_resources == resources, "ERROR: Wrong resources XML."


def test_get_uuid():
    from pycompss.util.supercomputer.scs import get_uuid

    uuid = "my_uuid"
    os.environ["COMPSS_UUID"] = uuid
    result = get_uuid()
    del os.environ["COMPSS_UUID"]
    assert result == uuid, "ERROR: Wrong UUID."


def test_get_log_dir():
    from pycompss.util.supercomputer.scs import get_log_dir

    log_dir = "my_log_dir"
    os.environ["COMPSS_LOG_DIR"] = log_dir
    result = get_log_dir()
    del os.environ["COMPSS_LOG_DIR"]
    assert result == log_dir, "ERROR: Wrong log directory."


def test_get_master_working_dir():
    from pycompss.util.supercomputer.scs import get_master_working_dir

    master_working_dir = "my_master_working_dir"
    os.environ["COMPSS_MASTER_WORKING_DIR"] = master_working_dir
    result = get_master_working_dir()
    del os.environ["COMPSS_MASTER_WORKING_DIR"]
    assert (
        result == master_working_dir
    ), "ERROR: Wrong master working directory."


def test_get_log_level():
    from pycompss.util.supercomputer.scs import get_log_level

    log_level = "my_log_level"
    os.environ["COMPSS_LOG_LEVEL"] = log_level
    result = get_log_level()
    del os.environ["COMPSS_LOG_LEVEL"]
    assert result == log_level, "ERROR: Wrong log level."


def test_get_tracing():
    from pycompss.util.supercomputer.scs import get_tracing

    tracing = "true"
    os.environ["COMPSS_TRACING"] = tracing
    result = get_tracing()
    assert result, "ERROR: Expected tracing enabled."
    tracing = "fakse"
    os.environ["COMPSS_TRACING"] = tracing
    result = get_tracing()
    del os.environ["COMPSS_TRACING"]
    assert not result, "ERROR: Expected tracing disabled."


def test_get_storage_conf():
    from pycompss.util.supercomputer.scs import get_storage_conf

    storage_conf = "my_storage_conf"
    os.environ["COMPSS_STORAGE_CONF"] = storage_conf
    result = get_storage_conf()
    del os.environ["COMPSS_STORAGE_CONF"]
    assert result == storage_conf, "ERROR: Wrong storage conf."
