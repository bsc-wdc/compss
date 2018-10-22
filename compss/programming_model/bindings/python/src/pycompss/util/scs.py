#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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


def get_reservation_nodes():
    """
    Get the nodes that belong to the current reservation.
    Currently only supports SLURM queuing system
    :return: List of nodes
    """
    nodes = os.environ['SLURM_JOB_NODELIST']
    expander_command = 'scontrol show hostname ' + nodes
    # Get the list of nodes that belong to the reservation (for the future project and resources xml creation)
    nodes = subprocess.check_output(expander_command.split(' ')).split()
    return nodes


def get_master_node():
    """
    Get the master node.
    TIP: The environment variable COMPSS_MASTER_NODE is defined in launch_compss script.
    :return: Master node
    """
    return os.environ['COMPSS_MASTER_NODE']


def get_master_port():
    """
    Get the master port.
    TIP: The environment variable COMPSS_MASTER_PORT is defined in launch_compss script.
    :return: Master port
    """
    return os.environ['COMPSS_MASTER_PORT']


def get_worker_nodes():
    """
    Get the worker nodes.
    TIP: The environment variable COMPSS_WORKER_NODES is defined in launch_compss script.
    :return: List of worker nodes
    """
    return os.environ['COMPSS_WORKER_NODES']


def get_xmls():
    """
    Get the project and resources from the environment variable exported from the submit_jupyter_job.sh
    :return: the project and resources paths
    """
    project = os.environ['COMPSS_PROJECT_XML']
    resources = os.environ['COMPSS_RESOURCES_XML']
    return project, resources


def get_uuid():
    """
    Get UUID.
    TIP: The environment variable COMPSS_UUID is defined in launch_compss script.
    :return: UUID
    """
    return os.environ['COMPSS_UUID']


def get_base_log_dir():
    """
    Get base log dir.
    TIP: The environment variable COMPSS_BASE_LOG_DIR is defined in launch_compss script.
    :return: Base log dir
    """
    return os.environ['COMPSS_BASE_LOG_DIR']


def get_specific_log_dir():
    """
    Get specific log dir.
    TIP: The environment variable COMPSS_SPECIFIC_LOG_DIR is defined in launch_compss script.
    :return: Specific log dir
    """
    return os.environ['COMPSS_SPECIFIC_LOG_DIR']


def get_tracing():
    """
    Get tracing boolean.
    TIP: The environment variable COMPSS_TRACING is defined in launch_compss script.
    :return: Tracing boolean
    """
    return 'true' == os.environ['COMPSS_TRACING']


def get_storage_conf():
    """
    Get storage configuration file.
    TIP: The environment variable COMPSS_STORAGE_CONF is defined in launch_compss script.
    :return: Storage configuration file path
    """
    return os.environ['COMPSS_STORAGE_CONF']


def generate_xmls(compss_home, nodes, master_port):
    """
    Generate project and resources xmls.
    This function should be used only within supercomputers
    WARNING: The configuration assumes that the first node is the master and the rest are workers.
    :param compss_home: COMPSs home path
    :param nodes: List of nodes
    :param master_port: Master port
    :return: Project.xml and resources.xml paths
    """
    # Create command for calling compss_xmls_generator
    xml_generator_script = 'Runtime/scripts/user/compss_xmls_generator'
    xml_generator = compss_home + os.path.sep + xml_generator_script
    command = [xml_generator]
    command += ['--sc_cfg=mn']  # Currently only supported in MN4
    command += ['--master_node=' + nodes[0]]
    command += ['--master_port=' + master_port]
    if len(nodes) > 1:
        command += ['--worker_nodes=' + ' '.join(nodes[1:])]
    command += ['--lang=python']
    # command += ['--worker_working_dir=gpfs']
    # Using default configuration for the rest of the parameters
    xml_generator_raw_output = subprocess.check_output(command).splitlines()
    # Find project and resource files
    project_xml = None
    resources_xml = None
    for l in xml_generator_raw_output:
        if l.startswith('Project.xml:'):
            # Project.xml:   ./project_1532694575.xml
            project_xml = l.split()[1]
        if l.startswith('Resources.xml:'):
            # Resources.xml: ./resources_1532694575.xml
            resources_xml = l.split()[1]
    return project_xml, resources_xml
