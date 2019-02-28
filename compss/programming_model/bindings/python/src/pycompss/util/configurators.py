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

"""
PyCOMPSs Binding - Util - configurators
=========================
This file contains the configurator methods.
Currently it is used by interactive.py and launch.py
"""

import os
from tempfile import mkstemp


def create_init_config_file(compss_home,
                            debug,
                            log_level,
                            project_xml,
                            resources_xml,
                            summary,
                            task_execution,
                            storage_conf,
                            task_count,
                            app_name,
                            uuid,
                            base_log_dir,
                            specific_log_dir,
                            graph,
                            monitor,
                            trace,
                            extrae_cfg,
                            comm,
                            conn,
                            master_name,
                            master_port,
                            scheduler,
                            cp,
                            classpath,
                            ld_library_path,
                            pythonpath,
                            jvm_workers,
                            cpu_affinity,
                            gpu_affinity,
                            fpga_affinity,
                            fpga_reprogram,
                            profile_input,
                            profile_output,
                            scheduler_config,
                            external_adaptation,
                            python_interpreter,
                            python_version,
                            python_virtual_environment,
                            propagate_virtual_environment):
    """
    Creates the initialization files for the runtime start (java options file).

    :param compss_home: <String> COMPSs installation path
    :param debug:  <Boolean> Enable/Disable debugging (True|False) (overrides log_level)
    :param log_level: <String> Define the log level ('off' (default) | 'info' | 'debug')
    :param project_xml: <String> Specific project.xml path
    :param resources_xml: <String> Specific resources.xml path
    :param summary: <Boolean> Enable/Disable summary (True|False)
    :param task_execution: <String> Who performs the task execution (normally "compss")
    :param storage_conf: None|<String> Storage configuration file path
    :param task_count: <Integer> Number of tasks (for structure initialization purposes)
    :param app_name: <String> Application name
    :param uuid: None|<String> Application UUID
    :param base_log_dir: None|<String> Base log path
    :param specific_log_dir: None|<String> Specific log path
    :param graph: <Boolean> Enable/Disable graph generation
    :param monitor: None|<Integer> Disable/Frequency of the monitor
    :param trace: <Boolean> Enable/Disable trace generation
    :param extrae_cfg: None|<String> Default extrae configuration/User specific extrae configuration
    :param comm: <String> GAT/NIO
    :param conn: <String> Connector (normally: es.bsc.compss.connectors.DefaultSSHConnector)
    :param master_name: <String> Master node name
    :param master_port: <String> Master node port
    :param scheduler: <String> Scheduler (normally: es.bsc.compss.scheduler.resourceEmptyScheduler.ResourceEmptyScheduler)
    :param cp: <String>  Application path
    :param classpath: <String> CLASSPATH environment variable contents
    :param ld_library_path: <String> LD_LIBRARY_PATH environment variable contents
    :param pythonpath: <String> PYTHONPATH environment variable contents
    :param jvm_workers: <String> Worker's jvm configuration (example: "-Xms1024m,-Xmx1024m,-Xmn400m")
    :param cpu_affinity: <String> CPU affinity (default: automatic)
    :param gpu_affinity: <String> GPU affinity (default: automatic)
    :param fpga_affinity: <String> FPGA affinity (default: automatic)
    :param fpga_reprogram: <String> FPGA reprogram command (default: '')
    :param profile_input: <String> profiling input
    :param profile_output: <String> profiling output
    :param scheduler_config: <String> Path to the file which contains the scheduler configuration.
    :param external_adaptation: <String> Enable external adaptation. This option will disable the Resource Optimizer
    :param python_interpreter: <String> Python interpreter
    :param python_version: <String> Python interpreter version
    :param python_virtual_environment: <String> Python virtual environment path
    :param propagate_virtual_environment: <Boolean> = Propagate python virtual environment to workers
    :return: None
    """
    fd, temp_path = mkstemp()
    jvm_options_file = open(temp_path, 'w')

    # JVM GENERAL OPTIONS
    jvm_options_file.write('-XX:+PerfDisableSharedMem\n')
    jvm_options_file.write('-XX:-UsePerfData\n')
    jvm_options_file.write('-XX:+UseG1GC\n')
    jvm_options_file.write('-XX:+UseThreadPriorities\n')
    jvm_options_file.write('-XX:ThreadPriorityPolicy=42\n')
    if debug or log_level == 'debug':
        jvm_options_file.write('-Dlog4j.configurationFile=' + compss_home + '/Runtime/configuration/log/COMPSsMaster-log4j.debug\n')  # DEBUG
    elif monitor is not None or log_level == 'info':
        jvm_options_file.write('-Dlog4j.configurationFile=' + compss_home + '/Runtime/configuration/log/COMPSsMaster-log4j.info\n')  # INFO
    else:
        jvm_options_file.write('-Dlog4j.configurationFile=' + compss_home + '/Runtime/configuration/log/COMPSsMaster-log4j\n')  # NO DEBUG
    jvm_options_file.write('-Dcompss.to.file=false\n')
    jvm_options_file.write('-Dcompss.project.file=' + project_xml + '\n')
    jvm_options_file.write('-Dcompss.resources.file=' + resources_xml + '\n')
    jvm_options_file.write('-Dcompss.project.schema=' + compss_home + '/Runtime/configuration/xml/projects/project_schema.xsd\n')
    jvm_options_file.write('-Dcompss.resources.schema=' + compss_home + '/Runtime/configuration/xml/resources/resources_schema.xsd\n')
    jvm_options_file.write('-Dcompss.lang=python\n')
    if summary:
        jvm_options_file.write('-Dcompss.summary=true\n')
    else:
        jvm_options_file.write('-Dcompss.summary=false\n')
    jvm_options_file.write('-Dcompss.task.execution=' + task_execution + '\n')
    if storage_conf is None:
        jvm_options_file.write('-Dcompss.storage.conf=null\n')
    else:
        jvm_options_file.write('-Dcompss.storage.conf=' + storage_conf + '\n')

    jvm_options_file.write('-Dcompss.core.count=' + str(task_count) + '\n')

    jvm_options_file.write('-Dcompss.appName=' + app_name + '\n')

    if uuid is None:
        import uuid
        my_uuid = str(uuid.uuid4())
    else:
        my_uuid = uuid

    jvm_options_file.write('-Dcompss.uuid=' + my_uuid + '\n')

    if base_log_dir is None:
        # it will be within $HOME/.COMPSs
        jvm_options_file.write('-Dcompss.baseLogDir=\n')
    else:
        jvm_options_file.write('-Dcompss.baseLogDir=' + base_log_dir + '\n')

    if specific_log_dir is None:
        jvm_options_file.write('-Dcompss.specificLogDir=\n')
    else:
        jvm_options_file.write('-Dcompss.specificLogDir=' + specific_log_dir + '\n')

    jvm_options_file.write('-Dcompss.appLogDir=/tmp/' + my_uuid + '/\n')

    if graph:
        jvm_options_file.write('-Dcompss.graph=true\n')
    else:
        jvm_options_file.write('-Dcompss.graph=false\n')

    if monitor is None:
        jvm_options_file.write('-Dcompss.monitor=0\n')
    else:
        jvm_options_file.write('-Dcompss.monitor=' + str(monitor) + '\n')

    if not trace or trace == 0:
        jvm_options_file.write('-Dcompss.tracing=0' + '\n')
    elif trace == 1:
        jvm_options_file.write('-Dcompss.tracing=1\n')
        os.environ['EXTRAE_CONFIG_FILE'] = compss_home + '/Runtime/configuration/xml/tracing/extrae_basic.xml'
    elif trace == 2:
        jvm_options_file.write('-Dcompss.tracing=2\n')
        os.environ['EXTRAE_CONFIG_FILE'] = compss_home + '/Runtime/configuration/xml/tracing/extrae_advanced.xml'
    else:
        jvm_options_file.write('-Dcompss.tracing=0' + '\n')

    if extrae_cfg is None:
        jvm_options_file.write('-Dcompss.extrae.file=null\n')
    else:
        jvm_options_file.write('-Dcompss.extrae.file=' + extrae_cfg + '\n')

    if comm == 'GAT':
        jvm_options_file.write('-Dcompss.comm=es.bsc.compss.gat.master.GATAdaptor\n')
    else:
        jvm_options_file.write('-Dcompss.comm=es.bsc.compss.nio.master.NIOAdaptor\n')

    jvm_options_file.write('-Dcompss.conn=' + conn + '\n')
    jvm_options_file.write('-Dcompss.masterName=' + master_name + '\n')
    jvm_options_file.write('-Dcompss.masterPort=' + master_port + '\n')
    jvm_options_file.write('-Dcompss.scheduler=' + scheduler + '\n')
    jvm_options_file.write('-Dgat.adaptor.path=' + compss_home + '/Dependencies/JAVA_GAT/lib/adaptors\n')
    if debug:
        jvm_options_file.write('-Dgat.debug=true\n')
    else:
        jvm_options_file.write('-Dgat.debug=false\n')
    jvm_options_file.write('-Dgat.broker.adaptor=sshtrilead\n')
    jvm_options_file.write('-Dgat.file.adaptor=sshtrilead\n')
    jvm_options_file.write('-Dcompss.worker.cp=' + cp + ':' + compss_home + '/Runtime/compss-engine.jar:' + classpath + '\n')
    jvm_options_file.write('-Dcompss.worker.jvm_opts=' + jvm_workers + '\n')
    jvm_options_file.write('-Dcompss.worker.cpu_affinity=' + cpu_affinity + '\n')
    jvm_options_file.write('-Dcompss.worker.gpu_affinity=' + gpu_affinity + '\n')
    jvm_options_file.write('-Dcompss.worker.fpga_affinity=' + fpga_affinity + '\n')
    jvm_options_file.write('-Dcompss.worker.fpga_reprogram=' + fpga_reprogram + '\n')
    jvm_options_file.write('-Dcompss.profile.input=' + profile_input + '\n')
    jvm_options_file.write('-Dcompss.profile.output=' + profile_output + '\n')
    jvm_options_file.write('-Dcompss.scheduler.config=' + scheduler_config + '\n')
    jvm_options_file.write('-Dcompss.external.adaptation=' + external_adaptation + '\n')

    # JVM OPTIONS - PYTHON
    jvm_options_file.write('-Djava.class.path=' + cp + ':' + compss_home + '/Runtime/compss-engine.jar:' + classpath + '\n')
    jvm_options_file.write('-Djava.library.path=' + ld_library_path + '\n')
    jvm_options_file.write('-Dcompss.worker.pythonpath=' + cp + ':' + pythonpath + '\n')
    jvm_options_file.write('-Dcompss.python.interpreter=' + python_interpreter + '\n')
    jvm_options_file.write('-Dcompss.python.version=' + python_version + '\n')
    jvm_options_file.write('-Dcompss.python.virtualenvironment=' + python_virtual_environment + '\n')
    if propagate_virtual_environment:
        jvm_options_file.write('-Dcompss.python.propagate_virtualenvironment=true\n')
    else:
        jvm_options_file.write('-Dcompss.python.propagate_virtualenvironment=false\n')

    # Uncomment for debugging purposes
    # jvm_options_file.write('-Xcheck:jni\n')
    # jvm_options_file.write('-verbose:jni\n')

    # Close the file
    jvm_options_file.close()
    os.close(fd)
    os.environ['JVM_OPTIONS_FILE'] = temp_path

    # print("Uncomment if you want to check the configuration file path.")
    # print("JVM_OPTIONS_FILE: %s" % temp_path)
