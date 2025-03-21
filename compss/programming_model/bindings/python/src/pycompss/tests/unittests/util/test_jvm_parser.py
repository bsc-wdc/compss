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
import shutil
import tempfile

from pycompss.util.exceptions import PyCOMPSsException


def test_jvm_parser():
    from pycompss.util.jvm.parser import convert_to_dict

    jvm_opt_file = tempfile.NamedTemporaryFile(delete=False).name
    temp_folder = tempfile.mkdtemp()
    jvm_expected_result = {
        "+PerfDisableSharedMem": True,
        "-UsePerfData": True,
        "+UseG1GC": True,
        "+UseThreadPriorities": True,
        "ThreadPriorityPolicy=42": True,
        "-Dlog4j.configurationFile": "/opt/COMPSs/Runtime/configuration/log/COMPSsMaster-log4j.debug",  # noqa: E501
        "-Dcompss.to.file": "false",
        "-Dcompss.project.file": "/opt/COMPSs/Runtime/configuration/xml/projects/default_project.xml",  # noqa: E501
        "-Dcompss.resources.file": "/opt/COMPSs/Runtime/configuration/xml/resources/default_resources.xml",  # noqa: E501
        "-Dcompss.project.schema": "/opt/COMPSs/Runtime/configuration/xml/projects/project_schema.xsd",  # noqa: E501
        "-Dcompss.resources.schema": "/opt/COMPSs/Runtime/configuration/xml/resources/resources_schema.xsd",  # noqa: E501
        "-Dcompss.lang": "python",
        "-Dcompss.summary": "false",
        "-Dcompss.task.execution": "compss",
        "-Dcompss.storage.conf": "null",
        "-Dcompss.streaming": "null",
        "-Dcompss.streaming.masterName": "null",
        "-Dcompss.streaming.masterPort": "null",
        "-Dcompss.core.count": "50",
        "-Dcompss.appName": "increment",
        "-Dcompss.uuid": "dc126fe7-1b0a-4360-80f2-55c815e2e604",
        "-Dcompss.baseLogDir": "",
        "-Dcompss.specificLogDir": "",
        "-Dcompss.appLogDir": temp_folder,
        "-Dcompss.graph": "false",
        "-Dcompss.monitor": "0",
        "-Dcompss.tracing": "0",
        "-Dcompss.extrae.file": "null",
        "-Dcompss.comm": "es.bsc.compss.nio.master.NIOAdaptor",
        "-Dcompss.conn": "es.bsc.compss.connectors.DefaultSSHConnector",
        "-Dcompss.masterName": "",
        "-Dcompss.masterPort": "",
        "-Dcompss.scheduler": "es.bsc.compss.scheduler.lookahead.locality.LocalityTS",  # noqa: E501
        "-Dgat.adaptor.path": "/opt/COMPSs/Dependencies/JAVA_GAT/lib/adaptors",
        "-Dgat.debug": "true",
        "-Dgat.broker.adaptor": "sshtrilead",
        "-Dgat.file.adaptor": "sshtrilead",
        "-Dcompss.worker.cp": "/home/user/gitlab/framework/compss/programming_model/bindings/python/src/pycompss/tests/runtime/../resources:/opt/COMPSs/Runtime/compss-engine.jar::/opt/COMPSs/Runtime/compss-engine.jar",  # noqa: E501
        "-Dcompss.worker.jvm_opts": "-Xms1024m,-Xmx1024m,-Xmn400m",
        "-Dcompss.worker.cpu_affinity": "automatic",
        "-Dcompss.worker.gpu_affinity": "automatic",
        "-Dcompss.worker.fpga_affinity": "automatic",
        "-Dcompss.worker.fpga_reprogram": "",
        "-Dcompss.profile.input": "",
        "-Dcompss.profile.output": "",
        "-Dcompss.scheduler.config": "",
        "-Dcompss.external.adaptation": "false",
        "-Djava.class.path": "/home/user/gitlab/framework/compss/programming_model/bindings/python/src/pycompss/tests/runtime/../resources:/opt/COMPSs/Runtime/compss-engine.jar::/opt/COMPSs/Runtime/compss-engine.jar",  # noqa: E501
        "-Djava.library.path": "/opt/COMPSs/Bindings/bindings-common/lib/:/opt/COMPSs/Runtime/compss-engine.jar:/usr/lib64/jvm/java-1.8.0/jre/lib/amd64/server/:/usr/lib64/mpi/gcc/openmpi/lib64/:/opt/COMPSs/Bindings/bindings-common/lib/:/opt/COMPSs/Runtime/compss-engine.jar:/usr/lib64/jvm/java-1.8.0/jre/lib/amd64/server/:/usr/lib64/mpi/gcc/openmpi/lib64/:/usr/lib64/mpi/gcc/openmpi/lib64::/opt/COMPSs/Bindings/bindings-common/lib:/usr/lib64/jvm/java/jre/lib/amd64/server",  # noqa: E501
        "-Dcompss.worker.pythonpath": "/home/user/gitlab/framework/compss/programming_model/bindings/python/src/pycompss/tests/runtime/../resources:/home/user/gitlab/framework/compss/programming_model/bindings/python:.:/opt/COMPSs/Bindings/python/:/opt/COMPSs/Bindings/bindings-common/lib/:/opt/COMPSs/Bindings/python/:/opt/COMPSs/Bindings/bindings-common/lib/:",  # noqa: E501
        "-Dcompss.python.interpreter": "python3",
        "-Dcompss.python.version": "3",
        "-Dcompss.python.virtualenvironment": "null",
        "-Dcompss.python.propagate_virtualenvironment": "true",
        "-Dcompss.python.mpi_worker": "false",
        "other": True,
    }
    with open(jvm_opt_file, "w") as f_jvm:
        f_jvm.write(
            """-XX:+PerfDisableSharedMem
-XX:-UsePerfData
-XX:+UseG1GC
-XX:+UseThreadPriorities
-XX:ThreadPriorityPolicy=42
-Dlog4j.configurationFile=/opt/COMPSs/Runtime/configuration/log/COMPSsMaster-log4j.debug
-Dcompss.to.file=false
-Dcompss.project.file=/opt/COMPSs/Runtime/configuration/xml/projects/default_project.xml
-Dcompss.resources.file=/opt/COMPSs/Runtime/configuration/xml/resources/default_resources.xml
-Dcompss.project.schema=/opt/COMPSs/Runtime/configuration/xml/projects/project_schema.xsd
-Dcompss.resources.schema=/opt/COMPSs/Runtime/configuration/xml/resources/resources_schema.xsd
-Dcompss.lang=python
-Dcompss.summary=false
-Dcompss.task.execution=compss
-Dcompss.storage.conf=null
-Dcompss.streaming=null
-Dcompss.streaming.masterName=null
-Dcompss.streaming.masterPort=null
-Dcompss.core.count=50
-Dcompss.appName=increment
-Dcompss.uuid=dc126fe7-1b0a-4360-80f2-55c815e2e604
-Dcompss.baseLogDir=
-Dcompss.specificLogDir=
-Dcompss.appLogDir={0}
-Dcompss.graph=false
-Dcompss.monitor=0
-Dcompss.tracing=0
-Dcompss.extrae.file=null
-Dcompss.comm=es.bsc.compss.nio.master.NIOAdaptor
-Dcompss.conn=es.bsc.compss.connectors.DefaultSSHConnector
-Dcompss.masterName=
-Dcompss.masterPort=
-Dcompss.scheduler=es.bsc.compss.scheduler.lookahead.locality.LocalityTS
-Dgat.adaptor.path=/opt/COMPSs/Dependencies/JAVA_GAT/lib/adaptors
-Dgat.debug=true
-Dgat.broker.adaptor=sshtrilead
-Dgat.file.adaptor=sshtrilead
-Dcompss.worker.cp=/home/user/gitlab/framework/compss/programming_model/bindings/python/src/pycompss/tests/runtime/../resources:/opt/COMPSs/Runtime/compss-engine.jar::/opt/COMPSs/Runtime/compss-engine.jar
-Dcompss.worker.jvm_opts=-Xms1024m,-Xmx1024m,-Xmn400m
-Dcompss.worker.cpu_affinity=automatic
-Dcompss.worker.gpu_affinity=automatic
-Dcompss.worker.fpga_affinity=automatic
-Dcompss.worker.fpga_reprogram=
-Dcompss.profile.input=
-Dcompss.profile.output=
-Dcompss.scheduler.config=
-Dcompss.external.adaptation=false
-Djava.class.path=/home/user/gitlab/framework/compss/programming_model/bindings/python/src/pycompss/tests/runtime/../resources:/opt/COMPSs/Runtime/compss-engine.jar::/opt/COMPSs/Runtime/compss-engine.jar
-Djava.library.path=/opt/COMPSs/Bindings/bindings-common/lib/:/opt/COMPSs/Runtime/compss-engine.jar:/usr/lib64/jvm/java-1.8.0/jre/lib/amd64/server/:/usr/lib64/mpi/gcc/openmpi/lib64/:/opt/COMPSs/Bindings/bindings-common/lib/:/opt/COMPSs/Runtime/compss-engine.jar:/usr/lib64/jvm/java-1.8.0/jre/lib/amd64/server/:/usr/lib64/mpi/gcc/openmpi/lib64/:/usr/lib64/mpi/gcc/openmpi/lib64::/opt/COMPSs/Bindings/bindings-common/lib:/usr/lib64/jvm/java/jre/lib/amd64/server
-Dcompss.worker.pythonpath=/home/user/gitlab/framework/compss/programming_model/bindings/python/src/pycompss/tests/runtime/../resources:/home/user/gitlab/framework/compss/programming_model/bindings/python:.:/opt/COMPSs/Bindings/python/:/opt/COMPSs/Bindings/bindings-common/lib/:/opt/COMPSs/Bindings/python/:/opt/COMPSs/Bindings/bindings-common/lib/:
-Dcompss.python.interpreter=python3
-Dcompss.python.version=3
-Dcompss.python.virtualenvironment=null
-Dcompss.python.propagate_virtualenvironment=true
-Dcompss.python.mpi_worker=false
other
""".format(
                temp_folder
            )  # noqa
        )
    result = convert_to_dict(jvm_opt_file)
    assert len(result) == len(
        jvm_expected_result
    ), "The sizes of the dictionaries does not match"
    for k, v in jvm_expected_result.items():
        if k not in result:
            raise PyCOMPSsException(
                "Key: %s is not in the result dictionary" % k
            )
        assert (
            v == result[k]
        ), "The value of key: %s does not match the expected value: %s" % (
            k,
            str(v),
        )
    assert (
        result == jvm_expected_result
    ), "The jvm opts file has not been parsed as expected"
    os.remove(jvm_opt_file)
    shutil.rmtree(temp_folder)
