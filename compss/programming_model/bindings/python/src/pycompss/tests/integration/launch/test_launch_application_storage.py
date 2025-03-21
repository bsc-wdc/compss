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
import subprocess
import sys

STORAGE_CONF = "/tmp/storage.conf"  # NOSONAR
TEMP_DIR = "/tmp/PSCO"  # NOSONAR
JAVA_API_JAR = ""
STORAGE_API = None


def __initialize_storage() -> None:
    """Initializes the dummy storage backend.
    Compiles the dummy storage backend from the tests sources.
    Sets the JAVA_API_JAR on the first call to this initialization.

    :return: None
    """
    global JAVA_API_JAR
    global STORAGE_API
    current_path = os.path.dirname(os.path.abspath(__file__))
    # Add python storage api to sys.path
    STORAGE_API = os.path.join(current_path, "resources")
    sys.path.insert(0, STORAGE_API)
    if JAVA_API_JAR == "":
        # Compile jar
        jar_source_path = os.path.join(
            current_path,
            "..",
            "..",
            "..",
            "..",
            "..",
            "..",
            "..",
            "..",
            "..",
            "utils",
            "storage",
        )
        compile_command = ["mvn", "clean", "package"]
        process = subprocess.Popen(
            compile_command, stdout=subprocess.PIPE, cwd=jar_source_path
        )
        output, error = process.communicate()
        if error:
            print(output.decode())
            print(error.decode())
        # Set JAVA_API_JAR
        JAVA_API_JAR = os.path.join(
            jar_source_path, "dummyPSCO", "target", "compss-dummyPSCO.jar"
        )
        print("Storage api jar: " + JAVA_API_JAR)
    # Set global environment
    os.environ["CLASSPATH"] = JAVA_API_JAR + ":" + os.environ["CLASSPATH"]
    os.environ["PYTHONPATH"] = STORAGE_API + ":" + os.environ["PYTHONPATH"]
    # Prepare temporary directory
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)
    os.mkdir(TEMP_DIR)
    # Prepare storage configuration
    if not os.path.exists(STORAGE_CONF):
        with open(STORAGE_CONF, "w") as fd:
            fd.write("localhost")


def __clean_storage():
    # Remove python storage api from sys.path
    sys.path.pop(0)
    # Clean directories
    if os.path.exists(STORAGE_CONF):
        os.remove(STORAGE_CONF)
    if os.path.exists(TEMP_DIR):
        shutil.rmtree(TEMP_DIR)


def test_launch_application():
    from pycompss.runtime.launch import launch_pycompss_application

    global JAVA_API_JAR
    global STORAGE_API

    current_path = os.path.dirname(os.path.abspath(__file__))
    app = os.path.join(current_path, "resources", "storage_app", "pscos.py")
    __initialize_storage()
    launch_pycompss_application(
        app,
        "main",
        debug=True,
        app_name="pscos",
        storage_conf=STORAGE_CONF,
        storage_impl=JAVA_API_JAR,
        classpath=JAVA_API_JAR,
        pythonpath=STORAGE_API,
    )
    __clean_storage()
