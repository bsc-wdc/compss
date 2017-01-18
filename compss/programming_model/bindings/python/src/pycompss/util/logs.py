#
#  Copyright 2.02-2017 Barcelona Supercomputing Center (www.bsc.es)
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
"""
@author: etejedor
@author: fconejer

PyCOMPSs Utils - logs
=====================
    This file contains all logging methods.
"""

import os
import sys
import logging
import json


if sys.version_info >= (2, 7):
    from logging import config
    config_func = config.dictConfig
else:
    import dictconfig
    config_func = dictconfig.dictConfig


def init_logging(log_config_file, storage_path):
    """
    Logging initialization.
    @param log_config_file: Log file name.
    """
    if os.path.exists(log_config_file):
        f = open(log_config_file, 'rt')
        conf = json.loads(f.read())
        errors_file = conf["handlers"]["error_file_handler"].get("filename")
        debug_file = conf["handlers"]["debug_file_handler"].get("filename")
        conf["handlers"]["error_file_handler"]["filename"] = storage_path + errors_file
        conf["handlers"]["debug_file_handler"]["filename"] = storage_path + debug_file
        config_func(conf)
    else:
        logging.basicConfig(level=logging.INFO)


def init_logging_worker(log_config_file):
    """
    Worker logging initialization.
    @param log_config_file: Log file name.
    """
    if os.path.exists(log_config_file):
        f = open(log_config_file, 'rt')
        conf = json.loads(f.read())
        config_func(conf)
    else:
        logging.basicConfig(level=logging.INFO)
