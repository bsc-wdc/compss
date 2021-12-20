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
PyCOMPSs Common piper utils logger
==================================
    This file contains the common pipers methods related to logging.
"""

import os
import logging
from pycompss.util.typing_helper import typing
from pycompss.runtime.commons import get_temporary_directory
from pycompss.util.logger.helpers import init_logging_worker_piper
import pycompss.util.context as context


def load_loggers(debug, persistent_storage):
    # type: (bool, bool) -> typing.Tuple[typing.Any, str, typing.Any, str]
    """ Load all loggers.

    :param debug: is Debug enabled.
    :param persistent_storage: is persistent storage enabled.
    :return: main logger of the application, the log config file (json),
             a list of loggers for the persistent data framework, and
             the temporary log directory.
    """
    # Load log level configuration file
    worker_path = os.path.dirname(os.path.realpath(__file__))
    log_cfg_path = "".join((worker_path, "/../../../../log"))
    if not os.path.isdir(log_cfg_path):
        # If not exists, then we are using the source for unit testing
        log_cfg_path = "".join((worker_path, "/../../../../../log"))
    if debug:
        # Debug
        log_json = "/".join((log_cfg_path, "logging_worker_debug.json"))
    else:
        # Default
        log_json = "/".join((log_cfg_path, "logging_worker_off.json"))
    log_dir = get_temporary_directory()
    log_dir_temp = log_dir
    # log_dir is of the form:
    #    With agents or worker in master: /path/to/working_directory/tmpFiles/pycompssID/../../log
    #    Normal master-worker execution : /path/to/working_directory/machine_name/pycompssID/../log
    # With normal master-worker execution, it transfers the err and out files in the
    # expected folder to the master.
    # With agents or worker in master it does not, so keep it in previous two folders:
    if context.is_nesting_enabled() or "tmpFiles" in log_dir:
        log_dir = os.path.join(log_dir, "..", "..", "log")
    else:
        log_dir = os.path.join(log_dir, "..", "log")
    init_logging_worker_piper(log_json, log_dir)

    # Define logger facilities
    logger = logging.getLogger("pycompss.worker.piper.piper_worker")
    storage_loggers = []
    if persistent_storage:
        storage_loggers.append(logging.getLogger('dataclay'))
        storage_loggers.append(logging.getLogger('hecuba'))
        storage_loggers.append(logging.getLogger('redis'))
        storage_loggers.append(logging.getLogger('storage'))
    return logger, log_json, storage_loggers, log_dir_temp
