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
PyCOMPSs Worker - Piper - Commons - Utils logger.

This file contains the common pipers methods related to logging.
"""

import logging

from pycompss.runtime.commons import GLOBALS
from pycompss.util.logger.helpers import init_logging_worker_piper
from pycompss.util.logger.remittent import LOG_REMITTENT
from pycompss.util.logger.level import LOG_LEVEL
from pycompss.util.typing_helper import typing


def load_loggers(
    debug: bool, persistent_storage: bool
) -> typing.Tuple[logging.Logger, typing.List[logging.Logger], str]:
    """Load all loggers.

    :param debug: Is Debug enabled.
    :param persistent_storage: Is persistent storage enabled.
    :return: Main logger of the application, the log config file (json),
             a list of loggers for the persistent data framework, and
             the temporary log directory.
    """
    # log_dir is of the form:
    #    With agents or worker in master:
    #        /path/to/working_directory/tmpFiles/pycompssID/../../log
    #    Normal master-worker execution :
    #        /path/to/working_directory/machine_name/pycompssID/../log
    # With normal master-worker execution, it transfers the err and out
    # files in the expected folder to the master.
    # With agents or worker in master it does not, so keep it in previous
    # two folders:
    log_dir = GLOBALS.get_log_directory()
    if not log_dir:
        if __debug__:
            print(
                "WARNING: Log dir not set, "
                + "using temporary directory as log dir."
            )
        log_dir = GLOBALS.get_temporary_directory()

    # Load log level configuration file
    if debug:
        # Debug
        init_logging_worker_piper(
            LOG_REMITTENT.WORKER, LOG_LEVEL.DEBUG, log_dir
        )
    else:
        # Default
        init_logging_worker_piper(LOG_REMITTENT.WORKER, LOG_LEVEL.OFF, log_dir)

    # Define logger facilities
    logger = logging.getLogger("pycompss.worker.piper.piper_worker")
    storage_loggers = []
    if persistent_storage:
        storage_loggers.append(logging.getLogger("dataclay"))
        storage_loggers.append(logging.getLogger("hecuba"))
        storage_loggers.append(logging.getLogger("redis"))
        storage_loggers.append(logging.getLogger("storage"))
    return logger, storage_loggers, log_dir
