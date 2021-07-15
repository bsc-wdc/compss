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
PyCOMPSs Common piper utils
===========================
    This file contains the common pipers methods.
"""

import os
import logging
from pycompss.runtime.commons import range
from pycompss.runtime.commons import set_temporary_directory
from pycompss.runtime.commons import get_temporary_directory
from pycompss.util.logger.helpers import init_logging_worker_piper
from pycompss.worker.piper.commons.constants import HEADER
from pycompss.worker.piper.commons.executor import Pipe
import pycompss.util.context as context


class PiperWorkerConfiguration(object):
    """
    Description of the configuration parameters for the Piper Worker.
    """

    __slots__ = ['nesting', 'debug', 'tracing', 'storage_conf',
                 'stream_backend', 'stream_master_name', 'stream_master_port',
                 'tasks_x_node', 'pipes', 'control_pipe', 'cache']

    def __init__(self):
        """
        Constructs an empty configuration description for the piper worker.
        """
        self.nesting = False
        self.debug = False
        self.tracing = False
        self.storage_conf = None
        self.stream_backend = None
        self.stream_master_name = None
        self.stream_master_port = None
        self.tasks_x_node = 0
        self.pipes = []
        self.control_pipe = None
        self.cache = False

    def update_params(self, argv):
        # type: (list) -> None
        """ Constructs a configuration description for the piper worker using
        the arguments.

        :param argv: arguments from the command line.
        :return: None
        """
        set_temporary_directory(argv[1], create_tmpdir=False)
        if argv[2] == 'true':
            context.enable_nesting()
            self.nesting = True
        self.debug = argv[3] == 'true'
        self.tracing = argv[4] == '1'
        self.storage_conf = argv[5]
        self.stream_backend = argv[6]
        self.stream_master_name = argv[7]
        self.stream_master_port = argv[8]
        self.cache = argv[9]
        self.tasks_x_node = int(argv[10])
        in_pipes = argv[11:11 + self.tasks_x_node]
        out_pipes = argv[11 + self.tasks_x_node:-2]
        if self.debug:
            assert self.tasks_x_node == len(in_pipes)
            assert self.tasks_x_node == len(out_pipes)
        self.pipes = []
        for i in range(0, self.tasks_x_node):
            self.pipes.append(Pipe(in_pipes[i], out_pipes[i]))
        self.control_pipe = Pipe(argv[-2], argv[-1])

    def print_on_logger(self, logger):
        # type: (...) -> None
        """ Prints the configuration through the given logger.

        :param logger: logger to output the configuration.
        :return: None
        """
        logger.debug(HEADER + "-----------------------------")
        logger.debug(HEADER + "Persistent worker parameters:")
        logger.debug(HEADER + "-----------------------------")
        logger.debug(HEADER + "Nesting        : " + str(self.nesting))
        logger.debug(HEADER + "Debug          : " + str(self.debug))
        logger.debug(HEADER + "Tracing        : " + str(self.tracing))
        logger.debug(HEADER + "Cache          : " + str(self.cache))
        logger.debug(HEADER + "Tasks per node : " + str(self.tasks_x_node))
        logger.debug(HEADER + "Pipe Pairs     : ")
        for pipe in self.pipes:
            logger.debug(HEADER + "                 * " + str(pipe))
        logger.debug(HEADER + "Storage conf.  : " + str(self.storage_conf))
        logger.debug(HEADER + "-----------------------------")


def load_loggers(debug, persistent_storage):
    # type: (bool, str) -> (..., str, ...)
    """ Load all loggers.

    :param debug: is Debug enabled.
    :param persistent_storage: is persistent storage enabled.
    :return: main logger of the application, the log config file (json) and
             a list of loggers for the persistent data framework.
    """
    # Load log level configuration file
    worker_path = os.path.dirname(os.path.realpath(__file__))
    log_cfg_path = "".join((worker_path, '/../../../../log'))
    if not os.path.isdir(log_cfg_path):
        # If not exists, then we are using the source for unit testing
        log_cfg_path = "".join((worker_path, '/../../../../../log'))
    if debug:
        # Debug
        log_json = "/".join((log_cfg_path, 'logging_worker_debug.json'))
    else:
        # Default
        log_json = "/".join((log_cfg_path, 'logging_worker_off.json'))
    log_dir = get_temporary_directory()
    # log_dir is of the form:
    #    With agents or worker in master: /path/to/working_directory/tmpFiles/pycompssID/
    #    Normal master-worker execution : /path/to/working_directory/machine_name/pycompssID/
    # With normal master-worker execution, it transfers the err and out files in the
    # expected folder to the master.
    # With agents or worker in master it does not, so keep it in previous two folders:
    if context.is_nesting_enabled() or "tmpFiles" in log_dir:
        log_dir = os.path.join(log_dir, "..", "..")
    else:
        log_dir = os.path.join(log_dir, "..")
    init_logging_worker_piper(log_json, log_dir)

    # Define logger facilities
    logger = logging.getLogger('pycompss.worker.piper.piper_worker')
    storage_loggers = []
    if persistent_storage:
        storage_loggers.append(logging.getLogger('dataclay'))
        storage_loggers.append(logging.getLogger('hecuba'))
        storage_loggers.append(logging.getLogger('redis'))
        storage_loggers.append(logging.getLogger('storage'))
    return logger, log_json, storage_loggers
