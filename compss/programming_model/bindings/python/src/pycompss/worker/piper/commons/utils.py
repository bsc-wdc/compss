#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Worker - Piper - Commons - Utils.

This file contains the common pipers methods.
"""

import logging

from pycompss.util.context import CONTEXT
from pycompss.runtime.commons import GLOBALS
from pycompss.util.typing_helper import typing
from pycompss.worker.piper.commons.constants import HEADER
from pycompss.worker.piper.commons.executor import Pipe


class PiperWorkerConfiguration:
    """Description of the configuration parameters for the Piper Worker class."""

    __slots__ = [
        "nesting",
        "debug",
        "tracing",
        "storage_conf",
        "stream_backend",
        "stream_master_name",
        "stream_master_port",
        "tasks_x_node",
        "exec_ids",
        "pipes",
        "control_pipe",
        "cache",
        "cache_profiler",
    ]

    def __init__(self) -> None:
        """Construct an empty configuration description for the piper worker.

        :returns: None.
        """
        self.nesting = False  # type: bool
        self.debug = False  # type: bool
        self.tracing = False  # type: bool
        self.storage_conf = ""  # type: str
        self.stream_backend = ""  # type: str
        self.stream_master_name = ""  # type: str
        self.stream_master_port = ""  # type: str
        self.tasks_x_node = 0  # type: int
        self.exec_ids = []  # type: typing.List[int]
        self.pipes = []  # type: typing.List[Pipe]
        self.control_pipe = Pipe()  # type: Pipe
        self.cache = False  # type: typing.Union[str, bool]
        self.cache_profiler = ""  # type: str

    def update_params(self, argv: typing.List[str]) -> None:
        """Construct a configuration description for the piper worker using the arguments.

        :param argv: Arguments from the command line.
        :return: None.
        """
        GLOBALS.set_temporary_directory(argv[1], create_tmpdir=False)
        if argv[2] == "true":
            CONTEXT.enable_nesting()
            self.nesting = True
        self.debug = argv[3] == "true"
        self.tracing = argv[4] == "true"
        self.storage_conf = argv[5]
        self.stream_backend = argv[6]
        self.stream_master_name = argv[7]
        self.stream_master_port = argv[8]
        self.cache = argv[9]
        self.cache_profiler = argv[10]
        self.tasks_x_node = int(argv[11])
        exec_ids = argv[12 : 12 + self.tasks_x_node]
        self.exec_ids = [int(exec_id) for exec_id in exec_ids]
        in_pipes = argv[12 + self.tasks_x_node : 12 + (self.tasks_x_node * 2)]
        out_pipes = argv[12 + (self.tasks_x_node * 2) : -2]
        if self.debug:
            assert self.tasks_x_node == len(in_pipes)
            assert self.tasks_x_node == len(out_pipes)
        self.pipes = []
        for i in range(0, self.tasks_x_node):
            self.pipes.append(Pipe(in_pipes[i], out_pipes[i]))
        self.control_pipe = Pipe(argv[-2], argv[-1])

    def print_on_logger(self, logger: logging.Logger) -> None:
        """Print the configuration through the given logger.

        :param logger: Logger to output the configuration.
        :return: None.
        """
        logger.debug(HEADER + "-----------------------------")
        logger.debug(HEADER + "Persistent worker parameters:")
        logger.debug(HEADER + "-----------------------------")
        logger.debug(HEADER + "Nesting        : " + str(self.nesting))
        logger.debug(HEADER + "Debug          : " + str(self.debug))
        logger.debug(HEADER + "Tracing        : " + str(self.tracing))
        logger.debug(HEADER + "Cache          : " + str(self.cache))
        logger.debug(HEADER + "Cache profiler : " + str(self.cache_profiler))
        logger.debug(HEADER + "Tasks per node : " + str(self.tasks_x_node))
        logger.debug(HEADER + "Exec ids       : ")
        for exec_id in self.exec_ids:
            logger.debug(HEADER + "                 * " + str(exec_id))
        logger.debug(HEADER + "Pipe Pairs     : ")
        for pipe in self.pipes:
            logger.debug(HEADER + "                 * " + str(pipe))
        logger.debug(HEADER + "Storage conf.  : " + str(self.storage_conf))
        logger.debug(HEADER + "Stream backend : " + str(self.stream_backend))
        logger.debug(HEADER + "Stream master  : " + str(self.stream_master_name))
        logger.debug(HEADER + "Stream port    : " + str(self.stream_master_port))
        logger.debug(HEADER + "-----------------------------")
