#!/usr/bin/python
#
#  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Worker Constants
=========================
    This file contains a set of constants used from the workers.
"""

# ###################### #
#  Tracing events Codes  #
# ###################### #
# Should be equal to Tracer.java definitions
SYNC_EVENTS = 8000666
TASK_EVENTS = 60000100
WORKER_EVENTS = 60000200
WORKER_RUNNING_EVENT = 1
PROCESS_TASK_EVENT = 2              # process_task
PROCESS_PING_EVENT = 3              # process_ping
PROCESS_QUIT_EVENT = 4              # process_quit
LOAD_LOGGERS_EVENT = 5              # load_loggers
INIT_STORAGE_EVENT = 6              # init_storage
STOP_STORAGE_EVENT = 7              # stop_storage
INIT_STORAGE_AT_WORKER_EVENT = 8    # initStorageAtWorker
FINISH_STORAGE_AT_WORKER_EVENT = 9  # finishStorageAtWorker
INIT_WORKER_POSTFORK_EVENT = 10     # initWorkerPostFork
FINISH_WORKER_POSTFORK_EVENT = 11   # finishWorkerPostFork
