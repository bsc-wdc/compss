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

from pycompss.tests.integration.worker.common_piper_tester import (
    evaluate_piper_worker_common,
)
from pycompss.tests.integration.worker.common_piper_tester import setup_argv


def worker_thread(argv, current_path):
    from pycompss.worker.piper.piper_worker import main

    # Start the piper worker
    setup_argv(argv, current_path)
    main()


def test_piper_worker():
    evaluate_piper_worker_common(worker_thread, mpi_worker=False)
