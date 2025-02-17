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
import sys

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor


def test_simple_notebook():
    current_path = os.path.dirname(os.path.abspath(__file__))
    simple_notebook = os.path.join(
        current_path, "../integration", "resources", "notebook", "simple.ipynb"
    )
    with open(simple_notebook) as f:
        nb = nbformat.read(f, as_version=4)
    if sys.version_info < (3, 0):
        raise Exception("Unsupported python version. Required Python 3.X")
    else:
        ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb)
