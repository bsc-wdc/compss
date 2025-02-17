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

"""
PyCOMPSs Tests - Integration - DDS - Examples.

This file contains the dds integration examples tests.
"""

import os
import shutil
import sys
import tempfile
import time

from pycompss.dds.examples import inverted_indexing
from pycompss.dds.examples import pi_estimation
from pycompss.dds.examples import terasort
from pycompss.dds.examples import transitive_closure
from pycompss.dds.examples import word_count
from pycompss.dds.examples import wordcount_k_means
from pycompss.runtime.binding import barrier
from pycompss.util.context import CONTEXT

# The DDS examples unittest only checks functionality (not the validity of
# the results).
# TODO: check that the results are OK.

EXAMPLES_NAME = "examples.py"


def pi_estimation_example():
    """Run the pi estimation example.

    :returns: None.
    """
    pi_estimation()


def transitive_closure_example():
    """Run the transitive closure example.

    :returns: None.
    """
    transitive_closure(2)


def wordcount_example():
    """Run the wordcount example.

    :returns: None.
    """
    current_path = os.path.dirname(os.path.abspath(__file__))
    wordcount_dataset_path = os.path.join(
        current_path, "../../../unittests/dds/dataset", "wordcount"
    )
    argv_backup = sys.argv
    sys.argv = [EXAMPLES_NAME, wordcount_dataset_path]
    word_count()
    sys.argv = argv_backup


def wordcount_k_means_example():
    """Run the wordcount K-means example.

    :returns: None.
    """
    if sys.version_info >= (3, 0):
        current_path = os.path.dirname(os.path.abspath(__file__))
        wordcount_k_means_dataset_path = os.path.join(
            current_path, "../../../unittests/dds/dataset", "wordcount"
        )
        argv_backup = sys.argv
        sys.argv = [EXAMPLES_NAME, wordcount_k_means_dataset_path]
        wordcount_k_means(dim=155)
        sys.argv = argv_backup
    else:
        print("NOTE: Spacy fails in Python 2 [deprecating].")


def terasort_example():
    """Run the terasort example.

    :returns: None.
    """
    result_path = tempfile.mkdtemp()
    current_path = os.path.dirname(os.path.abspath(__file__))
    terasort_dataset_path = os.path.join(
        current_path, "../../../unittests/dds/dataset", "terasort"
    )
    argv_backup = sys.argv
    sys.argv = [EXAMPLES_NAME, terasort_dataset_path, result_path]
    terasort()
    sys.argv = argv_backup
    if CONTEXT.in_pycompss():
        barrier()
        time.sleep(5)  # TODO: Why is this sleep needed?
    # Clean the results directory
    shutil.rmtree(result_path)


def inverted_indexing_example():
    """Run the inverted indexing example.

    :returns: None.
    """
    current_path = os.path.dirname(os.path.abspath(__file__))
    inverted_indexing_dataset_path = os.path.join(
        current_path, "../../../unittests/dds/dataset", "wordcount"
    )
    argv_backup = sys.argv
    sys.argv = [EXAMPLES_NAME, inverted_indexing_dataset_path]
    inverted_indexing()
    sys.argv = argv_backup


def main():
    """Run all examples.

    :returns: None.
    """
    # Examples to run
    pi_estimation_example()
    transitive_closure_example()
    wordcount_example()
    # Next test @unittest.skip("ERROR WITH SPACY (python 3.10 + spacy 2.3.2)")
    # wordcount_k_means_example()
    terasort_example()
    inverted_indexing_example()
