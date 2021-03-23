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

import os
import sys
import time
import shutil
import tempfile

from pycompss.runtime.binding import barrier
from pycompss.util.context import in_pycompss
from pycompss.dds.examples import pi_estimation
from pycompss.dds.examples import transitive_closure
from pycompss.dds.examples import word_count
from pycompss.dds.examples import wordcount_k_means
from pycompss.dds.examples import terasort
from pycompss.dds.examples import inverted_indexing


# The DDS examples unittest only checks functionality (not the validity of the results).
# TODO: check that the results are OK.

EXAMPLES_NAME = "examples.py"


def test_pi_estimation_example():
    pi_estimation()


def test_transitive_closure_example():
    transitive_closure(2)


def test_wordcount_example():
    current_path = os.path.dirname(os.path.abspath(__file__))
    wordcount_dataset_path = os.path.join(current_path, "dataset", "wordcount")
    argv_backup = sys.argv
    sys.argv = [EXAMPLES_NAME, wordcount_dataset_path]
    word_count()
    sys.argv = argv_backup


def test_wordcount_k_means_example():
    if sys.version_info >= (3, 0):
        current_path = os.path.dirname(os.path.abspath(__file__))
        wordcount_k_means_dataset_path = os.path.join(current_path,
                                                      "dataset", "wordcount")
        argv_backup = sys.argv
        sys.argv = [EXAMPLES_NAME, wordcount_k_means_dataset_path]
        wordcount_k_means(dim=155)
        sys.argv = argv_backup
    else:
        print("NOTE: Spacy fails in Python 2 [deprecating].")


def test_terasort_example():
    result_path = tempfile.mkdtemp()
    current_path = os.path.dirname(os.path.abspath(__file__))
    terasort_dataset_path = os.path.join(current_path, "dataset", "terasort")
    argv_backup = sys.argv
    sys.argv = [EXAMPLES_NAME, terasort_dataset_path, result_path]
    terasort()
    sys.argv = argv_backup
    if in_pycompss():
        barrier()
        time.sleep(5)  # TODO: Why is this sleep needed?
    # Clean the results directory
    shutil.rmtree(result_path)


def test_inverted_indexing_example():
    current_path = os.path.dirname(os.path.abspath(__file__))
    inverted_indexing_dataset_path = os.path.join(current_path,
                                                  "dataset", "wordcount")
    argv_backup = sys.argv
    sys.argv = [EXAMPLES_NAME, inverted_indexing_dataset_path]
    inverted_indexing()
    sys.argv = argv_backup


def main():

    # Examples to run
    test_pi_estimation_example()
    test_transitive_closure_example()
    test_wordcount_example()
    test_wordcount_k_means_example()
    test_terasort_example()
    test_inverted_indexing_example()

