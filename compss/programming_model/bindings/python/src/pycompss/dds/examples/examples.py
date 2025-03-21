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
PyCOMPSs DDS - Examples.

This file contains the DDS examples.
"""

import time

from pycompss.dds.examples.pi_estimation import pi_estimation
from pycompss.dds.examples.word_count import word_count
from pycompss.dds.examples.terasort import terasort
from pycompss.dds.examples.inverted_indexing import inverted_indexing
from pycompss.dds.examples.transitive_closure import transitive_closure
from pycompss.dds.examples.wordcount_k_means import wordcount_k_means


def run_examples():
    """Run available examples.

    Each example is in a single file that creates a sample dataset
    to be tested.

    :return: None.
    """
    print("________RUNNING EXAMPLES_________")
    start_time = time.time()

    pi_estimation_check = pi_estimation()
    word_count_check = word_count()
    terasort_check = terasort()
    inverted_indexing_check = inverted_indexing()
    transitive_closure_check = transitive_closure()
    wordcount_k_means_check = wordcount_k_means()
    print("____FINISHED RUNNING EXAMPLES____")
    print("STATUS:")
    print(f"- Pi estimation: {pi_estimation_check}")
    print(f"- Wordcount: {word_count_check}")
    print(f"- Terasort: {terasort_check}")
    print(f"- Inverted indexing: {inverted_indexing_check}")
    print(f"- Transitive closure: {transitive_closure_check}")
    print(f"- Wordcount k-means: {wordcount_k_means_check}")
    print("---------------------------------")
    print(f"- TOTAL ELAPSED TIME: {time.time() - start_time} (s)")
    print("---------------------------------")

    if (
        pi_estimation_check
        and word_count_check
        and terasort_check
        and inverted_indexing_check
        and transitive_closure_check
        and wordcount_k_means
    ):
        print("ALL EXAMPLES FINISHED SUCCESSFULLY")
    else:
        print("ERRORS WHERE FOUND IN ONE OR MORE EXAMPLES")

    print("---------------------------------")


if __name__ == "__main__":
    run_examples()
