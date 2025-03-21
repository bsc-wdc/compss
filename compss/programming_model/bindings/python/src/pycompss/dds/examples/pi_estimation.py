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
PyCOMPSs DDS - Examples - pi estimation.

This file contains the DDS pi estimation example.
"""

import random
import time

from pycompss.dds import DDS


def inside(_):
    """Check if inside.

    :return: If inside.
    """
    rand_x = random.random()
    rand_y = random.random()
    return (rand_x * rand_x) + (rand_y * rand_y) < 1


def pi_estimation():
    """Pi estimation.

    Example is taken from: https://spark.apache.org/examples.html

    :return: If the pi value calculated is between 3.1 and 3.2.
    """
    start = time.time()

    print("--- PI ESTIMATION ---")

    print("- Estimating Pi by 'throwing darts' algorithm.")
    tries = 100000
    print(f"- Number of tries: {tries}")

    count = DDS().load(range(0, tries), 10).filter(inside).count()
    rough_pi = 4.0 * count / tries

    print(f"- Pi is roughly {rough_pi}")
    print("- Elapsed Time: ", time.time() - start)
    print("---------------------")

    return 3.1 < rough_pi < 3.2
