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

"""PyCOMPSs Testbench for functions."""

from pycompss.functions.data import generator


def check_generator():
    """Check the generator function.

    :returns: None.
    """
    random_data = generator((12, 12), 4, 5, "random", True)
    normal_data = generator((12, 12), 4, 5, "normal", True)
    uniform_data = generator((12, 12), 4, 5, "uniform", True)

    assert (
        random_data != normal_data != uniform_data
    ), "The generator did not produce different data for different distributions"  # noqa: E501


def main():
    """Execute all function tests.

    :returns: None.
    """
    check_generator()


# Uncomment for command line check:
# if __name__ == "__main__":
#     main()
