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


# def test_data_generator():
#
#     # TODO: This must be tested with launch_pycompss_application
#
#     import pycompss.util.context as context
#     context.set_pycompss_context(context.OUT_OF_SCOPE)
#     from pycompss.functions.data import generator
#
#     size = (20, 2)
#     data_r = generator(size, 4, 0, 'random', True)
#     data_n = generator(size, 4, 0, 'normal', True)
#     data_u = generator(size, 4, 0, 'uniform', True)
#
#     context.set_pycompss_context(context.MASTER)
#
#     assert data_r != data_n != data_u, \
#         "ERROR: Got equal results when different were expected."


def test_data_chunks():
    from pycompss.functions.data import chunks
    data = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    expected_unbalanced = [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    chunked_unbalanced = list(chunks(data, 3, balanced=False))
    assert expected_unbalanced == chunked_unbalanced, \
        "ERROR: Got unexpected unbalanced chunking."
    expected_balanced = [[0, 1, 2, 3], [4, 5, 6], [7, 8, 9]]
    chunked_balanced = list(chunks(data, 3, balanced=True))
    assert expected_balanced == chunked_balanced, \
        "ERROR: Got unexpected balanced chunking."
