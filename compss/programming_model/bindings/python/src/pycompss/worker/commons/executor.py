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
PyCOMPSs Worker - Commons - Executor.

This file contains the common worker executor methods.
"""

from pycompss.api.parameter import TYPE


def build_return_params_message(types: list, values: list) -> str:
    """Build the return message with the parameters output.

    :param types: List of the parameter's types.
    :param values: List of the parameter's values.
    :return: Message as string.
    """
    err_msg = "return type-value length mismatch for return message."
    assert len(types) == len(values), "Inconsistent state: " + err_msg

    pairs = list(zip(types, values))
    num_params = len(pairs)
    params = [str(num_params)]
    for pair in pairs:
        value = str(pair[1])
        if pair[0] == TYPE.COLLECTION:
            value = value.replace(" ", "")
            value = value.replace("'", "")
        params.append(str(pair[0]))
        params.append(value)
    message = " ".join(params)
    return message
