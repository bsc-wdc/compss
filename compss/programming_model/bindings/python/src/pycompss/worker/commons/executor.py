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
PyCOMPSs Worker - Commons - Executor.

This file contains the common worker executor methods.
"""

from pycompss.api.parameter import TYPE
from pycompss.util.exceptions import PyCOMPSsException


def build_return_params_message(types: list, values: list) -> str:
    """Build the return message with the parameters output.

    :param types: List of the parameter's types.
    :param values: List of the parameter's values.
    :return: Message as string.
    """
    err_msg = "return type-value length mismatch for return message."
    if len(types) != len(values):
        raise PyCOMPSsException(f"Inconsistent state: {err_msg}")

    pairs = list(zip(types, values))
    num_params = len(pairs)
    params = [str(num_params)]
    for pair in pairs:
        if pair[0] == TYPE.COLLECTION:
            value = __build_collection_representation(pair[1])
        else:
            value = str(pair[1])
        params.append(str(pair[0]))
        params.append(value)
    message = " ".join(params)
    return message


def __build_collection_representation(value: list) -> str:
    """Create the representation of a collection from the list of lists format.

    CAUTION: Recursive function.
    The runtime expects:
      [
        [
          [(t1, v1), (t2, v2), ...],
          [(t1, v1), (t2, v2), ...],
          ...
        ],
        ...
      ]

    :param value: Collection message before processing.
    :return: The collection representation message.
    """
    result = "["
    first = True
    for i in value:
        if isinstance(i[0], list):
            # Has inner list
            result = result + __build_collection_representation(i)
        else:
            coll_type = i[0]
            coll_value = i[1].replace("'", "")
            if first:
                result = f"{result}({coll_type},{coll_value})"
                first = False
            else:
                result = f"{result},({coll_type},{coll_value})"
    return f"{result}]"
