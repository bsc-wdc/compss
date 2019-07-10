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


def build_return_params_message(types, values):
    """
    Build the return message with the parameters output.

    :param types: List of the parameter's types
    :param values: List of the parameter's values
    :return: Message as string
    """

    assert len(types) == len(values), 'Inconsistent state: return type-value length mismatch for return message.'

    pairs = list(zip(types, values))
    num_params = len(pairs)
    params = ''
    for pair in pairs:
        params = params + str(pair[0]) + ' ' + str(pair[1]) + ' '
    message = str(num_params) + ' ' + params
    return message
