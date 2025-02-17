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
PyCOMPSs runtime - Task - Keys.

This file contains the task keys.
"""


class ParamAliasKeys:  # pylint: disable=too-few-public-methods
    """Strings used in Tasks definition."""

    IN = "IN"
    OUT = "OUT"
    INOUT = "INOUT"
    CONCURRENT = "CONCURRENT"
    COMMUTATIVE = "COMMUTATIVE"
    IN_DELETE = "IN_DELETE"

    FILE = "FILE"
    FILE_IN = "FILE_IN"
    FILE_OUT = "FILE_OUT"
    FILE_INOUT = "FILE_INOUT"
    FILE_CONCURRENT = "FILE_CONCURRENT"
    FILE_COMMUTATIVE = "FILE_COMMUTATIVE"

    FILE_STDIN = "FILE_STDIN"
    FILE_STDERR = "FILE_STDERR"
    FILE_STDOUT = "FILE_STDOUT"

    FILE_IN_STDIN = "FILE_IN_STDIN"
    FILE_IN_STDERR = "FILE_IN_STDERR"
    FILE_IN_STDOUT = "FILE_IN_STDOUT"
    FILE_OUT_STDIN = "FILE_OUT_STDIN"
    FILE_OUT_STDERR = "FILE_OUT_STDERR"
    FILE_OUT_STDOUT = "FILE_OUT_STDOUT"
    FILE_INOUT_STDIN = "FILE_INOUT_STDIN"
    FILE_INOUT_STDERR = "FILE_INOUT_STDERR"
    FILE_INOUT_STDOUT = "FILE_INOUT_STDOUT"
    FILE_CONCURRENT_STDIN = "FILE_CONCURRENT_STDIN"
    FILE_CONCURRENT_STDERR = "FILE_CONCURRENT_STDERR"
    FILE_CONCURRENT_STDOUT = "FILE_CONCURRENT_STDOUT"
    FILE_COMMUTATIVE_STDIN = "FILE_COMMUTATIVE_STDIN"
    FILE_COMMUTATIVE_STDERR = "FILE_COMMUTATIVE_STDERR"
    FILE_COMMUTATIVE_STDOUT = "FILE_COMMUTATIVE_STDOUT"

    DIRECTORY = "DIRECTORY"
    DIRECTORY_IN = "DIRECTORY_IN"
    DIRECTORY_OUT = "DIRECTORY_OUT"
    DIRECTORY_INOUT = "DIRECTORY_INOUT"

    COLLECTION = "COLLECTION"
    COLLECTION_IN = "COLLECTION_IN"
    COLLECTION_INOUT = "COLLECTION_INOUT"
    COLLECTION_OUT = "COLLECTION_OUT"
    COLLECTION_IN_DELETE = "COLLECTION_IN_DELETE"
    COLLECTION_FILE = "COLLECTION_FILE"
    COLLECTION_FILE_IN = "COLLECTION_FILE_IN"
    COLLECTION_FILE_INOUT = "COLLECTION_FILE_INOUT"
    COLLECTION_FILE_OUT = "COLLECTION_FILE_OUT"

    DICT_COLLECTION = "DICT_COLLECTION"
    DICT_COLLECTION_IN = "DICT_COLLECTION_IN"
    DICT_COLLECTION_INOUT = "DICT_COLLECTION_INOUT"
    DICT_COLLECTION_OUT = "DICT_COLLECTION_OUT"
    DICT_COLLECTION_IN_DELETE = "DICT_COLLECTION_IN_DELETE"

    STREAM_IN = "STREAM_IN"
    STREAM_OUT = "STREAM_OUT"


class ParamDictKeys:  # pylint: disable=too-few-public-methods
    """Strings used in Parameter definition as dictionary."""

    # Exposed to the user (see api/parameter.py)
    TYPE = "type"
    DIRECTION = "direction"
    STDIOSTREAM = "stream"
    PREFIX = "prefix"
    DEPTH = "depth"
    WEIGHT = "weight"
    KEEP_RENAME = "keep_rename"
    # Private (see task/parameter.py)
    CONTENT_TYPE = "content_type"
    IS_FILE_COLLECTION = "is_file_collection"
    CACHE = "cache"


PARAM_ALIAS_KEYS = ParamAliasKeys()
PARAM_DICT_KEYS = ParamDictKeys()
