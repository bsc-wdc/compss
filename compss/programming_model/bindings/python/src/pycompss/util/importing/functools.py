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

"""
PyCOMPSs Functools import and backport
======================================
    This file defines the functools importing and backport management.
"""

try:
    # Python 3 lru_cache from functools (this will fail in Python 2)
    from functools import lru_cache
except ImportError:
    try:
        # Try to import lru_cache backport
        from functools32 import lru_cache  # noqa
    except ImportError:
        # So, this is Python 2 without lru_cache.
        # We need to define lru_cache decorator.
        def lru_cache(*args, **kwargs):  # noqa
            # Do nothing wrapper ==> lru_cache disabled.
            return lambda f: f
