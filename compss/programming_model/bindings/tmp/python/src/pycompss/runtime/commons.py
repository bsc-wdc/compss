#!/usr/bin/python
#
#  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import sys

# Empty string substitution key
EMPTY_STRING_KEY = "3mPtY57r1Ng"


# Coding/decoding escape
# Global python 3 variable
if sys.version_info >= (3, 0):
    STR_ESCAPE = 'unicode_escape'
    IS_PYTHON3 = True
else:
    STR_ESCAPE = 'string_escape'
    IS_PYTHON3 = False

# Determine the environment
ENVIRONMENT = 'terminal'
IS_INTERACTIVE = False
try:
    from IPython import get_ipython
    ipy_str = str(type(get_ipython()))
    if 'zmqshell' in ipy_str:
        ENVIRONMENT = 'jupyter'
        IS_INTERACTIVE = True
    if 'terminal' in ipy_str:
        ENVIRONMENT = 'ipython'
        IS_INTERACTIVE = True
except ImportError:
    ENVIRONMENT = 'terminal'
    IS_INTERACTIVE = False
