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
PyCOMPSs Binding - Commons
==========================
    This file contains the common definitions of the Python binding.
"""

import sys
import os
from tempfile import mkdtemp

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
    from IPython import get_ipython  # noqa

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

# Determine if running in a supercomputer
RUNNING_IN_SUPERCOMPUTER = False
if 'BSC_MACHINE' in os.environ and os.environ['BSC_MACHINE'] == 'mn4':
    # Only supported in MN4 currently
    RUNNING_IN_SUPERCOMPUTER = True

# Tracing hook environment variable
TRACING_HOOK_ENV_VAR = 'COMPSS_TRACING_HOOK'

# Set temporary dir
TEMP_DIR = '.'
TEMP_DIR_PREFIX = 'pycompss'
TEMP_DIR_FOLDER = 'tmpFiles/'
TEMP_OBJ_PREFIX = '/compss-serialized-obj_'

EXTRA_CONTENT_TYPE_FORMAT = "{}:{}"  # <module_path>:<class_name>

# Enable or disable small objects conversion to strings
# cross-module variable (set/modified from launch.py)
OBJECT_CONVERSION = False


##########################################################
# GETTERS AND SETTERS (see launch.py and interactive.py) #
##########################################################

def get_temporary_directory():
    """
    Temporary directory getter.

    :return: Temporary directory path
    """
    return TEMP_DIR


def set_temporary_directory(folder):
    """
    Creates the temporary directory from the folder parameter and
    sets the temporary directory variable.

    :param folder: Temporary directory path
    :return: None
    """
    global TEMP_DIR
    temp_dir = mkdtemp(prefix=TEMP_DIR_PREFIX,
                       dir=os.path.join(folder,
                                        TEMP_DIR_FOLDER))
    TEMP_DIR = temp_dir


def get_object_conversion():
    """
    Object conversion getter.

    :return: Boolean object conversion
    """
    return OBJECT_CONVERSION


def set_object_conversion(conversion=False):
    """
    Set object conversion to string.

    :param conversion: Boolean. True enable, False disable.
    :return: None
    """
    global OBJECT_CONVERSION
    OBJECT_CONVERSION = conversion
