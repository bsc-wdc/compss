#!/usr/bin/python
#
#  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
from pycompss.util.typing_helper import typing
from tempfile import mkdtemp

########################################
# Global variables set in this modules #
########################################

# Empty string substitution key
EMPTY_STRING_KEY = "3mPtY57r1Ng"

# Coding/decoding escape
# Global python 3 variable
if sys.version_info >= (3, 0):
    STR_ESCAPE = "unicode_escape"
    IS_PYTHON3 = True
    LIST_TYPE = list
    DICT_TYPE = dict
else:
    import types
    STR_ESCAPE = "string_escape"
    IS_PYTHON3 = False
    LIST_TYPE = types.ListType  # noqa
    DICT_TYPE = types.DictType  # noqa

# Determine the environment
ENVIRONMENT = "terminal"
IS_INTERACTIVE = False
try:
    from IPython import get_ipython  # noqa
    ipy_str = str(type(get_ipython()))
    if "zmqshell" in ipy_str:
        ENVIRONMENT = "jupyter"
        IS_INTERACTIVE = True
    if "terminal" in ipy_str:
        ENVIRONMENT = "ipython"
        IS_INTERACTIVE = True
except ImportError:
    ENVIRONMENT = "terminal"
    IS_INTERACTIVE = False

# Determine if running in a supercomputer
RUNNING_IN_SUPERCOMPUTER = False
if "BSC_MACHINE" in os.environ and os.environ["BSC_MACHINE"] == "mn4":
    # Only supported in MN4 currently
    RUNNING_IN_SUPERCOMPUTER = True

# Tracing hook environment variable
TRACING_HOOK_ENV_VAR = "COMPSS_TRACING_HOOK"

# Extra content type format
EXTRA_CONTENT_TYPE_FORMAT = "{}:{}"  # <module_path>:<class_name>

# Interactive mode file name
INTERACTIVE_FILE_NAME = "InteractiveMode"

# LONG DEFAULTS
DEFAULT_SCHED = "es.bsc.compss.scheduler.loadbalancing.LoadBalancingScheduler"
DEFAULT_CONN = "es.bsc.compss.connectors.DefaultSSHConnector"
DEFAULT_JVM_WORKERS = "-Xms1024m,-Xmx1024m,-Xmn400m"

#####################################################
# Builtin functions depending on the python version #
#####################################################

###############################################
# Global variables set from different modules #
###############################################

# Set temporary dir
_TEMP_DIR = ""
_TEMP_DIR_PREFIX = "pycompss"
_TEMP_DIR_FOLDER = "tmpFiles/"
_TEMP_OBJ_PREFIX = "/compss-serialized-obj_"

# Enable or disable small objects conversion to strings
# cross-module variable (set/modified from launch.py)
_OBJECT_CONVERSION = False
TRACING_TASK_NAME_TO_ID = dict()  # type: typing.Dict[str, int]

##########################################################
# GETTERS AND SETTERS (see launch.py and interactive.py) #
##########################################################


def get_temporary_directory():
    # type: () -> str
    """ Temporary directory getter.

    :return: Temporary directory path
    """
    return _TEMP_DIR


def set_temporary_directory(folder, create_tmpdir=True):
    # type: (str, bool) -> None
    """ Set the temporary directory.

    Creates the temporary directory from the folder parameter and
    sets the temporary directory variable.

    :param folder: Temporary directory path
    :param create_tmpdir: Create temporary directory within folder.
    :return: None
    """
    global _TEMP_DIR
    if create_tmpdir:
        temp_dir = mkdtemp(prefix=_TEMP_DIR_PREFIX,
                           dir=os.path.join(folder,
                                            _TEMP_DIR_FOLDER))
    else:
        temp_dir = mkdtemp(prefix=_TEMP_DIR_PREFIX,
                           dir=folder)
    _TEMP_DIR = temp_dir


def get_object_conversion():
    # type: () -> bool
    """ Object conversion getter.

    :return: Boolean object conversion
    """
    return _OBJECT_CONVERSION


def set_object_conversion(conversion=False):
    # type: (bool) -> None
    """ Set object conversion to string.

    :param conversion: Boolean. True enable, False disable.
    :return: None
    """
    global _OBJECT_CONVERSION
    _OBJECT_CONVERSION = conversion
