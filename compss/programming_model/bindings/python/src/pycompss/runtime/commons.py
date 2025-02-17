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
PyCOMPSs Binding - Commons.

This file contains the common definitions of the Python binding.
"""

import os
from tempfile import mkdtemp

from pycompss.util.typing_helper import typing  # noqa: F401


#######################################
# Global variables set in this module #
#######################################


class Constants:  # pylint: disable=R0902,R0903
    # disable=too-many-instance-attributes, too-few-public-methods
    """Common constants definitions."""

    __slots__ = (
        "empty_string_key",
        "python_interpreter",
        "str_escape",
        "environment",
        "is_interactive",
        "running_in_supercomputer",
        "tracing_hook_env_var",
        "extra_content_type_format",
        "interactive_file_name",
        "compss_interactive_source_file",
        "default_sched",
        "default_conn",
        "default_jvm_workers",
        "default_checkpoint_policy",
        "temp_dir_prefix",
        "temp_obj_prefix",
    )

    def __init__(self) -> None:
        """Constant constructor.

        :returns: None.
        """
        # Empty string substitution key
        self.empty_string_key = "3mPtY57r1Ng"
        # Default python interpreter
        self.python_interpreter = "python3"
        # Coding/decoding escape
        self.str_escape = "unicode_escape"
        # Determine the environment
        environment = "terminal"
        is_interactive = False
        try:
            from IPython import get_ipython  # noqa # pylint: disable=C0415

            # disable=import-outside-toplevel

            ipy_str = str(type(get_ipython()))
            if "zmqshell" in ipy_str:
                environment = "jupyter"
                is_interactive = True
            if "terminal" in ipy_str:
                environment = "ipython"
                is_interactive = True
        except ImportError:
            environment = "terminal"
            is_interactive = False
        self.environment = environment
        self.is_interactive = is_interactive
        # Determine if running in a supercomputer
        self.running_in_supercomputer = False
        if (
            "COMPSS_RUNNING_IN_SC" in os.environ
            and os.environ["COMPSS_RUNNING_IN_SC"] == "true"
        ):
            self.running_in_supercomputer = True
        elif (
            "BSC_MACHINE" in os.environ and os.environ["BSC_MACHINE"] == "mn4"
        ):
            # Only supported in MN4 currently
            self.running_in_supercomputer = True
        # Tracing hook environment variable
        self.tracing_hook_env_var = "COMPSS_TRACING_HOOK"
        # Extra content type format
        self.extra_content_type_format = "{}:{}"  # <module_path>:<class_name>
        # Interactive mode file name prefix
        self.interactive_file_name = "InteractiveMode"
        # Interactive source file that will be used
        self.compss_interactive_source_file = "compss_interactive_source_file"
        # LONG DEFAULTS
        self.default_sched = (
            "es.bsc.compss.scheduler.lookahead.locality.LocalityTS"
        )
        self.default_conn = "es.bsc.compss.connectors.DefaultSSHConnector"
        self.default_jvm_workers = "-Xms1024m,-Xmx1024m,-Xmn400m"
        self.default_checkpoint_policy = (
            "es.bsc.compss.checkpoint.policies.NoCheckpoint"
        )
        # Temporary directory/objects info
        self.temp_dir_prefix = "pycompss"
        self.temp_obj_prefix = "/compss-serialized-obj_"


# Placeholder for all constant variables
CONSTANTS = Constants()


###############################################
# Global variables set from different modules #
###############################################


class Globals:
    """Common global definitions."""

    __slots__ = (
        "log_dir",
        "temp_dir",
        "analysis_dir",
        "object_conversion",
        "tracing_task_name_to_id",
    )

    def __init__(self) -> None:
        """Global object constructor.

        :returns: None.
        """
        self.log_dir = ""
        self.temp_dir = ""
        self.analysis_dir = ""
        self.object_conversion = False
        self.tracing_task_name_to_id = {}  # type: typing.Dict[str, int]

    def get_log_directory(self) -> str:
        """Log directory getter.

        :return: Log directory path.
        """
        return self.log_dir

    def set_log_directory(self, folder: str) -> None:
        """Set the log directory.

        :param folder: Log directory path.
        :return: None.
        """
        if not os.path.exists(folder):
            os.mkdir(folder)
        self.log_dir = folder

    def get_temporary_directory(self) -> str:
        """Temporary directory getter.

        :return: Temporary directory path.
        """
        return self.temp_dir

    def set_temporary_directory(self, folder: str) -> None:
        """Set the temporary directory.

        Creates the temporary directory from the folder parameter and
        sets the temporary directory variable.

        :param folder: Temporary directory path.
        :return: None.
        """
        temp_dir = mkdtemp(prefix=CONSTANTS.temp_dir_prefix, dir=folder)
        self.temp_dir = temp_dir

    def get_analysis_directory(self) -> str:
        """Analysis directory getter.

        :return: Analysis directory path.
        """
        return self.analysis_dir

    def set_analysis_directory(self, folder: str) -> None:
        """Set the analysis directory.

        Creates the analysis directory from the folder parameter and
        sets the analysis directory variable.

        :param folder: Analysis directory path.
        :return: None.
        """
        if not os.path.exists(folder):
            os.mkdir(folder)
        self.analysis_dir = folder

    def in_tracing_task_name_to_id(self, task_name: str) -> bool:
        """Check if task_name is in tracing_task_name_to_id dictionary.

        :param task_name: Traced task name.
        :return: Boolean if exists.
        """
        return task_name in self.tracing_task_name_to_id

    def get_tracing_task_name_id(self, task_name: str) -> int:
        """Retrieve the identifier of the given task_name.

        :param task_name: Traced task name.
        :return: The task_name identifier.
        """
        return self.tracing_task_name_to_id[task_name]

    def set_tracing_task_name_to_id(self, task_name: str, value: int) -> None:
        """Set value as the identifier for the given task_name.

        :param task_name: Traced task name.
        :param value: Traced task identifier.
        :return: None.
        """
        self.tracing_task_name_to_id[task_name] = value

    def len_tracing_task_name_to_id(self) -> int:
        """Retrieve the amount of identifier registered.

        :return: The number of entries.
        """
        return len(self.tracing_task_name_to_id)


GLOBALS = Globals()
