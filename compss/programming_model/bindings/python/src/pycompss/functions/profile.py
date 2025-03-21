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
PyCOMPSs Functions: Profiling decorator.

This file defines the profiling decorator to be used below the task decorator.
"""

import os
import sys
import time
from functools import wraps
from io import StringIO

import psutil
from memory_profiler import profile as mem_profile
from pycompss.util.typing_helper import typing
from pycompss.util.context import CONTEXT

if __debug__:
    import logging

    LOGGER = logging.getLogger(__name__)

# Global environment variable name
__PROFILE_REDIRECT_ENV_VAR__ = "COMPSS_PROFILING_FILE"

FD_PATH = "/proc/self/fd/"
if sys.platform == "darwin":
    FD_PATH = "/dev/fd/"


class Profile:
    """Profile decorator class."""

    __slots__ = ("stream", "full_report", "precision", "backend", "redirect")

    def __init__(
        self,
        full_report: bool = True,
        precision: int = 1,
        backend: str = "psutil",
    ) -> None:
        """Store arguments passed to the decorator.

        :param full_report: If provide the full memory usage report.
        :param precision: Memory usage precision.
        :param backend: Memory measure backend.
        """
        self.stream = StringIO()
        self.full_report = full_report
        self.precision = precision
        self.backend = backend
        if __PROFILE_REDIRECT_ENV_VAR__ in os.environ:
            self.redirect = os.environ[__PROFILE_REDIRECT_ENV_VAR__]
        else:
            self.redirect = ""

    def __call__(self, function: typing.Callable) -> typing.Any:
        """Memory profiler decorator.

        :param function: Function to be profiled (can be a decorated function,
                         usually with @task decorator).
        :return: the decorator wrapper.
        """

        @wraps(function)
        def wrapped_f(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
            """Memory profiler decorator.

            :param args: Task arguments.
            :param kwargs: Task keyword arguments.
            :return: The result of executing function with the given *args and
                     **kwargs.
            """
            # Get job name from log file
            if CONTEXT.in_master():
                job_name = "master"
            else:
                stdout_fd = sys.stdout.fileno()
                job_log_file_path = os.readlink(f"{FD_PATH}{stdout_fd}")
                job_log_file_name = os.path.basename(job_log_file_path)
                job_name = job_log_file_name.split(".")[0]
            # Get initial memory usage
            initial_memory = psutil.virtual_memory().used
            # Get start time stamp
            start_time = time.time()
            # Run the user code
            result = mem_profile(
                func=function,
                stream=self.stream,
                precision=self.precision,
                backend=self.backend,
            )(*args, **kwargs)
            # Get elapsed time
            elapsed_time = time.time() - start_time
            # Get final memory usage
            final_memory = psutil.virtual_memory().used
            # Report before returning the result.
            report = self.stream.getvalue().strip()
            # Reset the stream StringIO
            self.stream.truncate(0)
            self.stream.seek(0)
            self.__print_report__(
                start_time,
                job_name,
                function.__name__,
                report,
                initial_memory,
                final_memory,
                elapsed_time,
            )
            return result

        return wrapped_f

    def __print_report__(  # pylint: disable=R0913,R0914
        # disable=too-many-arguments, too-many-locals
        self,
        start_time: float,
        job_name: str,
        function_name: str,
        report: str,
        initial_memory: int,
        final_memory: int,
        elapsed_time: float,
    ) -> None:
        """Show the memory usage report.

        :param start_time: Task start time.
        :param job_name: Job name.
        :param function_name: Function name.
        :param report: Memory usage report.
        :param initial_memory: Memory used before the task is executed.
        :param final_memory: Memory used after the task is executed.
        :param elapsed_time: Task elapsed time.
        :return: None
        """
        if self.full_report:
            job_name = f"Job name: {job_name}"
            st_time = f"Task start time: {start_time}"
            el_time = f"Elapsed time: {elapsed_time}"
            pre_mem = f"Initial memory: {initial_memory}"
            post_mem = f"Final memory: {final_memory}"
            report_info = (
                f"{report}\n"
                f"{job_name}\n"
                f"{st_time}\n"
                f"{el_time}\n"
                f"{pre_mem}\n"
                f"{post_mem}"
            )
            self.__redirect__(report_info)
        else:
            report_lines = report.splitlines()
            peak_memory = 0.0
            file_name = report_lines[0].split()[1]
            for line in report_lines[4:]:
                # 4: removes the header lines
                info = line.split()
                has_mem = True
                try:
                    usage = float(info[1].replace(",", ""))
                except ValueError:
                    has_mem = False
                    usage = 0.0
                if len(info) > 5 and has_mem:
                    # Is a full line with memory usage
                    if usage > peak_memory:
                        peak_memory = usage
            mem_usage = f"{initial_memory} {final_memory} {peak_memory} MiB"
            info_line = (
                f"{start_time:.7f} {job_name} {file_name} {function_name}"
            )
            info_line = f"{info_line} {elapsed_time} {mem_usage}"
            self.__redirect__(info_line)
        # Clear the stream for the next usage
        self.stream.truncate(0)

    def __redirect__(self, info: str) -> None:
        """Redirect the given info to the required output file or std.

        :param info: Information to redirect.
        :return: None
        """
        if self.redirect == "":
            print(info)
        else:
            with open(self.redirect, "a", encoding="utf-8") as file_descriptor:
                file_descriptor.write(f"{info}\n")
                file_descriptor.flush()

    def __del__(self) -> None:
        """Destructor.

        Closes the internal stream where the profiling is being redirected.
        """
        self.stream.close()


# ########################################################################### #
# ################### Profile DECORATOR ALTERNATIVE NAME #################### #
# ########################################################################### #

profile = Profile  # pylint: disable=invalid-name
PROFILE = Profile
