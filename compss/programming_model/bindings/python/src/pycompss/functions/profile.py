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
PyCOMPSs Functions: Profiling decorator.

This file defines the profiling decorator to be used below the task decorator.
"""

import os
from functools import wraps
from io import StringIO

from memory_profiler import profile as mem_profile
from pycompss.util.typing_helper import typing

__PROFILE_REDIRECT_ENV_VAR__ = "COMPSS_PROFILING_FILE"


class Profile:
    """Profile decorator class."""

    __slots__ = ("stream", "full_report", "precision", "backend", "redirect")

    def __init__(
        self, full_report: bool = True, precision: int = 1, backend: str = "psutil"
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
            :return: The result of executing function with the given *args and **kwargs.
            """
            result = mem_profile(
                func=function,
                stream=self.stream,
                precision=self.precision,
                backend=self.backend,
            )(*args, **kwargs)
            # Report before returning the result.
            report = self.stream.getvalue()
            self.__print_report__(function.__name__, report)
            return result

        return wrapped_f

    def __print_report__(self, function_name: str, report: str) -> None:
        """Show the memory usage report.

        :param function_name: Function name.
        :param report: Memory usage report.
        :return: None
        """
        if self.full_report:
            self.__redirect__(report)
        else:
            report = report.splitlines()
            peak_memory = 0
            file_name = report[0].split()[1]
            for line in report[4:]:
                # 4: removes the header lines
                info = line.split()
                if len(info) > 5:
                    # Is a full line with memory usage
                    usage = float(info[1].replace(",", ""))
                    if usage > peak_memory:
                        peak_memory = usage
            info = f"{file_name} {function_name} {peak_memory} MiB"
            self.__redirect__(info)
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
