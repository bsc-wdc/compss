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
PyCOMPSs Util - Interactive - monitor.

Provides auxiliary methods for the interactive mode.
"""

import os

from pycompss.util.typing_helper import typing

from pycompss.util.interactive.state import display
from pycompss.util.interactive.state import supports_dynamic_state
from pycompss.util.interactive.state import __get_play_widget
from pycompss.util.interactive.state import HTML
from pycompss.util.interactive.state import tabulate


def show_monitoring_information(log_path):
    """Show the monitoring information widget.

    :param log_path: Absolute path of the log folder.
    :return: None.
    """
    if supports_dynamic_state():

        def play_widget(
            i: typing.Any,  # pylint: disable=unused-argument
        ) -> None:
            __show_monitoring_info(log_path)

        play = __get_play_widget(play_widget, interval=250)
        display(play)  # noqa
    else:
        __show_monitoring_info(log_path)


def read_monitoring_file(log_path: str) -> typing.List[str]:
    """Read the monitoring file.

    :param log_path: Absolute path of the log folder.
    :return: List of messages in monitoring file.
    """
    # monitoring_file = os.path.join(log_path, "monitoring.txt")
    monitoring_file = os.environ["COMPSS_MONITORING_FILE"]
    with open(monitoring_file, "r") as monitoring_fd:
        contents = [line.rstrip() for line in monitoring_fd]
    return contents


def __show_monitoring_info(log_path: str) -> None:
    """Show tasks status.

    :param log_path: Absolute path of the log folder.
    :return: None.
    """
    monitoring_info = read_monitoring_file(log_path)
    if len(monitoring_info) == 0:
        # Do not show anything if there is no information to display
        return
    # Display table with values
    display(HTML(tabulate.tabulate(monitoring_info, tablefmt="html")))
