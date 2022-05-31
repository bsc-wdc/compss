#!/usr/bin/python
#
#  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
PyCOMPSs Util - Interactive - Graphs.

Provides auxiliary methods for the interactive mode with regard to graphs
"""

import os
import time

from pycompss.util.typing_helper import typing


def show_graph(
    log_path: str,
    name: str = "complete_graph",
    fit: bool = False,
    refresh_rate: int = 1,
    timeout: int = 0,
) -> None:
    """Show graph.

    :param log_path: Folder where the logs are.
    :param name: Graph to show (default: "complete_graph").
    :param fit: Fit to width [ True | False ] (default: False).
    :param refresh_rate: Update the current task graph every "refresh_rate"
                         seconds. Default 1 second if timeout != 0.
    :param timeout: Time during the current task graph is going to be updated.
    :return: None.
    """
    try:
        from graphviz import Source  # noqa
    except ImportError:
        print("Oops! graphviz is not available.")
        return None
    from IPython.display import clear_output  # noqa
    from IPython.display import display  # noqa

    # Check refresh rate and timeout
    assert timeout >= 0, "ERROR: timeout has to be >= 0"
    if timeout > 0:
        assert (
            refresh_rate < timeout
        ), "ERROR: refresh_rate can not be higher than timeout"
    # Set file name
    file_name = os.path.join(log_path, "monitor", name)
    # Act
    if timeout == 0:
        display(__get_graph_snapshot__(file_name, fit, Source))
    else:
        try:
            while timeout >= 0:
                clear_output(wait=True)
                display(__get_graph_snapshot__(file_name, fit, Source))
                time.sleep(refresh_rate)
                timeout = timeout - refresh_rate
        except KeyboardInterrupt:
            # User hit stop on the cell
            clear_output(wait=True)
            display(__get_graph_snapshot__(file_name, fit, Source))
    return None


def __get_graph_snapshot__(file_name: str, fit: bool, source: typing.Any) -> typing.Any:
    """Read the graph file and returns it as graphviz object.

    It is able to fit the size if indicated.

    :param file_name: Absolute path to the graph to get the snapshot.
    :param fit: Fit to size or not.
    :param source: Graphviz Source object
    :return: The graph snapshot to be rendered.
    """
    # Read graph file
    with open(file_name + ".dot", "r") as monitor_file:
        text = monitor_file.read()
    if fit:
        try:
            # Convert to png and show full picture
            extension = "jpeg"
            file = f"{file_name}.{extension}"
            if os.path.exists(file):
                os.remove(file)
            graph_source = source(text, filename=file_name, format=extension)
            graph_source.render()
            from IPython.display import Image  # noqa

            image = Image(filename=file)
            return image
        except Exception as general_exception:
            print("Oops! Failed rendering the graph.")
            raise general_exception
    else:
        return source(text)
