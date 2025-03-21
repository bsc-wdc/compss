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
PyCOMPSs Util - Interactive - State.

Provides auxiliary methods for the interactive mode to get the state.
"""

import os
from collections import defaultdict
from xml.etree import ElementTree

from pycompss.util.exceptions import PyCOMPSsException
from pycompss.util.typing_helper import typing

try:
    from IPython.display import HTML  # noqa
    from IPython.display import display  # noqa

    MISSING_DEPENDENCY = "tabulate"  # noqa
    import tabulate  # noqa

    MISSING_DEPENDENCY = "matplotlib"  # noqa
    import matplotlib.pyplot as plt  # noqa

    MISSING_DEPENDENCY = "numpy"  # noqa
    import numpy as np  # noqa

    np.random.seed(0)
    MISSING_DEPENDENCY = "None"  # noqa
except ImportError:
    HTML = None  # type: ignore
    display = None  # type: ignore
    plt = None  # type: ignore

try:
    import ipywidgets as widgets  # noqa
except ImportError:
    widgets = None  # type: ignore


def supports_dynamic_state() -> bool:
    """Check if the state can be displayed with widgets.

    :return: True if widgets available. False otherwise.
    """
    return widgets is not None


def check_monitoring_file(log_path: str) -> bool:
    """Check if there monitoring file exists or not.

    :param log_path: Absolute path of the log folder.
    :return: True if the compss monitoring file exists. False otherwise.
    """
    compss_state_xml = get_compss_state_xml(log_path)
    return os.path.exists(compss_state_xml)


def get_compss_state_xml(log_path: str) -> str:
    """Check if there is any missing pkg and return the status xml full path.

    :param log_path: Absolute path of the log folder.
    :return: The compss state full path.
    """
    if MISSING_DEPENDENCY != "None":
        raise PyCOMPSsException(f"Missing {MISSING_DEPENDENCY} package.")
    compss_state_xml = os.path.join(log_path, "monitor", "COMPSs_state.xml")
    return compss_state_xml


def parse_state_xml(log_path: str, field: str) -> typing.Any:
    """Convert the given xml to dictionary.

    :param log_path: Absolute path of the log folder.
    :param field: Field name to retrieve.
    :return: The content as dictionary (unless CoresInfo, which is a list).
    """
    state_xml = get_compss_state_xml(log_path)
    tree = ElementTree.parse(state_xml)
    root = tree.getroot()
    state_xml_dict = element_tree_to_dict(root)
    if state_xml_dict["COMPSsState"][field] == "":
        print("Please, submit any task in order to see the information")
        return None
    if field == "TasksInfo":
        return state_xml_dict["COMPSsState"][field]["Application"]
    if field == "CoresInfo":
        # This is a list if there is more than one core info
        # Otherwise it is a dictionary with the only core
        cores_info = state_xml_dict["COMPSsState"][field]["Core"]
        if isinstance(cores_info, dict):
            return [cores_info]
        return cores_info
    if field == "Statistics":
        return state_xml_dict["COMPSsState"][field]["Statistic"]
    if field == "ResourceInfo":
        return state_xml_dict["COMPSsState"][field]["Resource"]
    raise PyCOMPSsException("Unsupported status field")


def element_tree_to_dict(element_tree: ElementTree.Element) -> dict:
    """Convert an element tree into a dictionary recursively.

    :param element_tree: Element tree.
    :return: Dictionary.
    """
    build_dict = {
        element_tree.tag: {} if element_tree.attrib else None
    }  # type: dict
    children = list(element_tree)
    if children:
        def_dict = defaultdict(list)
        for child_dict in map(element_tree_to_dict, children):
            for key, value in child_dict.items():
                def_dict[key].append(value)
        build_dict = {
            element_tree.tag: {
                key: value[0] if len(value) == 1 else value
                for key, value in def_dict.items()
            }
        }
    if element_tree.attrib:
        build_dict[element_tree.tag].update(
            ("@" + key, value) for key, value in element_tree.attrib.items()
        )
    if element_tree.text:
        text = element_tree.text.strip()
        if children or element_tree.attrib:
            if text:
                build_dict[element_tree.tag]["#text"] = text
        else:
            build_dict[element_tree.tag] = text
    return build_dict


def show_tasks_info(log_path: str) -> None:
    """Show tasks info.

    :param log_path: Absolute path of the log folder.
    :return: None.
    """
    if supports_dynamic_state():

        def play_widget(
            i: typing.Any,
        ) -> None:  # pylint: disable=unused-argument
            __show_tasks_info(log_path)

        play = __get_play_widget(play_widget)
        display(play)  # noqa
    else:
        __show_tasks_info(log_path)


def __show_tasks_info(log_path: str) -> None:
    """Show tasks info.

    :param log_path: Absolute path of the log folder.
    :return: None.
    """
    cores_info = parse_state_xml(log_path, "CoresInfo")
    if cores_info is None:
        # Do not show anything if there is no information to display
        return
    labels = [
        "Signature",
        "ExecutedCount",
        "MaxExecutionTime",
        "MeanExecutionTime",
        "MinExecutionTime",
    ]
    cores = []
    for core in cores_info:
        new_task = []
        for label in labels:
            if label == "Signature":
                new_task.append(core["Impl"][label].split(".")[-1])
            elif label == "ExecutedCount":
                new_task.append(int(core["Impl"][label]))
            else:
                new_task.append(int(core["Impl"][label]) / 1000)
        cores.append(new_task)
    # Display graph
    task_names = [row[0] for row in cores]
    task_names[0] = "TaskName"
    task_count = [row[1] for row in cores]
    task_max = [row[2] for row in cores]
    task_mean = [row[3] for row in cores]
    task_min = [row[4] for row in cores]
    fig = plt.figure()
    ax1 = fig.add_subplot(1, 2, 1)
    ax2 = fig.add_subplot(1, 2, 2)
    errs = [[], []]  # type: typing.List[typing.List[str]]
    for i in range(len(task_names)):
        min_ = task_mean[i] - task_min[i]
        max_ = task_max[i] - task_mean[i]
        errs[0].append(min_)
        errs[1].append(max_)
    ax1.errorbar(task_names, task_mean, yerr=errs, lw=3, fmt="ok")
    ax1.set_ylabel("Time (seconds)")
    ax1.set_xlabel("Task name")
    task_mean = [mean * 1000 for mean in task_mean]
    ax2.scatter(
        task_names,
        task_count,
        s=task_mean,
        c=np.random.rand(len(task_names)),
        alpha=0.5,
    )
    ax2.set_ylim(0, max(task_count))
    ax2.ticklabel_format(axis="y", style="plain")
    ax2.set_ylabel("Amount of tasks")
    ax2.set_xlabel("Task name")
    plt.tight_layout()
    plt.show()
    # Display table with values
    display(
        HTML(tabulate.tabulate(cores, tablefmt="html", headers=labels))
    )  # noqa


def show_tasks_status(log_path: str) -> None:
    """Show tasks status.

    :param log_path: Absolute path of the log folder.
    :return: None.
    """
    if supports_dynamic_state():

        def play_widget(i):  # pylint: disable=unused-argument
            # type: (typing.Any) -> None
            __show_tasks_status(log_path)

        play = __get_play_widget(play_widget)
        display(play)  # noqa
    else:
        __show_tasks_info(log_path)


def __show_tasks_status(log_path: str) -> None:
    """Show tasks status.

    :param log_path: Absolute path of the log folder.
    :return: None.
    """
    tasks_info_dict = parse_state_xml(log_path, "TasksInfo")
    if tasks_info_dict is None:
        # Do not show anything if there is no information to display
        return
    # Display graph
    labels = ["InProgress", "Completed"]
    sizes = [tasks_info_dict[labels[0]], tasks_info_dict[labels[1]]]
    explode = (0, 0)
    _, ax1 = plt.subplots()  # first return (ignored) is fig1
    colors = ["b", "g"]
    ax1.pie(
        sizes,
        explode=explode,
        labels=labels,
        colors=colors,
        shadow=True,
        startangle=90,
    )
    ax1.axis("equal")
    plt.show()
    # Display table with values
    labels, values = __plain_lists(tasks_info_dict)
    display(
        HTML(tabulate.tabulate([values], tablefmt="html", headers=labels))
    )  # noqa


def show_statistics(log_path: str) -> None:
    """Show statistics info.

    :param log_path: Absolute path of the log folder.
    :return: None.
    """
    if supports_dynamic_state():

        def play_widget(
            i: typing.Any,
        ) -> None:  # pylint: disable=unused-argument
            __show_statistics(log_path)

        play = __get_play_widget(play_widget)
        display(play)  # noqa
    else:
        __show_statistics(log_path)


def __show_statistics(log_path: str) -> None:
    """Show statistics info.

    :param log_path: Absolute path of the log folder.
    :return: None.
    """
    statistics_dict = parse_state_xml(log_path, "Statistics")
    if statistics_dict is None:
        # Do not show anything if there is no information to display
        return
    # Display table with values
    labels = [statistics_dict["Key"]]
    values = [statistics_dict["Value"]]
    display(
        HTML(tabulate.tabulate([values], tablefmt="html", headers=labels))
    )  # noqa


def show_resources_status(log_path: str) -> None:
    """Show resources status info.

    :param log_path: Absolute path of the log folder.
    :return: None.
    """
    if supports_dynamic_state():

        def play_widget(
            i: typing.Any,
        ) -> None:  # pylint: disable=unused-argument
            __show_resources_status(log_path)

        play = __get_play_widget(play_widget)
        display(play)  # noqa
    else:
        __show_resources_status(log_path)


def __show_resources_status(log_path: str) -> None:
    """Show resources status info.

    :param log_path: Absolute path of the log folder.
    :return: None.
    """
    resource_info_dict = parse_state_xml(log_path, "ResourceInfo")
    if resource_info_dict is None:
        # Do not show anything if there is no information to display
        return
    # Display table with values
    labels, values = __plain_lists(resource_info_dict)
    display(
        HTML(tabulate.tabulate([values], tablefmt="html", headers=labels))
    )  # noqa


def __plain_lists(dictionary: dict) -> typing.Tuple[list, list]:
    """Convert a dictionary to two lists.

    IMPORTANT! Removes last element.

    :param dictionary: Dictionary to plain.
    :return: Labels and values.
    """
    labels = []
    values = []
    for key, value in dictionary.items():
        labels.append(key)
        values.append(value)
    labels.pop()
    values.pop()
    return labels, values


def __get_play_widget(
    function: typing.Callable, interval: int = 5000
) -> widgets.interactive:
    """Generate play widget.

    :param function: Function to associate with Play.
    :param interval: Refresh interval.
    :return: Play widget.
    """
    play = widgets.interactive(
        function,
        i=widgets.Play(
            value=0,
            min=0,
            max=500,
            step=1,
            interval=interval,
            description="Press play",
            disabled=False,
        ),
    )
    return play
