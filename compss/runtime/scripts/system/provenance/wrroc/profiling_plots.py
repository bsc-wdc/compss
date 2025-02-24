#!/usr/bin/env python3
#
#  Copyright 2002-2024 Barcelona Supercomputing Center (www.bsc.es)
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
import typing
import time
import os
import json
import sys

from pathlib import Path
from hashlib import sha256
from mmap import mmap, ACCESS_READ

from rocrate.rocrate import ROCrate
from rocrate.model.contextentity import ContextEntity

from provenance.processing.entities import get_manually_defined_software_requirements
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import pandas as pd


def timestamp_axis(num_entries, time_list):
    """
    Function to build the x-axis by including timestamps while preventing any overlapping

    :param num_entries: number of entries in the dataset
    :param time_list: list containing all the timestamps
    :return:
    """
    step = int(num_entries / 60) + 1
    labels = [f"{time_list[i]}" for i in range(0, len(time_list), step)]
    time_list = [i for i in range(0, len(time_list), step)]
    plt.xticks(time_list, labels=labels, rotation=80)
    plt.subplots_adjust(top=0.95, bottom=0.25)

def build_plot(title, time_list, value_list, name_dataset, measure, num_entries):
    """
    Function to build the plot of the profiling data (percentage such as CPU and memory usage)

    :param title: title of the plot
    :param time_list: list containing the timestamps
    :param value_list: list containing the data
    :param name_dataset: name of the data to assign to the label
    :param measure: unit of measure
    :param num_entries: length of the value_list
    :return:
    """
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(12, 8))
    avg_perc = round(sum(value_list) / num_entries, 2)

    if max(value_list) > 10**6:
        value_list = [value / 10**6 for value in value_list]
        measure = "Megabyte (MB)"

    ax.plot(time_list, value_list, marker=".", linestyle="-", label=name_dataset)

    timestamp_axis(num_entries, time_list)

    if avg_perc < 100:
        plt.axhline(avg_perc, color="b", linestyle="--", label=f"Average = {avg_perc}%")

    # step = int(num_entries / 60) + 1
    # labels = [f"{time_list[i]}" for i in range(0, len(time_list), step)]
    # time_list = [i for i in range(0, len(time_list), step)]

    # plt.xticks(time_list, labels=labels, rotation=80)
    # plt.subplots_adjust(top=0.95, bottom=0.25)

    ax.set_title(title, fontsize=20)
    ax.set_xlabel("Timestamp")
    ax.set_ylabel(measure)
    ax.grid(True)
    ax.legend()


def plot_bytes(
    time_list, first_df, first_df_name, second_df, second_df_name, num_entries, title
):
    """
    Function to generate the plots for metrics which use bytes

    :param time_list: list containing the timestamps
    :param first_df: list of the first dataset
    :param first_df_name: name of the first dataset
    :param second_df: list of the second dataset
    :param second_df_name: name of the second dataset
    :param num_entries: length of the time_list
    :param title: title of the plot
    :return:
    """
    plt.style.use("ggplot")
    fig, ax = plt.subplots(figsize=(12, 8))

    first_df = [value / 10**6 for value in first_df]
    second_df = [value / 10**6 for value in second_df]

    for i in range(num_entries):
        first_value = 0 if i == 0 else first_df[i - 1]
        second_value = 0 if i == 0 else second_df[i - 1]

        first_df[i] = first_value + first_df[i]
        second_df[i] = second_value + second_df[i]

    ax.plot(time_list, first_df, c="r", marker=".", linestyle="-", label=first_df_name)
    ax.plot(
        time_list, second_df, c="b", marker=".", linestyle="-", label=second_df_name
    )

    timestamp_axis(num_entries, time_list)

    ax.set_title(title, fontsize=20)
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Megabyte (MB)")
    ax.grid(True)
    ax.legend()


def build_plot_nodes(time_list, df_list, num_entries, colors, metric_name):
    """
    Function to generate the plots of CPU and memory usage of all nodes

    :param time_list: list containing the timestamps
    :param df_list: list containing the list of data of all nodes
    :param num_entries: length of timestamp_list
    :param colors: list of colors to use
    :param metric_name: name of the metric
    :return:
    """
    plt.figure(figsize=(12, 8))

    for (label, values), i in zip(df_list.items(), range(len(df_list))):
        index = i % len(colors)
        plt.plot(
            values[:num_entries],
            label=label,
            color=colors[index],
            marker=".",
            linestyle="-",
        )

    timestamp_axis(num_entries, time_list)

    plt.legend()
    plt.xlabel("Timestamp")
    plt.ylabel("Percentage")
    plt.title(f"Percentage of {metric_name} for each node")


def plot_results(folder_pathname) -> str:
    """
    Function to store the plots generated

    :param folder_pathname: pathname of the directory containing the data
    :return plots_pathname: pathname of the directory containing the plots generated
    """
    folder_pathname = str(folder_pathname)
    # with open(folder_pathname + '/stats.json', 'r') as f:
    #     stats_file = json.load(f)

    # try:
    #     master_node = stats_file['COMPSS_MASTER_NODE'].strip().upper()
    # except:
    #     master_node = None

    if not os.path.exists(folder_pathname):
        exit(1)

    plots_pathname = folder_pathname + "/plots/"

    list_of_cpus = {}
    list_of_mems = {}

    len_lists = 0
    final_timestamp = []

    # iterate on every file in the directory
    for csv_resources in os.listdir(folder_pathname):
        master_node = not "static_" in csv_resources

        csv_resources = os.path.join(folder_pathname, csv_resources)
        if not csv_resources.endswith(".csv") or os.path.isdir(csv_resources):
            continue

        machine_name = csv_resources.split(".csv")[0].split("_")[-1]
        # if machine_name.strip().upper() == master_node:
        #     machine_name += '(MASTER)'
        if master_node:
            machine_name += "-MASTER"

        df = pd.read_csv(csv_resources)
        df_length = len(df)

        cpu_usage = df["CPU"]
        mem_usage = df["MEM"]
        byte_sent = df["BYTE_SENT"]
        byte_recv = df["BYTE_RECV"]
        byte_read_disk = df["BYTE_READ_DISK"]
        byte_write_disk = df["BYTE_WRITE_DISK"]
        time_read_disk = df["TIME_READ_DISK"]
        time_write_disk = df["TIME_WRITE_DISK"]
        timestamps = df["TIME"]

        if df_length > len_lists:
            len_lists = df_length
            final_timestamp = timestamps

        list_of_cpus[machine_name] = list(cpu_usage)
        list_of_mems[machine_name] = list(mem_usage)

        output_path = plots_pathname + machine_name
        os.makedirs(output_path, exist_ok=True)

        build_plot(
            "CPU usage",
            timestamps,
            cpu_usage,
            name_dataset="CPU",
            measure="CPU %",
            num_entries=df_length,
        )
        plt.savefig(output_path + "/cpu.png")
        plt.close()

        build_plot(
            "Memory usage",
            timestamps,
            mem_usage,
            name_dataset="MEM",
            measure="Memory %",
            num_entries=df_length,
        )
        plt.savefig(output_path + "/mem.png")
        plt.close()

        plot_bytes(
            time_list=timestamps,
            first_df=byte_sent,
            first_df_name="Bytes sent",
            second_df=byte_recv,
            second_df_name="Bytes received",
            num_entries=df_length,
            title="Network usage",
        )
        plt.savefig(output_path + "/network_usage.png")
        plt.close()

        build_plot(
            "Data transferred: bytes sent",
            timestamps,
            byte_sent,
            name_dataset="BYTE_SENT",
            measure="Byte (B)",
            num_entries=df_length,
        )
        plt.savefig(output_path + "/bytes_sent.png")
        plt.close()

        build_plot(
            "Data transferred: bytes received",
            timestamps,
            byte_recv,
            name_dataset="BYTE_RECV",
            measure="Byte (B)",
            num_entries=df_length,
        )
        plt.savefig(output_path + "/bytes_received.png")
        plt.close()

        plot_bytes(
            time_list=timestamps,
            first_df=byte_write_disk,
            first_df_name="Bytes written",
            second_df=byte_read_disk,
            second_df_name="Bytes read",
            num_entries=df_length,
            title="Disk usage",
        )
        plt.savefig(output_path + "/disk_usage.png")
        plt.close()

        build_plot(
            "Disk usage: bytes written",
            timestamps,
            byte_write_disk,
            name_dataset="BYTE_WRITE_DISK",
            measure="Byte (B)",
            num_entries=df_length,
        )
        plt.savefig(output_path + "/bytes_written.png")
        plt.close()

        build_plot(
            "Disk usage: bytes read",
            timestamps,
            byte_read_disk,
            name_dataset="BYTE_READ_DISK",
            measure="Byte (B)",
            num_entries=df_length,
        )
        plt.savefig(output_path + "/bytes_read.png")
        plt.close()

    colors = list(mcolors.TABLEAU_COLORS.values())
    plt.style.use("ggplot")

    build_plot_nodes(
        final_timestamp,
        df_list=list_of_cpus,
        num_entries=len_lists,
        colors=colors,
        metric_name="CPU",
    )
    plt.savefig(plots_pathname + "cpu_nodes.png")
    plt.close()

    build_plot_nodes(
        final_timestamp,
        df_list=list_of_mems,
        num_entries=len_lists,
        colors=colors,
        metric_name="memory",
    )
    plt.savefig(plots_pathname + "mem_nodes.png")
    plt.close()

    return plots_pathname


def generate_plots(stats_path) -> str:
    """
    Function to generate all the plots

    :param stats_path: pathname of the folder containing the data
    :return plots_folder: pathname containing the plots
    """
    plots_folder = None
    try:
        plots_folder = plot_results(stats_path)
        print("PROVENANCE | Generation of the profiling plots.")
    except:
        print("PROVENANCE | ERROR Could not generate the profiling plots.")

    return plots_folder
