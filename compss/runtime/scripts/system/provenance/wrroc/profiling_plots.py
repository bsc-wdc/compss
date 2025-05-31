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
import os
import time

try:
    import matplotlib.pyplot as plt
    import matplotlib.colors as mcolors
    from matplotlib.lines import Line2D
except:
    print(
        "Error: matplotlib is not installed. Please install it using 'pip install matplotlib'."
    )
    exit(1)
try:
    import pandas as pd
except:
    print(
        "Error: pandas is not installed. Please install it using 'pip install pandas'."
    )
    exit(1)

# Color-blind friendly palette (Paul Tol's high contrast)
COLOR_PALETTE = [
    '#4477AA',  # blue
    '#EE6677',  # red
    '#228833',  # green
    '#CCBB44',  # yellow
    '#66CCEE',  # cyan
    '#AA3377',  # purple
    '#BBBBBB',  # grey
    '#000000'  # black
]

# Different marker styles for better distinction
MARKER_STYLES = ['o', 's', '^', 'v', 'D', 'p', '*', 'h', 'H', '+', 'x', '|', '_']

# Line styles for additional distinction
LINE_STYLES = ['-', '--', '-.', ':']


def timestamp_axis(num_entries, time_list):
    """
    Function to build the x-axis by including timestamps while preventing any overlapping

    :param num_entries: number of entries in the dataset
    :param time_list: list containing all the timestamps
    :return:
    """
    step = int(num_entries / 60) + 1
    selected_times = time_list[::step]
    labels = [str(time) for time in selected_times]
    time_indices = range(0, len(time_list), step)

    plt.xticks(time_indices, labels=labels, rotation=80)
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
    plt.rcParams['figure.dpi'] = 300  # Higher resolution
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['lines.linewidth'] = 1.5
    plt.rcParams['lines.markersize'] = 4  # Smaller markers

    fig, ax = plt.subplots(figsize=(12, 8))
    avg_perc = round(sum(value_list) / num_entries, 2)

    if max(value_list) > 10 ** 6:
        value_list = [value / 10 ** 6 for value in value_list]
        measure = "Megabyte (MB)"

    # Use solid line with less frequent markers
    ax.plot(
        time_list,
        value_list,
        marker='.',
        markersize=3,  # Smaller points
        markevery=10,  # Show marker every 10 points
        linestyle="-",
        label=name_dataset,
        color=COLOR_PALETTE[0]
    )

    timestamp_axis(num_entries, time_list)

    if avg_perc < 100:
        plt.axhline(
            avg_perc,
            color=COLOR_PALETTE[1],
            linestyle="--",
            label=f"Average = {avg_perc}%"
        )

    ax.set_title(title)
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
    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['lines.linewidth'] = 1.5
    plt.rcParams['lines.markersize'] = 4

    fig, ax = plt.subplots(figsize=(12, 8))

    first_df = [value / 10 ** 6 for value in first_df]
    second_df = [value / 10 ** 6 for value in second_df]

    for i in range(num_entries):
        first_value = 0 if i == 0 else first_df[i - 1]
        second_value = 0 if i == 0 else second_df[i - 1]

        first_df[i] = first_value + first_df[i]
        second_df[i] = second_value + second_df[i]

    # Different line styles and markers for better distinction
    ax.plot(
        time_list,
        first_df,
        color=COLOR_PALETTE[0],
        marker='s',
        markersize=3,
        markevery=10,
        linestyle="-",
        label=first_df_name
    )
    ax.plot(
        time_list,
        second_df,
        color=COLOR_PALETTE[1],
        marker='^',
        markersize=3,
        markevery=10,
        linestyle="--",
        label=second_df_name
    )

    timestamp_axis(num_entries, time_list)

    ax.set_title(title)
    ax.set_xlabel("Timestamp")
    ax.set_ylabel("Megabyte (MB)")
    ax.grid(True)
    ax.legend()


def build_plot_nodes(resampled_dfs, name_plot, metric, name_metric, colors):
    """
    Function to generate the plots of CPU and memory usage of all nodes

    :param resampled_dfs: list containing the dataframe of all nodes
    :param name_plot: name of the file to save
    :param metric: label used to select the metric in the dataframe
    :param name_metric: name of the metric to show in the plot
    :param colors: list of colors to use in the plots
    :return:
    """
    plt.style.use("ggplot")
    plt.rcParams['figure.dpi'] = 300
    plt.rcParams['savefig.dpi'] = 300
    plt.rcParams['lines.linewidth'] = 1.5
    plt.rcParams['lines.markersize'] = 3

    plt.figure(figsize=(12, 8))

    # Create custom legend handles for better distinction
    legend_elements = []

    for i, (label, resampled_df) in enumerate(resampled_dfs.items()):
        color = COLOR_PALETTE[i % len(COLOR_PALETTE)]
        marker = MARKER_STYLES[i % len(MARKER_STYLES)]
        linestyle = LINE_STYLES[i % len(LINE_STYLES)]

        plt.plot(
            resampled_df.index,
            resampled_df[metric],
            label=label,
            color=color,
            marker=marker,
            markersize=3,
            markevery=15,
            linestyle=linestyle,
            linewidth=1.5
        )

        # Create custom legend entry with all distinguishing features
        legend_elements.append(Line2D(
            [0], [0],
            color=color,
            marker=marker,
            linestyle=linestyle,
            label=label,
            markersize=8,
            linewidth=1.5
        ))

    all_times = pd.concat(resampled_dfs.values()).index.unique().sort_values()
    num_entries = len(all_times)
    step = int(num_entries / 60) + 1
    selected_times = all_times[::step]
    labels = [time.strftime("%Y-%m-%d %H:%M:%S") for time in selected_times]

    plt.xticks(selected_times, labels=labels, rotation=80)
    plt.subplots_adjust(top=0.95, bottom=0.25)

    plt.xlabel("Timestamp")
    plt.ylabel(f"{name_metric} usage (%)")
    plt.title(f"{name_metric} usage among the nodes")

    # Use custom legend
    plt.legend(handles=legend_elements)

    plt.grid(True)
    plt.tight_layout()

    # Save as SVG for vector format
    if name_plot.endswith('.svg'):
        name_plot = name_plot[:-4] + '.svg'
    plt.savefig(name_plot, format='svg')
    plt.close()


def plot_results(folder_pathname) -> str:
    """
    Function to store the plots generated

    :param folder_pathname: pathname of the directory containing the data
    :return plots_pathname: pathname of the directory containing the plots generated
    """
    folder_pathname = str(folder_pathname)
    if not os.path.exists(folder_pathname):
        print("Error: stats folder does not exist")
        exit(1)

    plots_pathname = folder_pathname + "/plots/"
    os.makedirs(plots_pathname, exist_ok=True)

    list_of_cpus = {}
    list_of_mems = {}

    df_list = []
    name_list = []

    num_files = sum(1 for f in os.listdir(folder_pathname) if f.endswith(".csv"))

    # iterate on every file in the directory
    for csv_resources in os.listdir(folder_pathname):
        csv_resources = os.path.join(folder_pathname, csv_resources)
        if not csv_resources.endswith(".csv") or os.path.isdir(csv_resources):
            continue

        machine_name = csv_resources.split(".csv")[0].split("_")[-1]
        name_list.append(machine_name)

        df = pd.read_csv(csv_resources)
        df_list.append(df)
        df_length = len(df)

        cpu_usage = df["CPU"]
        mem_usage = df["MEM"]
        byte_sent = df["BYTE_SENT"]
        byte_recv = df["BYTE_RECV"]
        byte_read_disk = df["BYTE_READ_DISK"]
        byte_write_disk = df["BYTE_WRITE_DISK"]
        timestamps = df["TIME"]

        list_of_cpus[machine_name] = list(cpu_usage)
        list_of_mems[machine_name] = list(mem_usage)

        output_path = plots_pathname + machine_name
        os.makedirs(output_path, exist_ok=True)

        build_plot(
            f"CPU usage of {machine_name}",
            timestamps,
            cpu_usage,
            name_dataset="CPU",
            measure="CPU %",
            num_entries=df_length,
        )
        plt.savefig(output_path + "/cpu.svg", format='svg')
        plt.close()

        build_plot(
            f"Memory usage of {machine_name}",
            timestamps,
            mem_usage,
            name_dataset="MEM",
            measure="Memory %",
            num_entries=df_length,
        )
        plt.savefig(output_path + "/mem.svg", format='svg')
        plt.close()

        if not byte_sent.isna().any().any() and not byte_recv.isna().any().any():
            plot_bytes(
                time_list=timestamps,
                first_df=byte_sent,
                first_df_name="Bytes sent",
                second_df=byte_recv,
                second_df_name="Bytes received",
                num_entries=df_length,
                title=f"Network usage of {machine_name}",
            )
            plt.savefig(output_path + "/network_usage.svg", format='svg')
            plt.close()

        if not byte_write_disk.isna().any().any() and not byte_read_disk.isna().any().any():
            plot_bytes(
                time_list=timestamps,
                first_df=byte_write_disk,
                first_df_name="Bytes written",
                second_df=byte_read_disk,
                second_df_name="Bytes read",
                num_entries=df_length,
                title=f"Disk usage of {machine_name}",
            )
            plt.savefig(output_path + "/disk_usage.svg", format='svg')
            plt.close()

    if num_files > 1:
        plt.style.use("ggplot")
        resampled_dfs = {}
        for df, label in zip(df_list, name_list):
            df["TIME"] = pd.to_datetime(df["TIME"])
            df.set_index("TIME", inplace=True)
            resampled_df = df.resample("s").mean().interpolate(method="linear")
            resampled_dfs[label] = resampled_df

        build_plot_nodes(
            resampled_dfs, plots_pathname + "cpu_nodes.svg", "CPU", "CPU", COLOR_PALETTE
        )
        build_plot_nodes(
            resampled_dfs, plots_pathname + "mem_nodes.svg", "MEM", "Memory", COLOR_PALETTE
        )

    return plots_pathname


def generate_plots(stats_path) -> str:
    """
    Function to generate all the plots

    :param stats_path: pathname of the folder containing the data
    :return plots_folder: pathname containing the plots
    """
    plots_folder = None
    start_time = time.time()
    try:
        plots_folder = plot_results(stats_path)
        elapsed_time = time.time() - start_time
        print(f"PROVENANCE | Profiling plots generation TIME: {elapsed_time:.2f} s.")
    except:
        print("PROVENANCE | ERROR Could not generate the profiling plots.")

    return plots_folder
