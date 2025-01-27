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
import os

import matplotlib.pyplot as plt
import pandas as pd


def build_plot(title, time_list, value_list, name_dataset, measure, num_entries):
    plt.style.use('ggplot')
    fig, ax = plt.subplots(figsize=(12, 8))

    if max(value_list) > 10**6:
        value_list = [value / 10 ** 6 for value in value_list]
        measure='Megabyte (MB)'

    ax.plot(time_list, value_list, marker='.', linestyle='-', label=name_dataset)

    step = int(num_entries / 60) + 1
    labels = [f"{time_list[i]}" for i in range(0, len(time_list), step)]
    time_list = [i for i in range(0, len(time_list), step)]

    plt.xticks(time_list, labels=labels, rotation=80)
    plt.subplots_adjust(top=0.95, bottom=0.25)

    ax.set_title(title, fontsize=20)
    ax.set_xlabel('Timestamp')
    ax.set_ylabel(measure)
    ax.grid(True)
    ax.legend()


def plot_results(folder_pathname):
    folder_pathname = str(folder_pathname)
    # with open(folder_pathname + '/stats.json', 'r') as f:
    #     stats_file = json.load(f)

    # try:
    #     master_node = stats_file['COMPSS_MASTER_NODE'].strip().upper()
    # except:
    #     master_node = None

    if not os.path.exists(folder_pathname):
        exit(1)

    plots_pathname = folder_pathname + '/plots/'

    # iterate on every file in the directory
    for csv_resources in os.listdir(folder_pathname):
        master_node = not 'static_' in csv_resources

        csv_resources = os.path.join(folder_pathname, csv_resources)
        if not csv_resources.endswith(".csv") or os.path.isdir(csv_resources):
            continue

        machine_name = csv_resources.split('.csv')[0].split('_')[-1]
        # if machine_name.strip().upper() == master_node:
        #     machine_name += '(MASTER)'
        if master_node:
            machine_name += "-MASTER"

        df = pd.read_csv(csv_resources)
        df_lenght = len(df)

        cpu_usage = df["CPU"]
        mem_usage = df["MEM"]
        byte_sent = df["BYTE_SENT"]
        byte_recv = df["BYTE_RECV"]
        byte_read_disk = df["BYTE_READ_DISK"]
        byte_write_disk = df["BYTE_WRITE_DISK"]
        time_read_disk = df["TIME_READ_DISK"]
        time_write_disk = df["TIME_WRITE_DISK"]
        timestamps = df["TIME"]
        # timestamps = [i for i in range(0, len(timestamps))]

        output_path = plots_pathname + machine_name
        os.makedirs(output_path, exist_ok=True)

        build_plot('CPU usage', timestamps, cpu_usage, name_dataset='CPU', measure='CPU %', num_entries=df_lenght)
        plt.savefig(output_path+'/cpu.png')
        plt.close()

        build_plot('Memory usage', timestamps, mem_usage, name_dataset='MEM', measure='Memory %', num_entries=df_lenght)
        plt.savefig(output_path + '/mem.png')
        plt.close()

        build_plot('Data transferred: bytes sent', timestamps, byte_sent, name_dataset='BYTE_SENT', measure='Byte (B)', num_entries=df_lenght)
        plt.savefig(output_path + '/bytes_sent.png')
        plt.close()

        build_plot('Data transferred: bytes received', timestamps, byte_recv, name_dataset='BYTE_RECV', measure='Byte (B)', num_entries=df_lenght)
        plt.savefig(output_path + '/bytes_received.png')
        plt.close()

        build_plot('Disk usage: bytes written', timestamps, byte_write_disk, name_dataset='BYTE_WRITE_DISK', measure='Byte (B)', num_entries=df_lenght)
        plt.savefig(output_path + '/bytes_written.png')
        plt.close()

        build_plot('Disk usage: bytes read', timestamps, byte_read_disk, name_dataset='BYTE_READ_DISK', measure='Byte (B)', num_entries=df_lenght)
        plt.savefig(output_path + '/bytes_read.png')
        plt.close()

    return plots_pathname

def generate_plots(stats_path):
    plots_folder = None
    try:
        plots_folder = plot_results(stats_path)
        print(
            "PROVENANCE | Generation of the profiling plots."
        )
    except:
        print(
            "PROVENANCE | ERROR Could not generate the profiling plots."
        )

    return plots_folder