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

"""COMPSs trace merger with EAR trace."""

import argparse
import collections
import glob
import operator
import os
import shutil
import typing


EVENT_OFFSET = 9100000


class MergerException(Exception):
    """Trace merger custom exception."""


class Trace:
    """Represents a trace (set of three files)."""

    def __init__(self, name: str) -> None:
        """Trace constructor.

        :param name: Trace name.
        """
        self.prv = f"{name}.prv"
        self.pcf = f"{name}.pcf"
        self.row = f"{name}.row"


def find_traces(
    energy_trace: str, compss_trace: str, output_trace: str
) -> typing.Tuple[Trace, Trace, Trace]:
    """Set the trace files from their names.

    :param energy_trace: EAR trace.
    :param compss_trace: COMPSs trace.
    :param output_trace: Output trace.
    :return: Trace objects from the three given trace names.
    """
    energy = Trace(energy_trace)
    compss = Trace(compss_trace)
    output = Trace(output_trace)
    return energy, compss, output


def merge_pcfs(energy: Trace, compss: Trace, output: Trace) -> None:
    """Merge and write PCF result file.

    :param pcf_data: EAR PCF data.
    :param compss_file: COMPSs PCF file.
    :param output_file: Result PCF file.
    :return: None
    """
    # Get energy pcf data
    with open(energy.pcf, "r", encoding="utf8") as pcf_fd:
        energy_data = pcf_fd.readlines()
    filtered_data = []
    for line in energy_data:
        if line.startswith("0"):
            l = line.strip().split("\t")
            filtered_data.append(l[1:])
    print(f"\t- Found metrics: {len(filtered_data)}")
    for i in filtered_data:
        print(f"\t\t- {i[1]}")
    # Read COMPSs pdf
    with open(compss.pcf, "r", encoding="utf8") as compss_fd:
        compss_data = compss_fd.read()
    # Merge contents
    pcf_events = ""
    for event in filtered_data:
        event_number = int(event[0]) + EVENT_OFFSET
        event_name = event[1]
        pcf_events = f"{pcf_events}EVENT_TYPE\n0\t{event_number}\t{event_name}\n\n"
    # Write results
    with open(output.pcf, "w", encoding="utf8") as output_fd:
        output_fd.write(compss_data)
        output_fd.write("\n")
        output_fd.write(pcf_events)


def get_ids(compss: Trace) -> typing.Tuple[bool, list[str], dict]:
    """Construct an auxiliar structure from the COMPS prv header.

    Requires the structure of nodes, applications and threads to build it.

    :param compss: COMPSs trace.
    :return: If the deployment has worker in master, a list of absolute
             ids, and a dictionary containing the nodes as key and in the
             value field a set of pairs containing the process id and its
             relative cpu value.
    """
    with open(compss.prv, "r", encoding="utf8") as prv_fd:
        prv_header = prv_fd.readline()
    header = prv_header.strip()
    distribution = ":".join(header.split(":")[5:])
    nodes = int(header.split(":")[3].split("(")[0])
    applications = int(header.split(":")[4])
    print(f"\t- Nodes: {nodes}")
    print(f"\t- Applications: {applications}")
    worker_in_master = False
    if applications == nodes + 1:
        worker_in_master = True
    print(f"\t- Worker in master: {worker_in_master}")
    plain_nodes = []
    for node in distribution.split("),"):
        if node.endswith(")"):
            plain_nodes.append(node[:-1])
        else:
            plain_nodes.append(node)
    print(f"\t- Trace header: {header}")
    total_procs = 0
    ids = [1]  # master id is 0
    master_procs_list = []
    worker_procs_list = []
    for node in plain_nodes:
        elements, content = node.split("(")
        processes = content.split(",")
        procs = int(processes[0].split(":")[0])
        if elements == "1":
            # Is master
            total_procs += procs
            master_procs_list.append(procs)
            worker_procs_list.append(0)
        if elements == "2":
            # Is worker
            master_procs_list.append(procs)
            for proc in processes:
                total_procs += int(proc.split(":")[0])
                worker_procs_list.append(int(proc.split(":")[0]))
                ids.append(int(proc.split(":")[1]) + 1)
    # Remove all worker master procs from worker_procs_list.
    del worker_procs_list[1::2]
    # Remove all master ids from ids.
    del ids[1::2]
    processes = list(zip(ids, master_procs_list, worker_procs_list))
    print(f"\t- Total amount of processes: {total_procs}")
    print(f"\t- Processes: {processes}")
    master_procs = processes[0]
    worker_procs = processes[1:]
    print(f"\t\t- Master: {master_procs}")
    print(f"\t\t- Worker: {worker_procs}")
    node_dict = {}
    accum_procs = master_procs[1]
    first = True
    worker_procs_ids = list(zip(*worker_procs))[0]
    for id, m, w in sorted(worker_procs, key=operator.itemgetter(0)):
        accum_procs += m - 1  # the last one is the python main worker process
        worker_relative_threads = list(range(1, w + 1))
        worker_relative_threads.reverse()
        # print(worker_relative_threads)
        all_threads = list(range(accum_procs, accum_procs + w + 1))
        if first and worker_in_master:
            main_thread = [(all_threads[0], master_procs[1] + m - 2)]
            worker_threads = list(zip(all_threads[1:], worker_relative_threads))
            first = False
        else:
            main_thread = [(all_threads[0], m - 1)]
            worker_threads = list(zip(all_threads[1:], worker_relative_threads))
        threads = main_thread + worker_threads
        real_id = worker_procs_ids.index(id) + 2
        node_dict[str(real_id)] = threads
        accum_procs += w
    filter_data = []
    for id, proc_ids in node_dict.items():
        procs, threads = zip(*proc_ids)
        filter_data = filter_data + list(procs)
    # print(filter_data)
    # print(len(filter_data))
    # print(node_dict)
    # print(f"len(node_dict): {len(node_dict)}")
    # for k, v in node_dict.items():
    #     print(f"- {k} - {len(v)}")
    #     print(f"- {k} - {v}")
    return worker_in_master, filter_data, node_dict


def get_energy_events(energy: Trace) -> list[list[str]]:
    """Read EAR trace.

    Additionally, adds the event id offset so that they are
    defined in a larger group (EVENT_OFFSET).

    :param energy: EAR trace.
    :return: List of events (each of the splitted in elements).
    """
    with open(energy.prv, "r", encoding="utf8") as energy_fd:
        data = energy_fd.readlines()
    events = data[1:]
    events_table = []
    for i in events:
        event = i.strip().split(":")
        if event[3] == "1" and event[4] == "1":
            event[6] = str(int(event[6]) + EVENT_OFFSET)
            events_table.append(event)
    return events_table


def adapt_ear_events(events: list[list[str]], ids: list, node_ids: dict) -> list:
    """Adapt EAR events to COMPSs structure.

    EAR trace contains events with a different structure than COMPSs:
        1:CPU:APPL:TASK:THREAD:TIME:EVENT_TYPE:EVENT_VALUE

    :param events: EAR events.
    :param ids: List of CPU ids (absolute position)
    :param node_ids: Dictionary of nodes containing tuples
                     (process id, relative CPU id)
    :return: Updated EAR events.
    """
    ear_events = []
    for e in events:
        event = e.copy()
        ear_cpu_id = event[2]
        # Absolute position in cpu
        event[1] = ids[int(ear_cpu_id) - 1]
        for k, v in node_ids.items():
            procs, threads = zip(*v)
            if event[1] in procs:
                position = procs.index(event[1])
                event[2] = k
                event[3] = "2"
                # Relative position in cpu
                event[4] = str(threads[position])
        ear_events.append(event)
    return ear_events


def find_events(
    data: list[list[str]], group: str, event_id: str, is_ear: bool
) -> list[list[str]]:
    """Find all events in data that match group and event id.

    It is able to find them in COMPSs and in EAR traces.

    :param data: _description_
    :param group: _description_
    :param event_id: _description_
    :param is_ear: _description_
    :return: _description_
    """
    events = []
    for event in data:
        line_events = event[6:]
        if is_ear:
            if group in line_events[0::2]:
                events.append(event)
        else:
            if (
                group in line_events[0::2]
                and event_id == line_events[line_events.index(group) + 1]
            ):
                events.append(event)
    return events


def synchronize_ear_events(
    ear_events: list[list[str]], compss: Trace
) -> typing.Tuple[list[list[str]], list[list[str]]]:
    """Adapt the timing of the EAR events to the COMPSs trace.

    :param ear_events: EAR trace events.
    :param compss: COMPSs trace.
    :return: COMPSs events and EAR events.
    """
    # Read all COMPSs events
    compss_events = []
    with open(compss.prv, "r", encoding="utf8") as fd_compss:
        for line in fd_compss:
            compss_events.append(line.rstrip().split(":"))
    # Find thread start timestamps
    compss_starts = find_events(compss_events, "9000200", "1", False)
    ear_starts = find_events(ear_events, "9100081", "0", True)
    # Set the main start timestamp
    compss_start = compss_starts[0][5]
    ear_start = ear_starts[0][ear_starts[0].index("9100081") + 1]
    print(f"\t- COMPSs start time: {compss_start}")
    print(f"\t- EAR start time: {ear_start}")
    offset = int(compss_start) - int(ear_start)
    print(f"\t- Offset time: {offset}")
    # Update the EAR events timestamp
    for event in ear_events:
        # Seconds to nanoseconds plus offset
        event[5] = str((int(event[5]) * 1000) + offset)
    return compss_events, ear_events


def write_events(
    compss_events: list[list[str]], ear_events: list[list[str]], output: Trace
) -> None:
    """Save all events in the output trace PRV file.

    :param compss_events: COMPSs events.
    :param ear_events: EAR events.
    :param output: Output trace result file.
    """
    output_events = compss_events + ear_events
    sorted_output = [output_events[0]] + sorted(
        output_events[1:], key=lambda x: (int(x[5]))
    )
    with open(output.prv, "w", encoding="utf8") as prv_fd:
        for event_i in sorted_output:
            prv_fd.write(":".join(map(str, event_i)) + "\n")


def copy_row(compss: Trace, output: Trace) -> None:
    """Copy the COMPSs row file into the output row file.

    In the merging process, we reuse the COMPSs row file.

    :param compss: COMPSs trace.
    :param output: Result trace.
    :return: None
    """
    shutil.copyfile(compss.row, output.row)


def process_parameters() -> typing.Tuple[str, str, str]:
    """Process the given parameters.

    :return: EAR generated trace name, COMPSs generated trace name and output trace name.
    """
    parser = argparse.ArgumentParser(description="COMPSs and EAR trace merger")
    parser.add_argument("-c", "--compss_trace", help="COMPSs trace file", type=str)
    parser.add_argument("-e", "--ear_trace", help="EAR trace file", type=str)
    parser.add_argument("-o", "--output_trace", help="Output trace", type=str)
    parser.add_argument(
        "-d",
        "--debug",
        help="Show debug messages (will affect performance)",
        action="store_true",
    )
    args = parser.parse_args()
    current_dir = os.getcwd()

    if args.compss_trace:
        compss_folder, compss_file = os.path.split(args.compss_trace)
    else:
        compss_folder = os.path.join(current_dir, "trace")
        compss_absolute_file = glob.glob(f"{compss_folder}/*.prv")[0]
        _, compss_file = os.path.split(compss_absolute_file)
    # compss_file does not keep the extension
    compss_file = os.path.splitext(compss_file)[0]

    if args.ear_trace:
        ear_folder, ear_file = os.path.split(args.ear_trace)
    else:
        ear_folder = os.path.join(current_dir, "energy")
        ear_absolute_file = glob.glob(f"{ear_folder}/*.prv")[0]
        _, ear_file = os.path.split(ear_absolute_file)
    # ear_file does not keep the extension
    ear_file = os.path.splitext(ear_file)[0]

    if args.output_trace:
        output_folder, output_file = os.path.split(args.output_trace)
        output_file = os.path.splitext(output_file)[0]
    else:
        output_folder = os.path.join(os.getcwd(), "final_trace")
        output_file = compss_file

    os.makedirs(output_folder, exist_ok=True)

    if args.debug:
        print("Parameters:")
        print(f"- COMPSs trace folder: {compss_folder}")
        print(f"- COMPSs trace name: {compss_file}")
        print(f"- EAR trace folder: {ear_folder}")
        print(f"- EAR trace name: {ear_file}")
        print(f"- Output trace folder: {output_folder}")
        print(f"- Output trace name: {output_file}")

    # Sanity checks:
    if not compss_file or not os.path.exists(
        os.path.join(compss_folder, f"{compss_file}.prv")
    ):
        raise MergerException("Could not find COMPSs trace file.")

    if not ear_file or not os.path.exists(os.path.join(ear_folder, f"{ear_file}.prv")):
        raise MergerException("Could not find EAR trace file.")

    compss_parameter = os.path.join(compss_folder, compss_file)
    ear_parameter = os.path.join(ear_folder, ear_file)
    output_parameter = os.path.join(output_folder, output_file)

    return ear_parameter, compss_parameter, output_parameter


def main() -> None:
    """Merge the COMPSs trace with the EAR trace into the output trace.

    To this end, adapts the EAR events to the COMPSs trace structure.

    :param energy_trace: EAR generated trace name.
    :param compss_trace: COMPSs generated trace name.
    :param output_trace: Output trace name.
    """
    print("Merging EAR events into COMPSs trace")

    print("- Processing parameters")
    energy_trace, compss_trace, output_trace = process_parameters()
    print("- Find traces")
    energy, compss, output = find_traces(energy_trace, compss_trace, output_trace)

    print("- Processing PCF")
    merge_pcfs(energy, compss, output)

    print("- Creating auxiliar structure (process, threads) for event adaptation.")
    worker_in_master, ids, node_ids = get_ids(compss)
    print(f"- Worker in master: {worker_in_master}")
    print(f"- Threads: {len(ids)}")
    print(f"- Nodes: {len(node_ids)}")
    for k, v in node_ids.items():
        print(f"\t- Worker threads in node {k}: {len(v)}")

    print("- Get EAR events")
    ear_events = get_energy_events(energy)
    print("- Adapt EAR events")
    adapted_ear_events = adapt_ear_events(ear_events, ids, node_ids)
    print("- Synchronize EAR events")
    compss_events, final_ear_events = synchronize_ear_events(adapted_ear_events, compss)

    print("- Writing PRV file")
    write_events(compss_events, final_ear_events, output)

    print("- Copy ROW file")
    copy_row(compss, output)


if __name__ == "__main__":
    main()
