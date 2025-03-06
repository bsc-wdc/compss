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

# For better print formatting
from __future__ import print_function

# Imports
import networkx as nx


#
# Args class
#
class Args(object):
    """
    Creates an object containing all the information from the command line arguments

    Attributes:
        - dot_file_path : Complete path to the input DOT file
            + type: String
        - output_file_path : Complete path to the ouput PDF file
            + type: String
    """

    _DEFAULT_OUTPUT_EXTENSION = ".pdf"

    def __init__(self, dot_file_path=None, output_file_path=None):
        """
        Initializes the Args object

        :param dot_file_path: Complete path to the input DOT file
            + type: String
        :param output_file_path: Complete path to the output PDF file
            + type: String
        """
        # Parse DOT file path
        import os

        if dot_file_path.endswith(".dot"):
            self.dot_file_path = os.path.abspath(dot_file_path)
        else:
            self.dot_file_path = os.path.abspath(dot_file_path + ".dot")

        # Parse PDF file path
        if output_file_path is None:
            # PDF file with same name than DOT file but changing extension
            self.output_file_path = (
                self.dot_file_path[:-4] + Args._DEFAULT_OUTPUT_EXTENSION
            )
        else:
            self.output_file_path = os.path.abspath(output_file_path)

    def get_dot_file_path(self):
        """
        Returns the complete file path of the input DOT file

        :return: The complete file path of the input DOT file
            + type: String
        """
        return self.dot_file_path

    def get_output_file_path(self):
        """
        Returns the complete file path of the output PDF file

        :return: The complete file path of the output PDF file
            + type: String
        """
        return self.output_file_path


#
# Graph class
#
class Graph(object):
    """
    Contains an in memory representation of the graph

    Attributes:
        - nodes : List of defined nodes
            + type: dict<String> = Tuple(String, String, String)
        - edges : List of defined edges
            + type: dict<Tuple(String, String)> = Tuple(String,)
        - g : Representation of the graph
            + type: networkx.DiGraph
    """

    def __init__(self, dot_file_path=None):
        """
        Initializes the Graph object

        :param dot_file_path: Complete file path of the input DOT file
            + type: String
        """
        if dot_file_path is None:
            raise Exception("ERROR: Empty input DOT file path")

        self.nodes = {}
        self.edges = {}

        # Always add Synchro0 starting point
        self.nodes["Synchro0"] = ("octagon", "#ff0000", "#FFFFFF")

        # Add nodes and edges from DOT file
        with open(dot_file_path) as f:
            for line in f:
                if (
                    ("shape" in line)
                    and ("fillcolor" in line)
                    and ("fontcolor" in line)
                ):
                    if "label" in line:
                        # Line defines a sync
                        l2 = line.split(",")

                        s_index = l2[0].index("[")
                        node_name = l2[0][:s_index]
                        shape = l2[1][len("shape=") + 1 :]
                        self.nodes[node_name] = (shape, "#ff0000", "#FFFFFF")
                    else:
                        # print("Adding node " + line)
                        # Line defines a node
                        l2 = line.split(",")

                        s_index = l2[0].index("[")
                        node_name = l2[0][:s_index]
                        shape = l2[0][s_index + 7 :]

                        l3 = l2[1].split()
                        fillcolor = l3[1][len('fillcolor="') : -1]
                        fontcolor = l3[2][len('fontcolor="') : -3]

                        self.nodes[node_name] = (shape, fillcolor, fontcolor)
                elif "->" in line:
                    # Line defines an edge
                    f_index = line.index("->")
                    node_from = line[:f_index].strip()
                    if "[" in line:
                        s_index = line.index("[")
                        e_index = line.index("]")
                        node_to = line[f_index + 2 : s_index].strip()
                        label = line[s_index + 9 : e_index - 2]
                    else:
                        s_index = line.index(";")
                        node_to = line[f_index + 2 : s_index].strip()
                        label = ""

                    self.edges[(node_from, node_to)] = (label,)

        # if __debug__:
        #     print("List of Nodes:")
        #     print(self.nodes)
        #     print("List of Edges")
        #     print(self.edges)

        # Create the graph
        self.g = nx.DiGraph()
        for node_name, node_info in self.nodes.items():
            self.g.add_node(node_name)
            # shape=node_info[0], style="filled", color="black", fillcolor=node_info[1], fontcolor=node_info[2])
        for edge, edge_info in self.edges.items():
            self.g.add_edge(u_of_edge=edge[0], v_of_edge=edge[1])  # label=edge_info[0])

        # if __debug__:
        #     print("Graph contents - List of Nodes:")
        #     print(self.g.nodes)
        #     print("Graph contents - List of Edges:")
        #     print(self.g.edges)

    def render(self, output_file_path=None):
        """
        Renders the in-memory graph into the given file path

        :param output_file_path: Complete file path of the output PDF file
            + type: String
        :return: None
        """
        if output_file_path is None:
            raise Exception("ERROR: Empty output PDF file path")

        # Compute node colors
        color_map = []
        for node_name in self.g.nodes:
            node_info = self.nodes[node_name]
            color_map.append(node_info[1])

        # if __debug__:
        #     print("Graph contents - List of Node colors:")
        #     print(color_map)

        # Compute depths
        depth_per_node = {}
        self._compute_depths(
            current_node="Synchro0", current_depth=0, depths=depth_per_node
        )
        # if __debug__:
        #     print("Depth per node:")
        #     print(depth_per_node)

        # Compute nodes on each depth
        nodes_per_depth = Graph._compute_nodes_per_depth(depths=depth_per_node)
        # if __debug__:
        #     print("Nodes per depth:")
        #     print(nodes_per_depth)

        # Create layout
        pos = Graph._compute_layout(
            nodes_per_depth=nodes_per_depth, width=1.0, vert_gap=0.2, vert_loc=0.0
        )
        # if __debug__:
        #     print("Layout:")
        #     print(pos)

        # Draw
        import matplotlib.pyplot as plt

        nx.draw(
            self.g,
            pos=pos,  # Node position
            arrows=True,  # Draw edge arrows
            arrowsize=2,  # Edge arrows size
            width=0.3,  # Edge size
            node_size=20,  # Node size
            node_color=color_map,  # Node color
            with_labels=True,  # Node labels
            font_size=1,  # Node labels font size
        )
        plt.savefig(output_file_path)

    def _compute_depths(self, current_node=None, current_depth=0, depths=None):
        """
        Computes the depths of the current node and its children assuming the given
        current_depth

        :param current_node: Name of the current node
            + type: String
        :param current_depth: Value of the current depth
            + type: Int
        :param depths: List of depths per node
            + type: Dict<String> = Int
        :return: None
        """
        if current_node is None:
            return {}

        # Process current node
        if depths is None:
            depths = {current_node: current_depth}
        else:
            if current_node in depths:
                depths[current_node] = max(depths[current_node], current_depth)
            else:
                depths[current_node] = current_depth

        # Iterate over children
        current_depth = current_depth + 1
        for child in self.g.neighbors(current_node):
            self._compute_depths(child, current_depth, depths)

    @staticmethod
    def _compute_nodes_per_depth(depths=None):
        """
        From a list of depths per node builds a list of nodes per depth

        :param depths: List of depths per node
            + type: Dict<String> = Int
        :return: List of nodes per depth
            + type: Dict<Int> = String
        """
        nodes_per_depth = {}
        for node, depth in depths.items():
            if depth in nodes_per_depth:
                nodes_per_depth[depth].append(node)
            else:
                nodes_per_depth[depth] = [node]

        return nodes_per_depth

    @staticmethod
    def _compute_layout(nodes_per_depth=None, width=1.0, vert_gap=0.2, vert_loc=0.0):
        """
        Given a list of nodes per depth and some visual sizes, computes the layout of
        all the nodes of the graph

        :param nodes_per_depth: List of nodes per depth
            + type: Dict<Int> = String
        :param width: Layout width
            + type: double
        :param vert_gap: Space between rows
            + type: double
        :param vert_loc: Starting vertical point
            + type: double
        :return: The layout of the graph
            + type: Dict<String> = (x, y)
        """
        pos = {}
        for depth in sorted(nodes_per_depth.keys()):
            nodes = nodes_per_depth[depth]
            dx = width / len(nodes)

            horz_loc = 0
            for node in nodes:
                horz_loc += dx
                pos[node] = (horz_loc, vert_loc)

            vert_loc = vert_loc - vert_gap

        return pos


############################################
# HELPER METHODS
############################################


def parse_arguments(cmd_args):
    """
    Parses command line arguments and returns an object containing the application information

    :param cmd_args: Command line arguments
        + type: List
    :return: Object containing the application information
        + type: Args
    """
    if len(cmd_args) == 1:
        dot_file_path = cmd_args[0]
        output_file_path = None
    elif len(cmd_args) == 2:
        dot_file_path = cmd_args[0]
        output_file_path = cmd_args[1]
    else:
        raise Exception("ERROR: Invalid number of parameters")

    return Args(dot_file_path=dot_file_path, output_file_path=output_file_path)


def process_graph(args):
    """
    Construct an in-memory representation of the given graph

    :param args: Application information
        + type: Args
    :return: An object containing the graph representation
        + type: Graph
    """
    dot_file_path = args.get_dot_file_path()
    return Graph(dot_file_path)


def render_graph(graph, args):
    """
    Render the given graph to the output location

    :param graph: Object containing the graph representation
        + type: Graph
    :param args: Application information
        + type: Args
    :return: None
    """
    output_file_path = args.get_output_file_path()
    graph.render(output_file_path)


############################################
# MAIN
############################################


def main():
    print("Starting Graph rendering...")
    # Import libraries
    import time
    import sys

    # Parse arguments
    if __debug__:
        print("[DEBUG] Parsing arguments...")
    time_start = time.time()
    args = parse_arguments(sys.argv[1:])
    time_arguments_end = time.time()
    if __debug__:
        print("[DEBUG] Arguments parsed")
        time_arguments = time_arguments_end - time_start
        print("[DEBUG] Arguments parsing time: " + str(time_arguments))

    # Process graph
    if __debug__:
        print("[DEBUG] Processing graph...")
    time_process_start = time.time()
    graph = process_graph(args)
    time_process_end = time.time()
    if __debug__:
        print("[DEBUG] Graph processed")
        time_process = time_process_end - time_process_start
        print("[DEBUG] Graph processing time: " + str(time_process))

    # Render graph
    if __debug__:
        print("[DEBUG] Rendering graph...")
    time_render_start = time.time()
    render_graph(graph, args)
    time_render_end = time.time()
    if __debug__:
        print("[DEBUG] Graph rendered")
        time_render = time_render_end - time_render_start
        print("[DEBUG] Graph rendering time: " + str(time_render))

    # END
    time_end = time.time()
    time_total = time_end - time_start
    print("Elapsed time: " + str(time_total))
    print("Graph rendering finished")


if __name__ == "__main__":
    main()
