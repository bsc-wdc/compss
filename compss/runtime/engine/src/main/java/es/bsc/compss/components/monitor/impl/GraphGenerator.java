/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.components.monitor.impl;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Task;
import es.bsc.compss.util.ErrorManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The Runtime Monitor class represents the component in charge to provide user with the current state of the execution.
 */
public class GraphGenerator {

    // Boolean to enable GraphGeneration or not
    private static final boolean MONITOR_ENABLED =
        System.getProperty(COMPSsConstants.MONITOR) != null && !"0".equals(System.getProperty(COMPSsConstants.MONITOR))
            ? true
            : false;
    private static final boolean DRAW_GRAPH =
        System.getProperty(COMPSsConstants.GRAPH) != null && "true".equals(System.getProperty(COMPSsConstants.GRAPH))
            ? true
            : false;
    private static final boolean GRAPH_GENERATOR_ENABLED = MONITOR_ENABLED || DRAW_GRAPH;

    // Stream dot description
    private static final String STREAM_DOT_DESCRIPTION = "[shape=rect style=\"rounded,filled\" width=0"
        + " height=0 margin=0.1 fontsize=10 fillcolor=\"#a9a9a9\" fontcolor=\"#000000\"]";

    // Graph filenames constants
    private static final String CURRENT_GRAPH_FILENAME = "current_graph.dot";
    private static final String COMPLETE_GRAPH_FILENAME = "complete_graph.dot";
    private static final String COMPLETE_GRAPH_TMP_FILENAME = "complete_graph.dot.tmp";
    private static final String COMPLETE_LEGEND_TMP_FILENAME = "complete_legend.dot.tmp";

    // Graph locations
    private static final String MONITOR_DIR_PATH;
    private static final String CURRENT_GRAPH_FILE;
    private static final String COMPLETE_GRAPH_FILE;
    private static final String COMPLETE_GRAPH_TMP_FILE;
    private static final String COMPLETE_LEGEND_TMP_FILE;
    // Graph buffers
    private static BufferedWriter full_graph;
    private static BufferedWriter current_graph;
    private static BufferedWriter legend;
    private static HashSet<Integer> legendTasks;
    private static int openCollectivesEdges = 0;
    private static HashMap<String, List<String>> pendingGroupDependencies = new HashMap<>();
    private static HashMap<String, List<Task>> openCommutativeGroups = new HashMap<>();

    private static final Logger LOGGER = LogManager.getLogger(Loggers.ALL_COMP);
    private static final String ERROR_MONITOR_DIR = "ERROR: Cannot create monitor directory";
    private static final String ERROR_ADDING_DATA = "Error adding task to graph file";
    private static final String ERROR_ADDING_EDGE = "Error adding edge to graph file";
    private static final String ERROR_OPEN_CURRENT_GRAPH = "Error openning current graph file";
    private static final String ERROR_CLOSE_CURRENT_GRAPH = "Error closing current graph file";
    private static final String ERROR_COMMIT_FINAL_GRAPH = "Error commiting full graph to file";

    static {
        if (GRAPH_GENERATOR_ENABLED) {
            // Set graph locations
            MONITOR_DIR_PATH = Comm.getAppHost().getAppLogDirPath() + "monitor" + File.separator;
            if (!new File(MONITOR_DIR_PATH).mkdir()) {
                ErrorManager.error(ERROR_MONITOR_DIR);
            }
            CURRENT_GRAPH_FILE = MONITOR_DIR_PATH + CURRENT_GRAPH_FILENAME;
            COMPLETE_GRAPH_FILE = MONITOR_DIR_PATH + COMPLETE_GRAPH_FILENAME;
            COMPLETE_GRAPH_TMP_FILE = MONITOR_DIR_PATH + COMPLETE_GRAPH_TMP_FILENAME;
            COMPLETE_LEGEND_TMP_FILE = MONITOR_DIR_PATH + COMPLETE_LEGEND_TMP_FILENAME;

            /* Current graph for monitor display ********************************************* */
            try {
                current_graph = new BufferedWriter(new FileWriter(CURRENT_GRAPH_FILE));
                emptyCurrentGraph();
                current_graph.close();
            } catch (IOException ioe) {
                LOGGER.error("Error generating current graph file", ioe);
            }

            /* Final graph for drawGraph option ********************************************* */
            try {
                full_graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_FILE));
                emptyFullGraph();
                full_graph.close();
            } catch (IOException ioe) {
                LOGGER.error("Error generating full graph file", ioe);
            }

            // Open a full graph working copy
            try {
                full_graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_TMP_FILE));
                openFullGraphFile(full_graph);
                openDependenceGraph(full_graph);
            } catch (IOException ioe) {
                LOGGER.error("Error generating graph file", ioe);
            }
            try {
                legend = new BufferedWriter(new FileWriter(COMPLETE_LEGEND_TMP_FILE));
            } catch (IOException ioe) {
                LOGGER.error("Error generating full graph working copy file", ioe);
            }
            legendTasks = new HashSet<>();
        } else {
            MONITOR_DIR_PATH = null;
            CURRENT_GRAPH_FILE = CURRENT_GRAPH_FILENAME;
            COMPLETE_GRAPH_FILE = COMPLETE_GRAPH_FILENAME;
            COMPLETE_GRAPH_TMP_FILE = COMPLETE_GRAPH_TMP_FILENAME;
            COMPLETE_LEGEND_TMP_FILE = COMPLETE_LEGEND_TMP_FILENAME;
        }
    }


    /**
     * Constructs a new Graph generator.
     */
    public GraphGenerator() {
        // All attributes are initialized in the static block. Nothing to do
    }

    /*
     ****************************************************************************************************************
     * PUBLIC STATIC METHODS
     ****************************************************************************************************************/
    /**
     * Returns whether the graph generator is enabled or not.
     *
     * @return {@code true} if the graph generator is enabled, {@code false} otherwise.
     */
    public static boolean isEnabled() {
        return GRAPH_GENERATOR_ENABLED;
    }

    /**
     * Returns the final monitor directory path.
     *
     * @return The final monitor directory path.
     */
    public static String getMonitorDirPath() {
        return MONITOR_DIR_PATH;
    }

    /*
     ****************************************************************************************************************
     * PUBLIC METHODS
     ****************************************************************************************************************/
    /**
     * Opens and initializes the current graph buffer file.
     */
    public BufferedWriter getAndOpenCurrentGraph() {
        try {
            current_graph = new BufferedWriter(new FileWriter(CURRENT_GRAPH_FILE));
            openCurrentGraphFile(current_graph);
        } catch (IOException e) {
            LOGGER.error(ERROR_OPEN_CURRENT_GRAPH);
            return null;
        }

        return current_graph;
    }

    /**
     * Closes header and buffer file of current graph.
     */
    public void closeCurrentGraph() {
        try {
            closeGraphFile(current_graph);
            current_graph.close();
        } catch (IOException e) {
            LOGGER.error(ERROR_CLOSE_CURRENT_GRAPH);
        }
    }

    /**
     * Prints in a file the final task graph.
     */
    public void commitGraph(boolean noMoreTasks) {
        LOGGER.debug("Commiting graph to final location");
        try {
            // Move dependence graph content to final location
            full_graph.close();

            try (FileInputStream sourceFIS = new FileInputStream(COMPLETE_GRAPH_TMP_FILE);
                FileOutputStream destFOS = new FileOutputStream(COMPLETE_GRAPH_FILE);
                FileChannel sourceChannel = sourceFIS.getChannel();
                FileChannel destChannel = destFOS.getChannel()) {

                destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
            }

            // Open tmp full graph again
            full_graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_TMP_FILE, true));

            // Close graph section
            BufferedWriter finalGraph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_FILE, true));
            closeDependenceGraph(finalGraph);

            // Move legend content to final location
            openLegend(finalGraph);

            legend.close();

            try (FileInputStream sourceFIS = new FileInputStream(COMPLETE_LEGEND_TMP_FILE);
                FileOutputStream destFOS = new FileOutputStream(COMPLETE_GRAPH_FILE, true);
                FileChannel sourceChannel = sourceFIS.getChannel();
                FileChannel destChannel = destFOS.getChannel();) {

                destChannel.position(destChannel.size());
                sourceChannel.transferTo(0, sourceChannel.size(), destChannel);
            }

            // Open tmp legend again
            legend = new BufferedWriter(new FileWriter(COMPLETE_LEGEND_TMP_FILE, true));
            if (noMoreTasks) {
                closeLegend(finalGraph);
                closeGraphFile(finalGraph);
            }

            // Close graph
            finalGraph.close();

        } catch (IOException e) {
            LOGGER.error(ERROR_COMMIT_FINAL_GRAPH, e);
        }
    }

    /**
     * Adds a synchro node to the graph.
     *
     * @param synchId New synchronization point.
     */
    public void addSynchroToGraph(int synchId) {
        try {
            full_graph.newLine();

            if (synchId == 0) {
                full_graph.write("Synchro" + synchId
                    + "[label=\"main\", shape=octagon, style=filled fillcolor=\"#8B0000\" fontcolor=\"#FFFFFF\"];");
            } else {
                full_graph.write("Synchro" + synchId
                    + "[label=\"sync\", shape=octagon, style=filled fillcolor=\"#ff0000\" fontcolor=\"#FFFFFF\"];");
            }
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Adds a barrier node to the graph.
     *
     * @param synchId New barrier point.
     */
    public void addBarrierToGraph(int synchId) {
        try {
            full_graph.newLine();
            full_graph.write("Synchro" + synchId
                + "[label=\"barrier\", shape=octagon, style=filled fillcolor=\"#ff0000\" fontcolor=\"#FFFFFF\"];");
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Adds a task node to the graph.
     *
     * @param task New task.
     */
    public void addTaskToGraph(Task task) {
        if (!task.hasCommutativeParams()) {
            addTask(task);
        }
        // else (has Commutative Params) -> the task is added when a group is assigned
    }

    private void addTask(Task task) {
        try {
            full_graph.newLine();
            full_graph.write(task.getDotDescription());
            int taskId = task.getTaskDescription().getCoreElement().getCoreId();
            if (!legendTasks.contains(taskId)) {
                legendTasks.add(taskId);
                legend.write(task.getLegendDescription());
            }
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Adds a task in a commutative grpu of to the graph.
     *
     * @param task New task.
     * @param identifier identifier of the commutative group
     */
    public void addTaskToCommutativeGroup(Task task, String identifier) {
        List<Task> tasks = openCommutativeGroups.get(identifier);
        if (tasks == null) {
            tasks = new LinkedList<>();
            openCommutativeGroups.put(identifier, tasks);
        }
        tasks.add(task);
    }

    /**
     * Adds a new stream node to the graph.
     *
     * @param label Stream node label name.
     */
    public void addStreamToGraph(String label) {
        try {
            full_graph.newLine();
            String dotDescription = label + STREAM_DOT_DESCRIPTION;
            full_graph.write(dotDescription);
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Start a group collection.
     */
    public void startGroupingEdges() {
        openCollectivesEdges += 1;
    }

    /**
     * Stop a group collection.
     */
    public void stopGroupingEdges() {
        openCollectivesEdges -= 1;
    }

    /**
     * Adds an edge to the graph from {@code src} to {@code tgt} with label {@code label}.
     *
     * @param src Source node.
     * @param tgt Target node.
     * @param edgeType Edge Type.
     * @param label Edge label.
     */
    public void addEdgeToGraph(String src, String tgt, EdgeType edgeType, String label) {
        if (openCollectivesEdges == 0) {
            if (pendingGroupDependencies.isEmpty()) {
                addSingleElementEdgeToGraph(src, tgt, edgeType, label);
            } else {
                for (Map.Entry<String, List<String>> entry : pendingGroupDependencies.entrySet()) {
                    String srctgt = entry.getKey();
                    try {
                        full_graph.newLine();
                        full_graph.write(srctgt + " [label=\"d" + label + " ("
                            + pendingGroupDependencies.get(srctgt).size() + ")" + "\",color=\"#024b30\",penwidth=2];");
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                pendingGroupDependencies.clear();
            }
        } else {
            List<String> labels = pendingGroupDependencies.get(src + " -> " + tgt);
            if (labels == null) {
                labels = new LinkedList<>();
                pendingGroupDependencies.put(src + " -> " + tgt, labels);
            }
            labels.add(label);
        }
    }

    private void addSingleElementEdgeToGraph(String src, String tgt, EdgeType edgeType, String label) {
        try {
            // Build the edge properties tag
            StringBuilder edgeProperties = new StringBuilder();
            String edgeTypeProps = edgeType.getProperties();
            if (!edgeTypeProps.isEmpty() || !label.isEmpty()) {
                edgeProperties.append(" [");
                if (!edgeTypeProps.isEmpty()) {
                    edgeProperties.append(edgeTypeProps);
                    if (!label.isEmpty()) {
                        edgeProperties.append(", ");
                    }
                }
                if (!label.isEmpty()) {
                    edgeProperties.append("label=\"d").append(label).append("\"");
                }
                edgeProperties.append("]");
            }
            edgeProperties.append(";");

            // Write entry
            full_graph.newLine();
            full_graph.write(src + " -> " + tgt + edgeProperties.toString());
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_EDGE, e);
        }
    }

    /**
     * Removes the temporary files.
     */
    public static void removeTemporaryGraph() {
        if (!new File(COMPLETE_GRAPH_TMP_FILE).delete()) {
            LOGGER.error("Cannot remove temporary graph file");
        }
        if (!new File(COMPLETE_LEGEND_TMP_FILE).delete()) {
            LOGGER.error("Cannot remove temporary legend file");
        }
    }

    /**
     * Adds an edge to the graph from a commutative group to a task from {@code src} to {@code tgt} with label
     * {@code label}.
     *
     * @param src Node from which the dependency arrow starts.
     * @param tgt Node to which the task is dependent.
     * @param label Data Id and version of the dependency.
     * @param identifier Commutative group identifier.
     * @param clusterType Commutative group or task group identifier.
     * @param edgeType Edge type.
     */
    public void addEdgeToGraphFromGroup(String src, String tgt, String label, String identifier, String clusterType,
        EdgeType edgeType) {
        try {
            // Build the edge properties tag
            StringBuilder edgeProperties = new StringBuilder();
            String edgeTypeProps = edgeType.getProperties();
            if (!edgeTypeProps.isEmpty() || !label.isEmpty()) {
                edgeProperties.append(" [");
                if (!edgeTypeProps.isEmpty()) {
                    edgeProperties.append(edgeTypeProps);
                    if (!label.isEmpty()) {
                        edgeProperties.append(", ");
                    }
                }
                if (!label.isEmpty()) {
                    edgeProperties.append("label=\"d").append(label).append("\"");
                }
                edgeProperties.append("]");
            }
            edgeProperties.append("[ ltail=\"").append(clusterType).append(identifier).append("\" ]");
            edgeProperties.append(";");

            // Print message
            full_graph.newLine();
            full_graph.write(src + " -> " + tgt + edgeProperties.toString());
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_EDGE, e);
        }
    }

    /**
     * Closes all the open Commutative Groups in the graph.
     */
    public void closeCommutativeGroups() {
        for (java.util.Map.Entry<String, List<Task>> entry : openCommutativeGroups.entrySet()) {
            String identifier = entry.getKey();
            List<Task> tasks = entry.getValue();
            printCommutativeGroup(identifier, tasks);
        }
        openCommutativeGroups.clear();
    }

    /**
     * Closes the given commutative group in the graph.
     *
     * @param identifier name of the group to close
     */
    public void closeCommutativeGroup(String identifier) {
        List<Task> tasks = openCommutativeGroups.remove(identifier);
        printCommutativeGroup(identifier, tasks);
    }

    private void printCommutativeGroup(String identifier, List<Task> tasks) {
        try {
            full_graph.newLine();

            StringBuilder msg1 = new StringBuilder();
            msg1.append("subgraph clusterCommutative").append(identifier).append(" {\n");
            full_graph.write(msg1.toString());

            StringBuilder msg2 = new StringBuilder();
            msg2.append("shape=rect;\n");
            msg2.append("node[height=0.75];\n");
            msg2.append("color=\"#A9A9A9\";\n");
            msg2.append("rank=same;\n");
            msg2.append("label=\"CGT").append(identifier).append("\";\n");
            full_graph.write(msg2.toString());
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
        if (tasks != null) {
            for (Task task : tasks) {
                addTask(task);
            }
        }
        try {
            full_graph.newLine();
            full_graph.write("}\n");
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Adds a reduce task to the graph.
     *
     * @param identifier Identifier of the task.
     */
    public void addReduceTaskToGraph(int identifier) {
        try {
            full_graph.newLine();

            StringBuilder msg1 = new StringBuilder();
            msg1.append("subgraph clusterReduce").append(identifier).append(" {\n");
            full_graph.write(msg1.toString());

            StringBuilder msg2 = new StringBuilder();
            msg2.append("shape=rect;\n");
            msg2.append("node[height=0.75];\n");
            msg2.append("color=\"#A9A9A9\";\n");
            msg2.append("rank=same;\n");
            msg2.append("label=\"RT").append(identifier).append("\";\n");
            full_graph.write(msg2.toString());
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Adds a task group to the graph.
     *
     * @param identifier Group identifier
     */
    public void addTaskGroupToGraph(String identifier) {
        try {
            full_graph.newLine();
            full_graph.write("subgraph clusterTasks" + identifier + " {\n");
            full_graph.write(
                "shape=rect;\n" + "node[height=0.75];\n" + "color=\"#A9A9A9\"; \n" + "label=\"" + identifier + "\";\n");
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Ends a commutative group subgraph.
     */
    public void closeGroupInGraph() {
        try {
            full_graph.newLine();
            full_graph.write("}\n");
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Starts a new subgraph.
     */
    public void createNewSubgraph() {
        try {
            full_graph.newLine();
            full_graph.write("subgraph{\n");
            full_graph.newLine();
            full_graph.write("                node[height=0.75];\n");
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /*
     * ***************************************************************************************************************
     * PRIVATE STATIC METHODS
     ****************************************************************************************************************/
    private static void emptyFullGraph() throws IOException {
        openFullGraphFile(full_graph);
        openDependenceGraph(full_graph);
        closeDependenceGraph(full_graph);
        openLegend(full_graph);
        closeLegend(full_graph);
        closeGraphFile(full_graph);
    }

    private static void emptyCurrentGraph() throws IOException {
        openCurrentGraphFile(current_graph);
        openDependenceGraph(current_graph);
        closeDependenceGraph(current_graph);
        closeGraphFile(current_graph);
    }

    private static void openFullGraphFile(BufferedWriter graph) throws IOException {
        graph.write("digraph {");
        graph.newLine();
        graph.write("  newrank=true;");
        graph.newLine();
        graph.write("  rankdir=TB;");
        graph.newLine();
        graph.write("  labeljust=\"l\";");
        graph.newLine();
        graph.write("  compound= true;");
        graph.newLine();
        graph.flush();
    }

    private static void openCurrentGraphFile(BufferedWriter graph) throws IOException {
        graph.write("digraph {");
        graph.newLine();
        graph.write("  rankdir=TB;");
        graph.newLine();
        graph.write("  labeljust=\"l\";");
        graph.newLine();
        graph.write("  compound= true;");
        graph.newLine();
        graph.flush();
    }

    private static void openDependenceGraph(BufferedWriter graph) throws IOException {
        graph.write("  subgraph dependence_graph {");
        graph.newLine();
        graph.write("    ranksep=0.20;");
        graph.newLine();
        graph.write("    node[height=0.75];");
        graph.newLine();
        graph.flush();
    }

    private static void openLegend(BufferedWriter graph) throws IOException {
        graph.write("  subgraph legend {");
        graph.newLine();
        graph.write("    rank=sink;");
        graph.newLine();
        graph.write("    node [shape=plaintext, height=0.75];");
        graph.newLine();
        graph.write("    ranksep=0.20;");
        graph.newLine();
        graph.write("    label = \"Legend\";");
        graph.newLine();
        graph.write("    key [label=<");
        graph.newLine();
        graph.write("      <table border=\"0\" cellpadding=\"2\" cellspacing=\"0\" cellborder=\"1\">");
        graph.newLine();
        graph.flush();
    }

    private static void closeGraphFile(BufferedWriter graph) throws IOException {
        graph.write("}");
        graph.newLine();
        graph.flush();
    }

    private static void closeDependenceGraph(BufferedWriter graph) throws IOException {
        graph.write("  }");
        graph.newLine();
        graph.flush();
    }

    private static void closeLegend(BufferedWriter graph) throws IOException {
        graph.write("      </table>");
        graph.newLine();
        graph.write("    >]");
        graph.newLine();
        graph.write("  }");
        graph.newLine();
        graph.flush();
    }

}
