/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Task;

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
    private static final boolean DRAW_GRAPH =
        System.getProperty(COMPSsConstants.GRAPH) != null && "true".equals(System.getProperty(COMPSsConstants.GRAPH));
    private static final boolean GRAPH_GENERATOR_ENABLED = RuntimeMonitor.isEnabled() || DRAW_GRAPH;

    // Stream dot description
    private static final String STREAM_DOT_DESCRIPTION = "[shape=rect style=\"rounded,filled\" width=0"
        + " height=0 margin=0.1 fontsize=10 fillcolor=\"#a9a9a9\" fontcolor=\"#000000\"]";

    // Graph filenames constants
    private static final String CURRENT_GRAPH_FILENAME = "current_graph.dot";
    private static final String COMPLETE_GRAPH_FILENAME = "complete_graph.dot";
    private static final String COMPLETE_GRAPH_TMP_FILENAME = "complete_graph.dot.tmp";
    private static final String COMPLETE_LEGEND_TMP_FILENAME = "complete_legend.dot.tmp";

    private static final Logger LOGGER = LogManager.getLogger(Loggers.ALL_COMP);
    private static final String ERROR_ADDING_DATA = "Error adding task to graph file";
    private static final String ERROR_ADDING_EDGE = "Error adding edge to graph file";
    private static final String ERROR_OPEN_CURRENT_GRAPH = "Error openning current graph file";
    private static final String ERROR_CLOSE_CURRENT_GRAPH = "Error closing current graph file";
    private static final String ERROR_COMMIT_FINAL_GRAPH = "Error commiting full graph to file";

    // Graph locations
    private final String currentGraphPath;
    private final String completeGraphPath;
    private final String completeGraphTmpPath;
    private final String completeLegendTmpPath;
    // Graph buffers
    private BufferedWriter fullGraph;
    private BufferedWriter currentGraph;
    private BufferedWriter legend;
    private HashSet<Integer> legendTasks;
    private int openCollectivesEdges = 0;
    private HashMap<String, List<Task>> openCommutativeGroups = new HashMap<>();
    private HashMap<String, List<String>> pendingGroupDependencies = new HashMap<>();


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
     * Constructs a new Graph generator.
     * 
     * @param graphPath path where the graph will be left.
     */
    public GraphGenerator(String graphPath) {
        // Set graph locations
        currentGraphPath = graphPath + CURRENT_GRAPH_FILENAME;
        completeGraphPath = graphPath + COMPLETE_GRAPH_FILENAME;
        completeGraphTmpPath = graphPath + COMPLETE_GRAPH_TMP_FILENAME;
        completeLegendTmpPath = graphPath + COMPLETE_LEGEND_TMP_FILENAME;
        if (GRAPH_GENERATOR_ENABLED) {
            /* Current graph for monitor display ********************************************* */
            try {
                currentGraph = new BufferedWriter(new FileWriter(currentGraphPath));
                emptyCurrentGraph();
                currentGraph.close();
            } catch (IOException ioe) {
                LOGGER.error("Error generating current graph file", ioe);
            }

            /* Final graph for drawGraph option ********************************************* */
            try {
                fullGraph = new BufferedWriter(new FileWriter(completeGraphPath));
                emptyFullGraph();
                fullGraph.close();
            } catch (IOException ioe) {
                LOGGER.error("Error generating full graph file", ioe);
            }

            // Open a full graph working copy
            try {
                fullGraph = new BufferedWriter(new FileWriter(completeGraphTmpPath));
                openFullGraphFile(fullGraph);
                openDependenceGraph(fullGraph);
            } catch (IOException ioe) {
                LOGGER.error("Error generating graph file", ioe);
            }
            try {
                legend = new BufferedWriter(new FileWriter(completeLegendTmpPath));
            } catch (IOException ioe) {
                LOGGER.error("Error generating full graph working copy file", ioe);
            }
            legendTasks = new HashSet<>();
        }
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
            currentGraph = new BufferedWriter(new FileWriter(currentGraphPath));
            openCurrentGraphFile(currentGraph);
        } catch (IOException e) {
            LOGGER.error(ERROR_OPEN_CURRENT_GRAPH);
            return null;
        }

        return currentGraph;
    }

    /**
     * Closes header and buffer file of current graph.
     */
    public void closeCurrentGraph() {
        try {
            closeGraphFile(currentGraph);
            currentGraph.close();
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
            fullGraph.close();

            try (FileInputStream sourceFIS = new FileInputStream(completeGraphTmpPath);
                FileOutputStream destFOS = new FileOutputStream(completeGraphPath);
                FileChannel sourceChannel = sourceFIS.getChannel();
                FileChannel destChannel = destFOS.getChannel()) {

                destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
            }

            // Open tmp full graph again
            fullGraph = new BufferedWriter(new FileWriter(completeGraphTmpPath, true));

            // Close graph section
            BufferedWriter finalGraph = new BufferedWriter(new FileWriter(completeGraphPath, true));
            closeDependenceGraph(finalGraph);

            // Move legend content to final location
            openLegend(finalGraph);

            legend.close();

            try (FileInputStream sourceFIS = new FileInputStream(completeLegendTmpPath);
                FileOutputStream destFOS = new FileOutputStream(completeGraphPath, true);
                FileChannel sourceChannel = sourceFIS.getChannel();
                FileChannel destChannel = destFOS.getChannel();) {

                destChannel.position(destChannel.size());
                sourceChannel.transferTo(0, sourceChannel.size(), destChannel);
            }

            // Open tmp legend again
            legend = new BufferedWriter(new FileWriter(completeLegendTmpPath, true));
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
            fullGraph.newLine();

            if (synchId == 0) {
                fullGraph.write("Synchro" + synchId
                    + "[label=\"main\", shape=octagon, style=filled fillcolor=\"#8B0000\" fontcolor=\"#FFFFFF\"];");
            } else {
                fullGraph.write("Synchro" + synchId
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
            fullGraph.newLine();
            fullGraph.write("Synchro" + synchId
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
            fullGraph.newLine();
            fullGraph.write(task.getDotDescription());
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
     * Creates a Commutative Group in the graph.
     *
     * @param identifier identifier of the commutative group
     */
    public void addCommutativeGroup(String identifier) {
        List<Task> tasks = new LinkedList<>();
        openCommutativeGroups.put(identifier, tasks);
    }

    /**
     * Adds a task in a commutative group of to the graph.
     *
     * @param task New task.
     * @param identifier identifier of the commutative group
     */
    public void addTaskToCommutativeGroup(Task task, String identifier) {
        List<Task> tasks = openCommutativeGroups.get(identifier);
        if (tasks == null) {
            // This should never happen
            tasks = new LinkedList<>();
            openCommutativeGroups.put(identifier, tasks);
        }
        tasks.add(task);
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

    /**
     * Adds a new stream node to the graph.
     *
     * @param label Stream node label name.
     */
    public void addStreamToGraph(String label) {
        try {
            fullGraph.newLine();
            String dotDescription = label + STREAM_DOT_DESCRIPTION;
            fullGraph.write(dotDescription);
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
                addSingleElementEdgeToGraph(src, tgt, edgeType, label);
                for (Map.Entry<String, List<String>> entry : pendingGroupDependencies.entrySet()) {
                    String srctgt = entry.getKey();
                    try {
                        fullGraph.newLine();
                        Boolean first = false;
                        String f = "";
                        String l = "";
                        for (String s : pendingGroupDependencies.get(srctgt)) {
                            if (first && pendingGroupDependencies.get(srctgt).size() <= 2) {
                                l = ",d" + s;
                            } else if (first && pendingGroupDependencies.get(srctgt).size() > 2) {
                                l = ",...,d" + s;
                            } else {
                                first = true;
                                f = "d" + s;
                            }
                        }
                        if (pendingGroupDependencies.get(srctgt).size() <= 2) {
                            fullGraph.write(srctgt + " [label=\"[" + f + l + "]\",color=\"#024b30\",penwidth=2];");
                        } else {
                            fullGraph.write(
                                srctgt + " [label=\"[" + f + l + "](" + pendingGroupDependencies.get(srctgt).size()
                                    + ")" + "\",color=\"#024b30\",penwidth=2];");
                        }
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
            }
            labels.add(label);
            pendingGroupDependencies.put(src + " -> " + tgt, labels);
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
            fullGraph.newLine();
            fullGraph.write(src + " -> " + tgt + edgeProperties.toString());
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_EDGE, e);
        }
    }

    /**
     * Removes the temporary files.
     */
    public void removeTemporaryGraph() {
        if (!new File(completeGraphTmpPath).delete()) {
            LOGGER.error("Cannot remove temporary graph file");
        }
        if (!new File(completeLegendTmpPath).delete()) {
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
            fullGraph.newLine();
            fullGraph.write(src + " -> " + tgt + edgeProperties.toString());
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

    private void printCommutativeGroup(String identifier, List<Task> tasks) {
        try {
            fullGraph.newLine();

            StringBuilder msg1 = new StringBuilder();
            msg1.append("subgraph clusterCommutative").append(identifier).append(" {\n");
            fullGraph.write(msg1.toString());

            StringBuilder msg2 = new StringBuilder();
            msg2.append("shape=rect;\n");
            msg2.append("node[height=0.75];\n");
            msg2.append("color=\"#A9A9A9\";\n");
            msg2.append("rank=same;\n");
            msg2.append("label=\"CGT").append(identifier).append("\";\n");
            fullGraph.write(msg2.toString());
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
        if (tasks != null) {
            for (Task task : tasks) {
                addTask(task);
            }
        }
        try {
            fullGraph.newLine();
            fullGraph.write("}\n");
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
            fullGraph.newLine();

            StringBuilder msg1 = new StringBuilder();
            msg1.append("subgraph clusterReduce").append(identifier).append(" {\n");
            fullGraph.write(msg1.toString());

            StringBuilder msg2 = new StringBuilder();
            msg2.append("shape=rect;\n");
            msg2.append("node[height=0.75];\n");
            msg2.append("color=\"#A9A9A9\";\n");
            msg2.append("rank=same;\n");
            msg2.append("label=\"RT").append(identifier).append("\";\n");
            fullGraph.write(msg2.toString());
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
            fullGraph.newLine();
            fullGraph.write("subgraph clusterTasks" + identifier + " {\n");
            fullGraph.write(
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
            fullGraph.newLine();
            fullGraph.write("}\n");
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Starts a new subgraph.
     */
    public void createNewSubgraph() {
        try {
            fullGraph.newLine();
            fullGraph.write("subgraph{\n");
            fullGraph.newLine();
            fullGraph.write("                node[height=0.75];\n");
        } catch (IOException e) {
            LOGGER.error(ERROR_ADDING_DATA, e);
        }
    }

    /*
     * ***************************************************************************************************************
     * PRIVATE STATIC METHODS
     ****************************************************************************************************************/
    private void emptyFullGraph() throws IOException {
        openFullGraphFile(fullGraph);
        openDependenceGraph(fullGraph);
        closeDependenceGraph(fullGraph);
        openLegend(fullGraph);
        closeLegend(fullGraph);
        closeGraphFile(fullGraph);
    }

    private void emptyCurrentGraph() throws IOException {
        openCurrentGraphFile(currentGraph);
        openDependenceGraph(currentGraph);
        closeDependenceGraph(currentGraph);
        closeGraphFile(currentGraph);
    }

    private void openFullGraphFile(BufferedWriter graph) throws IOException {
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

    private void openCurrentGraphFile(BufferedWriter graph) throws IOException {
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

    private void openDependenceGraph(BufferedWriter graph) throws IOException {
        graph.write("  subgraph dependence_graph {");
        graph.newLine();
        graph.write("    ranksep=0.20;");
        graph.newLine();
        graph.write("    node[height=0.75];");
        graph.newLine();
        graph.flush();
    }

    private void openLegend(BufferedWriter graph) throws IOException {
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

    private void closeGraphFile(BufferedWriter graph) throws IOException {
        graph.write("}");
        graph.newLine();
        graph.flush();
    }

    private void closeDependenceGraph(BufferedWriter graph) throws IOException {
        graph.write("  }");
        graph.newLine();
        graph.flush();
    }

    private void closeLegend(BufferedWriter graph) throws IOException {
        graph.write("      </table>");
        graph.newLine();
        graph.write("    >]");
        graph.newLine();
        graph.write("  }");
        graph.newLine();
        graph.flush();
    }

}
