package integratedtoolkit.components.monitor.impl;

import integratedtoolkit.ITConstants;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Task;
import integratedtoolkit.util.ErrorManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.HashSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The Runtime Monitor class represents the component in charge to provide user with the current state of the execution.
 */
public class GraphGenerator {

    // Boolean to enable GraphGeneration or not
    private static final boolean monitorEnabled = System.getProperty(ITConstants.IT_MONITOR) != null
            && !"0".equals(System.getProperty(ITConstants.IT_MONITOR)) ? true : false;
    private static final boolean drawGraph = System.getProperty(ITConstants.IT_GRAPH) != null
            && "true".equals(System.getProperty(ITConstants.IT_GRAPH)) ? true : false;
    private static final boolean graphGeneratorEnabled = monitorEnabled || drawGraph;

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

    private static final Logger logger = LogManager.getLogger(Loggers.ALL_COMP);
    private static final String ERROR_MONITOR_DIR = "ERROR: Cannot create monitor directory";
    private static final String ERROR_ADDING_DATA = "Error adding task to graph file";
    private static final String ERROR_ADDING_EDGE = "Error adding edge to graph file";
    private static final String ERROR_OPEN_CURRENT_GRAPH = "Error openning current graph file";
    private static final String ERROR_CLOSE_CURRENT_GRAPH = "Error closing current graph file";
    private static final String ERROR_COMMIT_FINAL_GRAPH = "Error commiting full graph to file";

    static {
        if (graphGeneratorEnabled) {
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
                logger.error("Error generating current graph file", ioe);
            }

            /* Final graph for drawGraph option ********************************************* */
            try {
                full_graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_FILE));
                emptyFullGraph();
                full_graph.close();
            } catch (IOException ioe) {
                logger.error("Error generating full graph file", ioe);
            }

            // Open a full graph working copy
            try {
                full_graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_TMP_FILE));
                openGraphFile(full_graph);
                openDependenceGraph(full_graph);
            } catch (IOException ioe) {
                logger.error("Error generating graph file", ioe);
            }
            try {
                legend = new BufferedWriter(new FileWriter(COMPLETE_LEGEND_TMP_FILE));
            } catch (IOException ioe) {
                logger.error("Error generating full graph working copy file", ioe);
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
     * Constructs a new Graph generator
     * 
     */
    public GraphGenerator() {
        // All attributes are initialized in the static block. Nothing to do
    }

    /*
     ****************************************************************************************************************
     * PUBLIC STATIC METHODS
     ****************************************************************************************************************/

    /**
     * Returns whether the graph generator is enabled or not
     * 
     * @return
     */
    public static boolean isEnabled() {
        return graphGeneratorEnabled;
    }

    /**
     * Returns the final monitor directory path
     * 
     * @return
     */
    public static String getMonitorDirPath() {
        return MONITOR_DIR_PATH;
    }

    /*
     ****************************************************************************************************************
     * PUBLIC METHODS
     ****************************************************************************************************************/
    /**
     * Opens and initializes the current graph buffer file
     * 
     */
    public BufferedWriter getAndOpenCurrentGraph() {
        try {
            current_graph = new BufferedWriter(new FileWriter(CURRENT_GRAPH_FILE));
            openGraphFile(current_graph);
        } catch (IOException e) {
            logger.error(ERROR_OPEN_CURRENT_GRAPH);
            return null;
        }

        return current_graph;
    }

    /**
     * Closes header and buffer file of current graph
     */
    public void closeCurrentGraph() {
        try {
            closeGraphFile(current_graph);
            current_graph.close();
        } catch (IOException e) {
            logger.error(ERROR_CLOSE_CURRENT_GRAPH);
        }
    }

    /**
     * Prints in a file the final task graph
     * 
     */
    public void commitGraph() {
        logger.debug("Commiting graph to final location");
        try {
            // Move dependence graph content to final location
            full_graph.close();
            FileChannel sourceChannel = null;
            FileChannel destChannel = null;
            try (FileInputStream sourceFIS = new FileInputStream(COMPLETE_GRAPH_TMP_FILE);
                    FileOutputStream destFOS = new FileOutputStream(COMPLETE_GRAPH_FILE)) {
                sourceChannel = sourceFIS.getChannel();
                destChannel = destFOS.getChannel();
                destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
            } catch (IOException ioe) {
                throw ioe;
            } finally {
                if (sourceChannel != null) {
                    sourceChannel.close();
                }
                if (destChannel != null) {
                    destChannel.close();
                }
            }
            full_graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_TMP_FILE, true));

            // Close graph section
            BufferedWriter finalGraph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_FILE, true));
            closeDependenceGraph(finalGraph);

            // Move legend content to final location
            openLegend(finalGraph);

            legend.close();
            sourceChannel = null;
            destChannel = null;
            try (FileInputStream sourceFIS = new FileInputStream(COMPLETE_LEGEND_TMP_FILE);
                    FileOutputStream destFOS = new FileOutputStream(COMPLETE_GRAPH_FILE, true)) {
                sourceChannel = sourceFIS.getChannel();
                destChannel = destFOS.getChannel();
                destChannel.position(destChannel.size());
                sourceChannel.transferTo(0, sourceChannel.size(), destChannel);
            } catch (IOException ioe) {
                throw ioe;
            } finally {
                if (sourceChannel != null) {
                    sourceChannel.close();
                }
                if (destChannel != null) {
                    destChannel.close();
                }
            }
            legend = new BufferedWriter(new FileWriter(COMPLETE_LEGEND_TMP_FILE, true));

            closeLegend(finalGraph);

            // Close graph
            closeGraphFile(finalGraph);
            finalGraph.close();
        } catch (IOException e) {
            logger.error(ERROR_COMMIT_FINAL_GRAPH, e);
        }
    }

    /**
     * Adds a synchro node to the graph
     * 
     * @param synchId
     */
    public void addSynchroToGraph(int synchId) {
        try {
            full_graph.newLine();
            full_graph.write(
                    "Synchro" + synchId + "[label=\"sync\", shape=octagon, style=filled fillcolor=\"#ff0000\" fontcolor=\"#FFFFFF\"];");
        } catch (IOException e) {
            logger.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Adds a barrier node to the graph
     * 
     * @param synchId
     */
    public void addBarrierToGraph(int synchId) {
        try {
            full_graph.newLine();
            full_graph.write(
                    "Synchro" + synchId + "[label=\"barrier\", shape=octagon, style=filled fillcolor=\"#ff0000\" fontcolor=\"#FFFFFF\"];");
        } catch (IOException e) {
            logger.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Adds a task node to the graph
     * 
     * @param task
     */
    public void addTaskToGraph(Task task) {
        try {
            full_graph.newLine();
            full_graph.write(task.getDotDescription());
            int taskId = task.getTaskDescription().getId();
            if (!legendTasks.contains(taskId)) {
                legendTasks.add(taskId);
                legend.write(task.getLegendDescription());
            }
        } catch (IOException e) {
            logger.error(ERROR_ADDING_DATA, e);
        }
    }

    /**
     * Adds an edge to the graph from @src to @tgt with label @label
     * 
     * @param src
     * @param tgt
     * @param label
     */
    public void addEdgeToGraph(String src, String tgt, String label) {
        try {
            full_graph.newLine();
            full_graph.write(src + " -> " + tgt + (label.isEmpty() ? ";" : "[ label=\"d" + label + "\" ];"));
        } catch (IOException e) {
            logger.error(ERROR_ADDING_EDGE, e);
        }
    }

    /**
     * Removes the temporary files
     * 
     */
    public static void removeTemporaryGraph() {
        if (!new File(COMPLETE_GRAPH_TMP_FILE).delete()) {
            logger.error("Cannot remove temporary graph file");
        }
        if (!new File(COMPLETE_LEGEND_TMP_FILE).delete()) {
            logger.error("Cannot remove temporary legend file");
        }
    }

    /*
     * ***************************************************************************************************************
     * PRIVATE STATIC METHODS
     ****************************************************************************************************************/
    private static void emptyFullGraph() throws IOException {
        openGraphFile(full_graph);
        openDependenceGraph(full_graph);
        closeDependenceGraph(full_graph);
        openLegend(full_graph);
        closeLegend(full_graph);
        closeGraphFile(full_graph);
    }

    private static void emptyCurrentGraph() throws IOException {
        openGraphFile(current_graph);
        openDependenceGraph(current_graph);
        closeDependenceGraph(current_graph);
        closeGraphFile(current_graph);
    }

    private static void openGraphFile(BufferedWriter graph) throws IOException {
        graph.write("digraph {");
        graph.newLine();
        graph.write("  rankdir=TB;");
        graph.newLine();
        graph.write("  labeljust=\"l\";");
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
