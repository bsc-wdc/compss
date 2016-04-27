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

import org.apache.log4j.Logger;


/**
 * The Runtime Monitor class represents the component in charge to provide user
 * with the current state of the execution.
 */
public class GraphGenerator {
	
	// Boolean to enable GraphGeneration or not
	private static final boolean monitorEnabled = System.getProperty(ITConstants.IT_MONITOR) != null
            && !System.getProperty(ITConstants.IT_MONITOR).equals("0") ? true : false;
    private static final boolean drawGraph = System.getProperty(ITConstants.IT_GRAPH) != null
            && System.getProperty(ITConstants.IT_GRAPH).equals("true") ? true : false;
    private static final boolean graphGeneratorEnabled = (monitorEnabled || drawGraph);
    
	// Graph locations
    private static final String monitorDirPath;
    private static String CURRENT_GRAPH 		= "current_graph.dot";
    private static String COMPLETE_GRAPH 		= "complete_graph.dot";
    private static String COMPLETE_GRAPH_TMP 	= "complete_graph.dot.tmp";
    private static String COMPLETE_LEGEND_TMP 	= "complete_legend.dot.tmp";
    // Graph buffer
    private static BufferedWriter graph;
    private static BufferedWriter legend;
    private static HashSet<Integer> legendTasks;

    private static final Logger logger = Logger.getLogger(Loggers.ALL_COMP);
    private static final String ERROR_MONITOR_DIR 	= "ERROR: Cannot create monitor directory";
    private static final String ERROR_ADDING_DATA 	= "Error adding task to graph file";
    private static final String ERROR_ADDING_EDGE 	= "Error adding edge to graph file";
    private static final String ERROR_COMMIT_GRAPH 	= "Error commiting graph to file";

    static {
        if (graphGeneratorEnabled) {
            monitorDirPath = Comm.appHost.getAppLogDirPath() + "monitor" + File.separator;
            if (!new File(monitorDirPath).mkdir()) {
            	ErrorManager.error(ERROR_MONITOR_DIR);
            }
            CURRENT_GRAPH = monitorDirPath + "current_graph.dot";
            COMPLETE_GRAPH = monitorDirPath + "complete_graph.dot";
            COMPLETE_GRAPH_TMP = monitorDirPath + "complete_graph.dot.tmp";
            COMPLETE_LEGEND_TMP = monitorDirPath + "complete_legend.dot.tmp";
            
            if (drawGraph) {
            	// Generate an empty graph file
                try {
                    graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH));
                    emptyGraph(graph);
                    graph.close();
                } catch (IOException ioe) {
                    logger.error("Error generating graph file", ioe);
                }
                
                // Open a working copy
                try {
                    graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_TMP));
                    openGraphFile(graph);
                    openDependenceGraph(graph);
                } catch (IOException ioe) {
                    logger.error("Error generating graph file", ioe);
                }
                try {
                    legend = new BufferedWriter(new FileWriter(COMPLETE_LEGEND_TMP));
                } catch (IOException ioe) {
                    logger.error("Error generating graph file", ioe);
                }
                legendTasks = new HashSet<Integer>();
            }

        } else {
            monitorDirPath = null;
        }
    }
    
    
    /**
     * Constructs a new Graph generator
     * 
     */
    public GraphGenerator() {
    }
    
    /* ******************************************************************
     * PUBLIC STATIC METHODS
     * ******************************************************************/

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
        return monitorDirPath;
    }
    
    /* ******************************************************************
     * PUBLIC  METHODS
     * ******************************************************************/
    /**
     * Prints in a file the current dependency graph
     * 
     */
    public void printCurrentGraphState() {
    	logger.debug("Commiting current graph to current location");
    	commitGraph(CURRENT_GRAPH);
    }
    
    /**
     * Prints in a file the final task graph
     * 
     */
    public void commitGraph() {
    	logger.debug("Commiting graph to final location");
    	commitGraph(COMPLETE_GRAPH);
    }
    
    /**
     * Adds a synchro node to the graph
     * 
     * @param synchId
     */
    public void addSynchroToGraph(int synchId) {
        try {
            graph.newLine();
            graph.write("Synchro" + synchId + "[label=\"sync\", shape=octagon, style=filled fillcolor=\"#ff0000\" fontcolor=\"#FFFFFF\"];");
        } catch (Exception e) {
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
            graph.newLine();
            graph.write(task.getDotDescription());
            int taskId = task.getTaskParams().getId();
            if (!legendTasks.contains(taskId)) {
            	legendTasks.add(taskId);
            	legend.write(task.getLegendDescription());
            }
        } catch (Exception e) {
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
            graph.newLine();
            graph.write(src + " -> " + tgt + (label.isEmpty() ? ";" : "[ label=\"d" + label + "\" ];"));
        } catch (Exception e) {
            logger.error(ERROR_ADDING_EDGE, e);
        }
    }

    /**
     * Removes the temporary files
     * 
     */
    public static void removeTemporaryGraph() {
        new File(COMPLETE_GRAPH_TMP).delete();
        new File(COMPLETE_LEGEND_TMP).delete();
    }

    
    /* ******************************************************************
     * PRIVATE METHODS
     * ******************************************************************/
    private void commitGraph(String finalFile) {
    	logger.debug("Commiting graph to final location");
        try {
        	// Move dependence graph content to final location
            graph.close();

            FileChannel sourceChannel = null;
            FileChannel destChannel = null;
            try {
                sourceChannel = new FileInputStream(COMPLETE_GRAPH_TMP).getChannel();
                destChannel = new FileOutputStream(finalFile).getChannel();
                destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
            } finally {
                sourceChannel.close();
                destChannel.close();
            }
            graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_TMP, true));
            
            // Close graph section
            BufferedWriter finalGraph = new BufferedWriter(new FileWriter(finalFile, true));
            closeDependenceGraph(finalGraph);
            
            // Move legend content to final location
            openLegend(finalGraph);
            
            legend.close();
            try {
                sourceChannel = new FileInputStream(COMPLETE_LEGEND_TMP).getChannel();
                destChannel = new FileOutputStream(finalFile, true).getChannel();
                destChannel.position(destChannel.size());
                sourceChannel.transferTo(0, sourceChannel.size(), destChannel);
            } finally {
                sourceChannel.close();
                destChannel.close();
            }
            legend = new BufferedWriter(new FileWriter(COMPLETE_LEGEND_TMP, true));
            
            closeLegend(finalGraph);
            
            // Close graph
            closeGraphFile(finalGraph);
            finalGraph.close();    
        } catch (Exception e) {
        	logger.error(ERROR_COMMIT_GRAPH, e);
        }
    }
    
    
    /* ******************************************************************
     * PRIVATE STATIC METHODS
     * ******************************************************************/
    private static void emptyGraph(BufferedWriter fw) throws IOException {
    	openGraphFile(graph);
        openDependenceGraph(graph);
        closeDependenceGraph(graph);
        openLegend(graph);
        closeLegend(graph);
        closeGraphFile(graph);
    }
    
    private static void openGraphFile(BufferedWriter fw) throws IOException {
    	fw.write("digraph {");
        fw.newLine();
        fw.write("  rankdir=TB;");
        fw.newLine();
        fw.write("  labeljust=\"l\";");
        fw.newLine();
        fw.flush();
    }
    
    private static void openDependenceGraph(BufferedWriter fw) throws IOException {
    	fw.write("  subgraph dependence_graph {");
        fw.newLine();
        fw.write("    ranksep=0.20;");
        fw.newLine();
        fw.write("    node[height=0.75];");
        fw.newLine();
        fw.flush();
    }
    
    private static void openLegend(BufferedWriter fw) throws IOException {
	    fw.write("  subgraph legend {");
        fw.newLine();
        fw.write("    rank=sink;");
        fw.newLine();
        fw.write("    node [shape=plaintext, height=0.75];");
        fw.newLine();
        fw.write("    ranksep=0.20;");
        fw.newLine();
        fw.write("    label = \"Legend\";");
        fw.newLine();
        fw.write("    key [label=<");
        fw.newLine();
        fw.write("      <table border=\"0\" cellpadding=\"2\" cellspacing=\"0\" cellborder=\"0\">");
        fw.newLine();
        fw.flush();
    }
    
    private static void closeGraphFile(BufferedWriter fw) throws IOException {
    	fw.write("}");
        fw.newLine();
        fw.flush();
    }
    
    private static void closeDependenceGraph(BufferedWriter fw) throws IOException {
    	fw.write("  }");
        fw.newLine();
        fw.flush();
    }
    
    private static void closeLegend(BufferedWriter fw) throws IOException {
    	fw.write("      </table>");
        fw.newLine();
        fw.write("    >]");
        fw.newLine();
    	fw.write("  }");
        fw.newLine();
        fw.flush();
    }

}
