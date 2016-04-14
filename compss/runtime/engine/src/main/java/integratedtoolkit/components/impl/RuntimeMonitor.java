package integratedtoolkit.components.impl;

import integratedtoolkit.ITConstants;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Task;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;


/**
 * The Runtime Monitor class represents the component in charge to provide user
 * with the current state of the execution.
 */
public class RuntimeMonitor implements Runnable {

    private static String CURRENT_GRAPH = "current_graph.dot";
    private static String COMPLETE_GRAPH = "complete_graph.dot";
    private static String COMPLETE_GRAPH_TMP = "complete_graph.dot.tmp";

    private static BufferedWriter graph;

    private static final boolean monitorEnabled = System.getProperty(ITConstants.IT_MONITOR) != null
            && !System.getProperty(ITConstants.IT_MONITOR).equals("0") ? true : false;

    // Graph drawing
    private static final boolean drawGraph = System.getProperty(ITConstants.IT_GRAPH) != null
            && System.getProperty(ITConstants.IT_GRAPH).equals("true") ? true : false;

    private static final String monitorDirPath;
    private static final String ERROR_MONITOR_DIR = "ERROR: Cannot create monitor directory";
    private static final Logger logger = Logger.getLogger(Loggers.ALL_COMP);

    static {
        if (monitorEnabled || drawGraph) {
            monitorDirPath = Comm.appHost.getAppLogDirPath() + "monitor" + File.separator;
            if (!new File(monitorDirPath).mkdir()) {
            	ErrorManager.error(ERROR_MONITOR_DIR);
            }
            CURRENT_GRAPH = monitorDirPath + "current_graph.dot";
            COMPLETE_GRAPH = monitorDirPath + "complete_graph.dot";
            COMPLETE_GRAPH_TMP = monitorDirPath + "complete_graph.dot.tmp";
            if (drawGraph) {
                try {
                    graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH));
                    openGraph(graph);
                    closeGraph(graph);
                    graph = null;
                } catch (IOException ioe) {
                    logger.error("Error generating graph file", ioe);
                }
                try {
                    graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_TMP));
                    openGraph(graph);
                } catch (IOException ioe) {
                    logger.error("Error generating graph file", ioe);
                }
            }

        } else {
            monitorDirPath = null;
        }

    }

    public static String getMonitorDirPath() {
        return monitorDirPath;
    }

    /**
     * Task Dispatcher associated to the monitor
     */
    private TaskDispatcher TD;
    /**
     * Task Processor associated to the monitor
     */
    private AccessProcessor TP;
    /**
     * Time inbetween two state queries
     */
    private long sleepTime;
    /**
     * Monitor keeps making queries
     */
    private boolean keepRunning;
    /**
     * The monitor thread is still alive
     */
    private boolean running;
    /**
     * Monitor Thread
     */
    private Thread monitor;

    /**
     * COMPSs installation directory
     */
    String installDir;

    /**
     * Contructs a new Runtime monitor which periodically checks the current
     * state of the execution and gives the outputs to the user.
     *
     * @param TP Task Processor associated to the monitor
     * @param TD Task Dispatcher associated to the monitor
     * @param sleepTime interval of time between state queries
     */
    public RuntimeMonitor(AccessProcessor TP, TaskDispatcher TD, long sleepTime) {
        this.TD = TD;
        this.TP = TP;
        this.keepRunning = true;
        this.sleepTime = sleepTime;
        installDir = System.getenv().get("IT_HOME");
        monitor = new Thread(this);
        monitor.setName("Monitor Thread");
        monitor.start();
    }

    /**
     * Checks periodically the status of the execution and returns the results
     * to the user
     */
    public void run() {
        running = true;
        while (keepRunning) {
            try {
                printCurrentGraphState();
                getXMLTaskState();
                ResourceManager.printLoadInfo();
                ResourceManager.printResourcesState();
                Thread.sleep(sleepTime);
            } catch (Exception e) {
            }
        }
        running = false;
    }

    /**
     * Stops the monitoring
     */
    public void shutdown() {
        
        this.keepRunning = false;
        //monitor.interrupt(); //Commented because it doesn't work as expected

        try {
            while (running) {
                Thread.sleep(sleepTime);
            }
            printCurrentGraphState();
            getXMLTaskState(); 
        } catch (Exception e) {
        }
        
        //Clears the execution files
        new File(monitorDirPath + "monitor.xml").delete();
    }

    public static void commitGraph() {
        try {
            graph.close();
            FileChannel sourceChannel = null;
            FileChannel destChannel = null;
            try {
                sourceChannel = new FileInputStream(COMPLETE_GRAPH_TMP).getChannel();
                destChannel = new FileOutputStream(COMPLETE_GRAPH).getChannel();
                destChannel.transferFrom(sourceChannel, 0, sourceChannel.size());
            } finally {
                sourceChannel.close();
                destChannel.close();
            }
            graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH, true));
            closeGraph(graph);
            graph = new BufferedWriter(new FileWriter(COMPLETE_GRAPH_TMP, true));
        } catch (Exception e) {
        }
    }

    private static void openGraph(java.io.BufferedWriter fw) throws IOException {
        fw.write("digraph {");
        fw.newLine();
        fw.write("ranksep=0.20;");
        fw.newLine();
        fw.write("node[height=0.75];");
        fw.newLine();
    }

    public static void addSynchroToGraph(int synchId) {
        try {
            graph.newLine();
            graph.write("Synchro" + synchId + "[label=\"sync\", shape=octagon, style=filled fillcolor=\"#ff0000\" fontcolor=\"#FFFFFF\"];");
        } catch (Exception e) {
            logger.error("Error adding task to graph file", e);
        }
    }

    public static void addTaskToGraph(Task task) {
        try {
            graph.newLine();
            graph.write(task.getDotDescription());
        } catch (Exception e) {
            logger.error("Error adding task to graph file", e);
        }
    }

    public static void addEdgeToGraph(String src, String tgt, String label) {
        try {
            graph.newLine();
            graph.write(src + " -> " + tgt + (label.isEmpty() ? ";" : "[ label=\"d" + label + "\" ];"));
        } catch (Exception e) {
            logger.error("Error adding edge to graph file", e);
        }
    }

    private static void closeGraph(java.io.BufferedWriter fw) throws IOException {
        fw.write("}");
        fw.close();
    }

    public static void removeTemporaryGraph() {
        new File(COMPLETE_GRAPH_TMP).delete();
    }

    /**
     * Prints in a file the current dependency graph
     */
    private void printCurrentGraphState() throws IOException {
        String currentState = TP.getCurrentGraphState();
        BufferedWriter current = new BufferedWriter(new FileWriter(CURRENT_GRAPH));
        openGraph(current);
        current.write(currentState);
        closeGraph(current);
        current = null;
    }

    /**
     * Prints in a file the current state of the Task load
     */
    private void getXMLTaskState() throws IOException {
        java.io.BufferedWriter fw = new java.io.BufferedWriter(new java.io.FileWriter(monitorDirPath + "COMPSs_state.xml"));
        StringBuilder sb = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                + "<?xml-stylesheet type=\"text/xsl\" href=\"" + installDir + "/xml/monitor/monitor.xsl\"?>\n"
                + "<COMPSsState>\n");
        sb.append(TP.getCurrentTaskState());
        sb.append(TD.getCurrentMonitoringData());
        sb.append("</COMPSsState>");
        fw.write(sb.toString());
        fw.close();
        fw = null;
    }

    public static boolean isEnabled() {
        return monitorEnabled;
    }

}
