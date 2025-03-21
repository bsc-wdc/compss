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
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.log.LoggerManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Represents the thread to handle all the information needed by the COMPSs Monitor.
 */
public class RuntimeMonitor implements Runnable {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.ALL_COMP);
    private static final Logger LOGGER_API = LogManager.getLogger(Loggers.API);

    private static final String ERROR_MONITOR_DIR = "ERROR: Cannot create monitor directory";
    private static final String ERROR_GENERATING_DATA = "Error generating monitoring data";

    // Monitor properties
    private static final boolean MONITOR_ENABLED =
        System.getProperty(COMPSsConstants.MONITOR) != null && !System.getProperty(COMPSsConstants.MONITOR).equals("0");
    private static final String MONITOR_DIR_PATH = LoggerManager.getLogDir() + "monitor" + File.separator;


    /**
     * Returns whether the monitor is enabled or not.
     *
     * @return {@code true} if the monitor is enabled, {@code false} otherwise.
     */
    public static boolean isEnabled() {
        return MONITOR_ENABLED;
    }


    /**
     * Task Dispatcher associated to the monitor.
     */
    private final TaskDispatcher td;
    /**
     * Access Processor associated to the monitor.
     */
    private final AccessProcessor ap;
    /**
     * Graph Generator associated to the monitor.
     */
    private final GraphHandler gh;
    /**
     * Time between two state queries.
     */
    private final long sleepTime;
    /**
     * Monitor keeps making queries.
     */
    private boolean keepRunning;
    /**
     * The monitor thread is still alive.
     */
    private boolean running;
    /**
     * Monitor Thread.
     */
    private final Thread monitor;

    /**
     * COMPSs installation directory.
     */
    private final String installDir;


    /**
     * Constructs a new Runtime monitor. If the monitor parameter has been used, it starts a new thread which
     * periodically checks the current state of the execution and gives the outputs to the user. If only the graph
     * parameter (or none) has been used, the monitor starts but NOT as a thread.
     *
     * @param ap Task Processor associated to the monitor
     * @param td Task Dispatcher associated to the monitor
     * @param sleepTime interval of time between state queries
     */
    public RuntimeMonitor(AccessProcessor ap, TaskDispatcher td, long sleepTime) {
        this.td = td;
        this.ap = ap;
        if (GraphGenerator.isEnabled()) {
            if (!new File(MONITOR_DIR_PATH).mkdir()) {
                ErrorManager.error(ERROR_MONITOR_DIR);
            }
            this.gh = new DotGraph(MONITOR_DIR_PATH);
        } else {
            this.gh = new NoGraph();
        }

        // Configure and start internal monitor thread
        this.keepRunning = true;
        this.sleepTime = sleepTime;
        this.installDir = System.getenv().get(COMPSsConstants.COMPSS_HOME);
        if (isEnabled()) {
            this.monitor = new Thread(this);
            this.monitor.setName("Monitor Thread");
            this.monitor.start();
        } else {
            this.monitor = null;
        }
    }

    /**
     * Returns the Graph Handler for the monitoring.
     * 
     * @return graphHandler for the monitoring.
     */
    public GraphHandler getGraphHandler() {
        return this.gh;
    }

    /**
     * Checks periodically the status of the execution and returns the results to the user.
     */
    public void run() {
        this.running = true;
        while (this.keepRunning) {
            try {
                // Print XML state for Monitor
                getXMLTaskState();

                // Print current task graph
                printCurrentGraph();

                // Print load and resources information on log
                this.td.printCurrentState();
                ResourceManager.printResourcesState();

                // Sleep
                Thread.sleep(this.sleepTime);
            } catch (IOException ioe) {
                LOGGER.error(ERROR_GENERATING_DATA, ioe);
            } catch (InterruptedException ie) {
                LOGGER.error(ERROR_GENERATING_DATA, ie);
                Thread.currentThread().interrupt();
            }
        }
        this.running = false;
    }

    /**
     * Stops the monitoring.
     */
    public void shutdown() {
        if (isEnabled()) {
            LOGGER_API.debug("Stopping Monitor...");

            this.keepRunning = false;

            try {
                while (this.running) {
                    Thread.sleep(this.sleepTime);
                }
                // Print XML state for Monitor
                getXMLTaskState();

                // Print current task graph
                printCurrentGraph();
            } catch (IOException | InterruptedException ioe) {
                LOGGER.error(ERROR_GENERATING_DATA, ioe);
            }

            // Clears the execution files
            if (!new File(MONITOR_DIR_PATH + "monitor.xml").delete()) {
                LOGGER.error("Error clearing monitor.xml execution files");
            }
        }
        if (GraphGenerator.isEnabled()) {
            LOGGER_API.debug("Stopping Graph generation...");
            this.gh.removeCurrentGraph();
        }
    }

    /**
     * Prints in a file the current state of the Task load.
     */
    private void getXMLTaskState() throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>").append("\n");
        sb.append("<?xml-stylesheet type=\"text/xsl\" href=\"").append(installDir)
            .append("/xml/monitor/monitor.xsl\"?>").append("\n");
        sb.append("<COMPSsState>").append("\n");
        sb.append(this.ap.getCurrentTaskState());
        sb.append(this.td.getCurrentMonitoringData());
        sb.append("</COMPSsState>");

        BufferedWriter fw = new BufferedWriter(new FileWriter(MONITOR_DIR_PATH + "COMPSs_state.xml"));
        fw.write(sb.toString());
        fw.close();
    }

    /**
     * Prints the current graph to the specified GM file.
     */
    private void printCurrentGraph() {
        BufferedWriter graph = this.gh.getAndOpenCurrentGraph();
        if (graph != null) {
            this.td.printCurrentGraph(graph);
            this.gh.closeCurrentGraph();
        }
    }

}
