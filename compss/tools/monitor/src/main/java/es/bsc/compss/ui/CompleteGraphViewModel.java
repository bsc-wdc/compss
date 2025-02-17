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
package es.bsc.compss.ui;

import es.bsc.compss.commons.Loggers;
import es.bsc.compss.exceptions.EmptyCompleteGraphException;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.Filedownload;


public class CompleteGraphViewModel {

    private static final long COMPLETE_GRAPH_EMPTY_SIZE = 738;

    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_VM_GRAPH);

    private String completeGraph;
    private String completeGraphLastUpdateTime;


    @Init
    public void init() {
        this.completeGraph = new String();
        this.completeGraphLastUpdateTime = new String();
    }

    public String getCompleteGraph() {
        return this.completeGraph;
    }

    /**
     * Downloads the complete graph to the user machine.
     */
    @Command
    public void download() {
        try {
            if ((this.completeGraph.equals(Constants.GRAPH_NOT_FOUND_PATH))
                || (this.completeGraph.equals(Constants.GRAPH_EXECUTION_DONE_PATH))
                || (this.completeGraph.equals(Constants.UNSELECTED_GRAPH_PATH))
                || (this.completeGraph.equals(Constants.EMPTY_GRAPH_PATH))) {
                Filedownload.save(this.completeGraph, null);
            } else {
                Filedownload.save(this.completeGraph.substring(0, this.completeGraph.lastIndexOf("?")), null);
            }
        } catch (Exception e) {
            // Cannot download file. Nothing to do
            LOGGER.error("Cannot download complete graph");
        }
    }

    /**
     * Updates the complete graph of the currently monitored application.
     * 
     * @param monitoredApp Application being monitored.
     */
    @Command
    @NotifyChange("completeGraph")
    public void update(Application monitoredApp) {
        LOGGER.debug("Updating Complete Graph...");
        String completeMonitorLocation = monitoredApp.getPath() + Constants.MONITOR_COMPLETE_DOT_FILE;
        File completeMonitorFile = new File(completeMonitorLocation);
        if (completeMonitorFile.exists()) {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
            String modifiedTime = sdf.format(completeMonitorFile.lastModified());
            if (!modifiedTime.equals(this.completeGraphLastUpdateTime)) {
                // Update needed
                try {
                    String completeGraphSVG = File.separator + "svg" + File.separator + monitoredApp.getName() + "_"
                        + Constants.COMPLETE_GRAPH_FILE_NAME;
                    this.completeGraph = loadGraph(completeMonitorLocation, completeGraphSVG);
                    this.completeGraphLastUpdateTime = modifiedTime;
                } catch (EmptyCompleteGraphException ecge) {
                    LOGGER.debug("Empty complete graph");
                    this.completeGraph = Constants.EMPTY_GRAPH_PATH;
                    this.completeGraphLastUpdateTime = modifiedTime;
                } catch (Exception e) {
                    this.completeGraph = Constants.GRAPH_NOT_FOUND_PATH;
                    this.completeGraphLastUpdateTime = "";
                    LOGGER.error("Graph generation error", e);
                }
            } else {
                LOGGER.debug("Complete Graph is already loaded");
            }
        } else {
            this.completeGraph = Constants.GRAPH_NOT_FOUND_PATH;
            this.completeGraphLastUpdateTime = "";
            LOGGER.debug("Complete Graph file not found");
        }
    }

    @Command
    @NotifyChange("completeGraph")
    public void clear() {
        this.completeGraph = Constants.UNSELECTED_GRAPH_PATH;
        this.completeGraphLastUpdateTime = "";
    }

    private String loadGraph(String location, String target)
        throws EmptyCompleteGraphException, IOException, InterruptedException {

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Loading Graph...");
            LOGGER.debug("   - Monitoring source: " + location);
            LOGGER.debug("   - Monitoring target: " + target);
        }
        // Create SVG
        String targetFullPath = System.getProperty("catalina.base") + File.separator + "webapps" + File.separator
            + "compss-monitor" + File.separator + target;
        String[] createSVG = { "/bin/bash",
            "-c",
            "dot -T svg -Gnewrank=true " + location + " > " + targetFullPath };
        Process p1 = Runtime.getRuntime().exec(createSVG);
        p1.waitFor();

        // If the complete graph is empty, throw exception to load empty graph image
        File graphFile = new File(targetFullPath);
        if (!graphFile.exists() || graphFile.length() <= COMPLETE_GRAPH_EMPTY_SIZE) {
            throw new EmptyCompleteGraphException("Empty complete graph");
        }

        // Add JSPan.js configuration
        String[] addJSScript = { "/bin/bash",
            "-c",
            "sed -i \"s/\\<g id\\=\\\"graph0/script xlink:href\\=\\\"SVGPan.js\\\"\\/\\>\\n\\<g id\\=\\\"viewport/\" "
                + targetFullPath };
        Process p2 = Runtime.getRuntime().exec(addJSScript);
        p2.waitFor();
        // Workaround for architectures with dot tool generating main graph as graph1 not graph0
        String[] addJSScript2 = { "/bin/bash",
            "-c",
            "sed -i \"s/\\<g id\\=\\\"graph1/script xlink:href\\=\\\"SVGPan.js\\\"\\/\\>\\n\\<g id\\=\\\"viewport/\" "
                + targetFullPath };
        Process p3 = Runtime.getRuntime().exec(addJSScript2);
        p3.waitFor();

        String[] createViewBox = { "/bin/bash",
            "-c",
            "sed -i \"s/<svg .*/<svg xmlns\\=\\\"http:\\/\\/www.w3.org\\/2000\\/svg\\\" "
                + "xmlns:xlink\\=\\\"http:\\/\\/www.w3.org\\/1999\\/xlink\\\"\\>/g\" " + targetFullPath };
        Process p4 = Runtime.getRuntime().exec(createViewBox);
        p4.waitFor();

        // Load graph image
        LOGGER.debug("Graph loaded");
        return target + "?t=" + System.currentTimeMillis();
    }

}
