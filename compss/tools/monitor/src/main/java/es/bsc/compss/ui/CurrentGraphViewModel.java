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

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.Filedownload;


public class CurrentGraphViewModel {

    private String graph;
    private String graphLastUpdateTime;
    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_VM_GRAPH);


    @Init
    public void init() {
        this.graph = new String();
        this.graphLastUpdateTime = new String();
    }

    public String getGraph() {
        return this.graph;
    }

    /**
     * Downloads the displayed current graph.
     */
    @Command
    public void download() {
        try {
            if ((this.graph.equals(Constants.GRAPH_NOT_FOUND_PATH))
                || (this.graph.equals(Constants.GRAPH_EXECUTION_DONE_PATH))
                || (this.graph.equals(Constants.UNSELECTED_GRAPH_PATH))
                || (this.graph.equals(Constants.EMPTY_GRAPH_PATH))) {
                Filedownload.save(this.graph, null);
            } else {
                Filedownload.save(this.graph.substring(0, this.graph.lastIndexOf("?")), null);
            }
        } catch (Exception e) {
            // Cannot download file. Nothing to do
            LOGGER.error("Cannot download current graph");
        }
    }

    @Command
    @NotifyChange("graph")
    void update(Application monitoredApp) {
        LOGGER.debug("Updating Graph...");
        String monitorLocation = monitoredApp.getPath() + Constants.MONITOR_CURRENT_DOT_FILE;

        File monitorFile = new File(monitorLocation);
        if (monitorFile.exists()) {
            if (monitorFile.length() > 45) {
                SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
                String modifiedTime = sdf.format(monitorFile.lastModified());
                if (!modifiedTime.equals(this.graphLastUpdateTime)) {
                    try {
                        String graphSVG = File.separator + "svg" + File.separator + monitoredApp.getName() + "_"
                            + Constants.GRAPH_FILE_NAME;
                        this.graph = loadGraph(monitorLocation, graphSVG);
                        this.graphLastUpdateTime = modifiedTime;
                    } catch (Exception e) {
                        this.graph = Constants.GRAPH_NOT_FOUND_PATH;
                        this.graphLastUpdateTime = "";
                        LOGGER.error("Graph generation error");
                    }
                } else {
                    LOGGER.debug("Graph is already loaded");
                }
            } else {
                // The empty graph has length = 45
                this.graph = Constants.GRAPH_EXECUTION_DONE_PATH;
                this.graphLastUpdateTime = "";
            }
        } else {
            this.graph = Constants.GRAPH_NOT_FOUND_PATH;
            this.graphLastUpdateTime = "";
            LOGGER.debug("Graph file not found");
        }
    }

    @Command
    @NotifyChange("graph")
    public void clear() {
        this.graph = Constants.UNSELECTED_GRAPH_PATH;
        this.graphLastUpdateTime = "";
    }

    private String loadGraph(String location, String target) throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Loading Graph...");
            LOGGER.debug("   - Monitoring source: " + location);
            LOGGER.debug("   - Monitoring target: " + target);
        }
        // Create SVG
        String[] createSVG = { "/bin/sh",
            "-c",
            "dot -T svg " + location + " > " + System.getProperty("catalina.base") + File.separator + "webapps"
                + File.separator + "compss-monitor" + File.separator + target };
        Process p1 = Runtime.getRuntime().exec(createSVG);

        try {
            p1.waitFor();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Add JSPan.js configuration
        /*
         * String[] addJSScript = { "/bin/sh", "-c",
         * "sed -i \"s/\\<g id\\=\\\"graph0/script xlink:href\\=\\\"SVGPan.js\\\"\\/\\>\\n\\<g id\\=\\\"viewport/\" " +
         * System.getProperty("catalina.base") + File.separator + "webapps" + File.separator + "compss-monitor" +
         * File.separator + target}; Process p2 = Runtime.getRuntime().exec(addJSScript); p2.waitFor();
         */

        // String[] createViewBox = {
        // "/bin/sh",
        // "-c",
        // "sed -i \"s/<svg .*/<svg xmlns\\=\\\"http:\\/\\/www.w3.org\\/2000\\/svg\\\"
        // xmlns:xlink\\=\\\"http:\\/\\/www.w3.org\\/1999\\/xlink\\\"\\>/g\" "
        // + System.getProperty("catalina.base") + File.separator + "webapps" + File.separator + "compss-monitor" +
        // File.separator + target};
        // Process p3 = Runtime.getRuntime().exec(createViewBox);
        // p3.waitFor();

        // Load graph image
        LOGGER.debug("Graph loaded");
        return target + "?t=" + System.currentTimeMillis();
    }

}
