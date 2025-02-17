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
import es.bsc.compss.monitoringparsers.ResourcesLogParser;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.BindingParam;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.zk.ui.util.Clients;


public class LoadChartViewModel {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_VM_LOAD_CHART);
    private static final int TIMEOUT = 3_000; // ms - for internet connection check

    private String divUuid; // ZUL div UUID's
    private String chartType;
    private boolean drawnNoConnection;
    private boolean dranChart;


    /**
     * Initializes the load chart view model.
     */
    @Init
    public void init() {
        this.divUuid = new String("");
        this.chartType = new String(Constants.TOTAL_LOAD_CHART);
        this.drawnNoConnection = false;
        this.dranChart = false;
    }

    /**
     * Sets the zul UUID for the chart plot.
     * 
     * @param divuuid New zul UUID.
     */
    @Command
    public void setDivUuid(@BindingParam("divuuid") String divuuid) {
        this.divUuid = divuuid;
    }

    public String getChartType() {
        return this.chartType;
    }

    public void setChartType(String chartType) {
        this.chartType = chartType;
        draw();
    }

    /**
     * Updates the load chart view model.
     */
    @Command
    public void update() {
        LOGGER.debug("Updating Load Chart View Model...");
        ResourcesLogParser.parse();
        draw();
        LOGGER.debug("Load Chart View Model updated");
    }

    /**
     * Clears the load chart view model.
     */
    @Command
    public void clear() {
        this.chartType = Constants.TOTAL_LOAD_CHART;
        ResourcesLogParser.clear();
        draw();
    }

    private void draw() {
        if (!this.divUuid.equals("")) {
            // Check internet connection
            if (testInet()) {
                LOGGER.debug("Internet connection available. Generating google-chart");
                if (drawnNoConnection) {
                    // In some moment we didn't had connection. Erase warning
                    Clients.evalJavaScript("eraseNoConnection('" + this.divUuid + "');");
                    drawnNoConnection = false;
                }
                dranChart = true;
                if (this.chartType.equals(Constants.TOTAL_LOAD_CHART)) {
                    Clients.evalJavaScript(
                        "drawTotalLoadChart('" + this.divUuid + "'," + ResourcesLogParser.getTotalLoad() + ");");
                } else if (this.chartType.equals(Constants.LOAD_PER_CORE_CHART)) {
                    Clients.evalJavaScript(
                        "drawLoadPerCoreChart('" + this.divUuid + "'," + ResourcesLogParser.getLoadPerCore() + ");");
                } else if (this.chartType.equals(Constants.TOTAL_RUNNING_CHART)) {
                    Clients.evalJavaScript(
                        "drawTotalCores('" + this.divUuid + "'," + ResourcesLogParser.getTotalRunningCores() + ");");
                } else if (this.chartType.equals(Constants.RUNNING_PER_CORE_CHART)) {
                    Clients.evalJavaScript("drawCoresPerCoreChart('" + this.divUuid + "',"
                        + ResourcesLogParser.getRunningCoresPerCore() + ");");
                } else if (this.chartType.equals(Constants.TOTAL_PENDING_CHART)) {
                    Clients.evalJavaScript(
                        "drawTotalCores('" + this.divUuid + "'," + ResourcesLogParser.getTotalPendingCores() + ");");
                } else if (this.chartType.equals(Constants.PENDING_PER_CORE_CHART)) {
                    Clients.evalJavaScript("drawCoresPerCoreChart('" + this.divUuid + "',"
                        + ResourcesLogParser.getPendingCoresPerCore() + ");");
                } else if (this.chartType.equals(Constants.RESOURCES_STATUS_CHART)) {
                    Clients.evalJavaScript("drawTotalResourcesStatusChart('" + this.divUuid + "',"
                        + ResourcesLogParser.getResourcesStatus() + ");");
                } else {
                    LOGGER.warn("WARNING: Invalid chart type. Rendering empty graph");
                    Clients.evalJavaScript("drawEmpty('" + this.divUuid + "');");
                }
            } else {
                LOGGER.info("No internet connection. Rendering warning image");
                if (dranChart) {
                    // In some moment we had connection. Erase chart
                    Clients.evalJavaScript("eraseChart('" + this.divUuid + "');");
                    dranChart = false;
                }

                // If last step we already didn't had connection, we do nothing. Otherwise we paint the
                // "unable to connect" image
                if (!drawnNoConnection) {
                    Clients.evalJavaScript(
                        "drawNoConnection('" + this.divUuid + "','" + Constants.NO_CONNECTION_IMG_PATH + "');");
                    drawnNoConnection = true;
                }
            }
        } else {
            LOGGER.debug("DivUUID not found. Cannot render chart.");
            drawnNoConnection = false;
            dranChart = false;
        }
    }

    private boolean testInet() {
        Socket sock = new Socket();
        InetSocketAddress addr = new InetSocketAddress("google.com", 80);
        try {
            sock.connect(addr, TIMEOUT);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            try {
                sock.close();
            } catch (IOException e) {
                LOGGER.warn("WARNING: Cannot close Inet connection");
            }
        }
    }

}
