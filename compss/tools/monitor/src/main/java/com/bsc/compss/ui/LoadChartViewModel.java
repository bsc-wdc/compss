package com.bsc.compss.ui;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.BindingParam;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.zk.ui.util.Clients;

import com.bsc.compss.commons.Loggers;

import monitoringParsers.ResourcesLogParser;


public class LoadChartViewModel {

    private String divUUID; // ZUL div UUID's
    private String chartType;

    private boolean noConnection_drawn;
    private boolean chart_drawn;
    private final int TIMEOUT = 3000; // ms - for internet connection check
    private static final Logger logger = LogManager.getLogger(Loggers.UI_VM_LOAD_CHART);


    @Init
    public void init() {
        this.divUUID = new String("");
        this.chartType = new String(Constants.TOTAL_LOAD_CHART);
        this.noConnection_drawn = false;
        this.chart_drawn = false;
    }

    @Command
    public void setDivUUID(@BindingParam("divuuid") String divuuid) {
        this.divUUID = divuuid;
    }

    public String getChartType() {
        return this.chartType;
    }

    public void setChartType(String chartType) {
        this.chartType = chartType;
        draw();
    }

    @Command
    public void update() {
        logger.debug("Updating Load Chart View Model...");
        ResourcesLogParser.parse();
        draw();
        logger.debug("Load Chart View Model updated");
    }

    @Command
    public void clear() {
        this.chartType = Constants.TOTAL_LOAD_CHART;
        ResourcesLogParser.clear();
        draw();
    }

    private void draw() {
        if (!this.divUUID.equals("")) {
            // Check internet connection
            if (testInet()) {
                logger.debug("Internet connection available. Generating google-chart");
                if (noConnection_drawn) {
                    // In some moment we didn't had connection. Erase warning
                    Clients.evalJavaScript("eraseNoConnection('" + this.divUUID + "');");
                    noConnection_drawn = false;
                }
                chart_drawn = true;
                if (this.chartType.equals(Constants.TOTAL_LOAD_CHART)) {
                    Clients.evalJavaScript("drawTotalLoadChart('" + this.divUUID + "'," + ResourcesLogParser.getTotalLoad() + ");");
                } else if (this.chartType.equals(Constants.LOAD_PER_CORE_CHART)) {
                    Clients.evalJavaScript("drawLoadPerCoreChart('" + this.divUUID + "'," + ResourcesLogParser.getLoadPerCore() + ");");
                } else if (this.chartType.equals(Constants.TOTAL_RUNNING_CHART)) {
                    Clients.evalJavaScript("drawTotalCores('" + this.divUUID + "'," + ResourcesLogParser.getTotalRunningCores() + ");");
                } else if (this.chartType.equals(Constants.RUNNING_PER_CORE_CHART)) {
                    Clients.evalJavaScript(
                            "drawCoresPerCoreChart('" + this.divUUID + "'," + ResourcesLogParser.getRunningCoresPerCore() + ");");
                } else if (this.chartType.equals(Constants.TOTAL_PENDING_CHART)) {
                    Clients.evalJavaScript("drawTotalCores('" + this.divUUID + "'," + ResourcesLogParser.getTotalPendingCores() + ");");
                } else if (this.chartType.equals(Constants.PENDING_PER_CORE_CHART)) {
                    Clients.evalJavaScript(
                            "drawCoresPerCoreChart('" + this.divUUID + "'," + ResourcesLogParser.getPendingCoresPerCore() + ");");
                } else if (this.chartType.equals(Constants.RESOURCES_STATUS_CHART)) {
                    Clients.evalJavaScript(
                            "drawTotalResourcesStatusChart('" + this.divUUID + "'," + ResourcesLogParser.getResourcesStatus() + ");");
                } else {
                    logger.warn("WARNING: Invalid chart type. Rendering empty graph");
                    Clients.evalJavaScript("drawEmpty('" + this.divUUID + "');");
                }
            } else {
                logger.info("No internet connection. Rendering warning image");
                if (chart_drawn) {
                    // In some moment we had connection. Erase chart
                    Clients.evalJavaScript("eraseChart('" + this.divUUID + "');");
                    chart_drawn = false;
                }

                // If last step we already didn't had connection, we do nothing. Otherwise we paint the
                // "unable to connect" image
                if (!noConnection_drawn) {
                    Clients.evalJavaScript("drawNoConnection('" + this.divUUID + "','" + Constants.NO_CONNECTION_IMG_PATH + "');");
                    noConnection_drawn = true;
                }
            }
        } else {
            logger.debug("DivUUID not found. Cannot render chart.");
            noConnection_drawn = false;
            chart_drawn = false;
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
                logger.warn("WARNING: Cannot close Inet connection");
            }
        }
    }

}
