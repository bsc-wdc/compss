package com.bsc.compss.ui;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.Filedownload;

import com.bsc.compss.commons.Loggers;
import com.bsc.compss.exceptions.EmptyCompleteGraphException;


public class CompleteGraphViewModel {
    
    private static final long COMPLETE_GRAPH_EMPTY_SIZE = 738;
    
    private static final Logger logger = LogManager.getLogger(Loggers.UI_VM_GRAPH);

    private String completeGraph;
    private String completeGraphLastUpdateTime;


    @Init
    public void init() {
        completeGraph = new String();
        completeGraphLastUpdateTime = new String();
    }

    public String getCompleteGraph() {
        return this.completeGraph;
    }

    @Command
    public void download() {
        try {
            if ((completeGraph.equals(Constants.GRAPH_NOT_FOUND_PATH)) 
                    || (completeGraph.equals(Constants.GRAPH_EXECUTION_DONE_PATH))
                    || (completeGraph.equals(Constants.UNSELECTED_GRAPH_PATH))
                    || (completeGraph.equals(Constants.EMPTY_GRAPH_PATH))) {
                Filedownload.save(completeGraph, null);
            } else {
                Filedownload.save(completeGraph.substring(0, completeGraph.lastIndexOf("?")), null);
            }
        } catch (Exception e) {
            // Cannot download file. Nothing to do
            logger.error("Cannot download complete graph");
        }
    }

    @Command
    @NotifyChange("completeGraph")
    public void update(Application monitoredApp) {
        logger.debug("Updating Complete Graph...");
        String completeMonitorLocation = monitoredApp.getPath() + Constants.MONITOR_COMPLETE_DOT_FILE;
        File completeMonitorFile = new File(completeMonitorLocation);
        if (completeMonitorFile.exists()) {
            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
            String modifiedTime = sdf.format(completeMonitorFile.lastModified());
            if (!modifiedTime.equals(completeGraphLastUpdateTime)) {
                // Update needed
                try {
                    String completeGraphSVG = File.separator + "svg" + File.separator + monitoredApp.getName() + "_"
                            + Constants.COMPLETE_GRAPH_FILE_NAME;
                    completeGraph = loadGraph(completeMonitorLocation, completeGraphSVG);
                    completeGraphLastUpdateTime = modifiedTime;
                } catch (EmptyCompleteGraphException ecge) {
                    logger.debug("Empty complete graph");
                    completeGraph = Constants.EMPTY_GRAPH_PATH;
                    completeGraphLastUpdateTime = modifiedTime;
                } catch (Exception e) {
                    completeGraph = Constants.GRAPH_NOT_FOUND_PATH;
                    completeGraphLastUpdateTime = "";
                    logger.error("Graph generation error", e);
                }
            } else {
                logger.debug("Complete Graph is already loaded");
            }
        } else {
            completeGraph = Constants.GRAPH_NOT_FOUND_PATH;
            completeGraphLastUpdateTime = "";
            logger.debug("Complete Graph file not found");
        }
    }

    @Command
    @NotifyChange("completeGraph")
    public void clear() {
        completeGraph = Constants.UNSELECTED_GRAPH_PATH;
        completeGraphLastUpdateTime = "";
    }

    private String loadGraph(String location, String target) throws EmptyCompleteGraphException, IOException, InterruptedException {
        if (logger.isDebugEnabled()) {
            logger.debug("Loading Graph...");
            logger.debug("   - Monitoring source: " + location);
            logger.debug("   - Monitoring target: " + target);
        }
        // Create SVG
        String targetFullPath = System.getProperty("catalina.base") + File.separator + "webapps" + File.separator 
                + "compss-monitor" + File.separator + target;
        String[] createSVG = { "/bin/sh", "-c", "dot -T svg " + location + " > " + targetFullPath };
        Process p1 = Runtime.getRuntime().exec(createSVG);
        p1.waitFor();
        
        //- If the complete graph is empty, throw exception to load empty graph image
        File graphFile = new File(targetFullPath);
        if (!graphFile.exists() || graphFile.length() <= COMPLETE_GRAPH_EMPTY_SIZE) {
            throw new EmptyCompleteGraphException("Empty complete graph");
        }

        // Add JSPan.js configuration
        String[] addJSScript = { "/bin/sh", "-c",
                "sed -i \"s/\\<g id\\=\\\"graph0/script xlink:href\\=\\\"SVGPan.js\\\"\\/\\>\\n\\<g id\\=\\\"viewport/\" "
                        + targetFullPath };
        Process p2 = Runtime.getRuntime().exec(addJSScript);
        p2.waitFor();

        String[] createViewBox = { "/bin/sh", "-c",
                "sed -i \"s/<svg .*/<svg xmlns\\=\\\"http:\\/\\/www.w3.org\\/2000\\/svg\\\" xmlns:xlink\\=\\\"http:\\/\\/www.w3.org\\/1999\\/xlink\\\"\\>/g\" "
                        + targetFullPath };
        Process p3 = Runtime.getRuntime().exec(createViewBox);
        p3.waitFor();

        // Load graph image
        logger.debug("Graph loaded");
        return target + "?t=" + System.currentTimeMillis();
    }

}
