package com.bsc.compss.ui;

import java.io.File;
import java.text.SimpleDateFormat;

import org.apache.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.Filedownload;


public class CompleteGraphViewModel {
	private String completeGraph;
	private String completeGraphLastUpdateTime;
	private static final Logger logger = Logger.getLogger("compssMonitor.GraphVM");

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
        			|| (completeGraph.equals(Constants.EMPTY_GRAPH_PATH))) {
        			Filedownload.save(completeGraph, null);
        		} else {
        			Filedownload.save(completeGraph.substring(0, completeGraph.lastIndexOf("?")), null);
        		}
    	} catch (Exception e) {
    		//Cannot download file. Nothing to do
    		logger.error("Cannot download complete graph");
    	}
    }
    
    @Command
    @NotifyChange("completeGraph")
    public void update (Application monitoredApp) {
    	logger.debug("Updating Complete Graph...");
		String completeMonitorLocation = monitoredApp.getPath() + Constants.MONITOR_COMPLETE_DOT_FILE;
		File completeMonitorFile = new File(completeMonitorLocation);
		if (completeMonitorFile.exists()) {
			SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss");
			String modifiedTime = sdf.format(completeMonitorFile.lastModified());
			if (!modifiedTime.equals(completeGraphLastUpdateTime)) {
				//Update needed
				try {
					String completeGraphSVG = File.separator + "svg" + File.separator + monitoredApp.getName() + "_" + Constants.COMPLETE_GRAPH_FILE_NAME;
	        		completeGraph = loadGraph(completeMonitorLocation, completeGraphSVG);
	        		completeGraphLastUpdateTime = modifiedTime;
	        	} catch (Exception e) {
	        		completeGraph = Constants.GRAPH_NOT_FOUND_PATH;
	        		completeGraphLastUpdateTime = "";
	        		logger.error("Graph generation error");
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
    	completeGraph = Constants.EMPTY_GRAPH_PATH;
    	completeGraphLastUpdateTime = "";
    }
    
    private String loadGraph(String location, String target) throws Exception {
    	if (logger.isDebugEnabled()) {
    		logger.debug("Loading Graph...");
    		logger.debug("   - Monitoring source: " + location);
    		logger.debug("   - Monitoring target: " + target);
    	}
    	//Create SVG
    	String[] createSVG = {
    			"/bin/sh",
    			"-c",
    			"dot -T svg "+ location +" > " + System.getProperty("catalina.base") + File.separator + "webapps" + File.separator + "compss-monitor" + File.separator + target};
		Process p1 = Runtime.getRuntime().exec(createSVG);
		p1.waitFor();
		
		//Add JSPan.js configuration
		String[] addJSScript = {
				"/bin/sh",
				"-c",
				"sed -i \"s/\\<g id\\=\\\"graph0/script xlink:href\\=\\\"SVGPan.js\\\"\\/\\>\\n\\<g id\\=\\\"viewport/\" " + System.getProperty("catalina.base") + File.separator + "webapps" + File.separator + "compss-monitor" + File.separator + target};
		Process p2 = Runtime.getRuntime().exec(addJSScript);
		p2.waitFor();
		
		String[] createViewBox = {
				"/bin/sh",
				"-c",
				"sed -i \"s/<svg .*/<svg xmlns\\=\\\"http:\\/\\/www.w3.org\\/2000\\/svg\\\" xmlns:xlink\\=\\\"http:\\/\\/www.w3.org\\/1999\\/xlink\\\"\\>/g\" " + System.getProperty("catalina.base") + File.separator + "webapps" + File.separator + "compss-monitor" + File.separator + target};
		Process p3 = Runtime.getRuntime().exec(createViewBox);
		p3.waitFor();

    	//Load graph image
		logger.debug("Graph loaded");
		return target + "?t=" + System.currentTimeMillis();
    }

}
