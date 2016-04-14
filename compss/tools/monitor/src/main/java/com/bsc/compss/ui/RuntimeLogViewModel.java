package com.bsc.compss.ui;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.apache.log4j.Logger;
import org.zkoss.bind.annotation.BindingParam;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;


public class RuntimeLogViewModel {
	private String runtimeLogPath;
	private int lastParsedLine;
	private String content;
	private String filter;
	
	private static final Logger logger = Logger.getLogger("compssMonitor.ItLogVM");
	private static final String itLogNotSelected = "Application's runtime.log file not selected";
	
    @Init
    public void init() {
    	this.runtimeLogPath = new String("");
    	this.lastParsedLine = 0;
    	this.content = new String(itLogNotSelected);
    	this.filter = new String("");
    }
    
    public String getRuntimeLog() {
    	return this.content;
    }
    
    public String getFilter() {
    	return this.filter;
    }
    
    @Command
    @NotifyChange({"runtimeLog", "filter"}) 
    public void setFilter(@BindingParam("filter") String filter) {
    	this.filter = filter;
		this.lastParsedLine = 0;
		this.content = "";
    }
    
    @Command
    @NotifyChange({"runtimeLog", "filter"}) 
    public void update () {
    	if (!Properties.BASE_PATH.equals("")) {
    		//Check if applicaction has changed
    		String newPath = Properties.BASE_PATH + File.separator + Constants.RUNTIME_LOG;
    		if (!this.runtimeLogPath.equals(newPath)) {
    			//Load new application
    			this.runtimeLogPath = newPath;
    			this.lastParsedLine = 0;
    			this.content = "";
    			this.filter = "";
    		}
    		//Parse
    		logger.debug("Parsing runtime.log file...");
    		try {
    			FileReader fr = new FileReader(this.runtimeLogPath);
    			BufferedReader br = new BufferedReader(fr);
    			StringBuilder sb = new StringBuilder("");
    			String line = br.readLine();
    			int i = 0;
    			while (line != null) {
    				if (i > this.lastParsedLine) {
    					if (line.contains(filter)) {
    						sb.append(line).append("\n");
    					}
    				}
    				i = i + 1;
    				line = br.readLine();
    			}
    			this.content += sb.toString();
    			this.lastParsedLine = i - 1;
    			br.close();
    			fr.close();
    		} catch (Exception e) {
    			logger.error("Cannot parse runtime.log file: " + this.runtimeLogPath);
    		}
    	} else {
    		//Load default value
    		this.clear();
    	}
    }
    
    @Command
    @NotifyChange("runtimeLog")
    public void clear () {
    	//Load default value
    	this.runtimeLogPath = "";
    	this.lastParsedLine = 0;
    	this.content = itLogNotSelected;
    	this.filter = "";
    }
    
}
