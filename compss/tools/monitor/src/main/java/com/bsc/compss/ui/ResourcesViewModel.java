package com.bsc.compss.ui;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.ListModelList;


public class ResourcesViewModel {
	private List<Resource> resources;
	private static final Logger logger = Logger.getLogger("compssMonitor.ResourcesVM");

    @Init
    public void init () {
    	resources = new LinkedList<Resource>();
    }
    
    public List<Resource> getResources () {
    	return new ListModelList<Resource>(this.resources);
    }
  
    @Command
    @NotifyChange("resources")
    public void update (List<String[]> newResourcesData) {
    	logger.debug("Updating Resources ViewModel...");
    	//Erase all current resources
    	resources.clear();
    	
    	//Import new resources
    	for (String[] dr : newResourcesData) {
    		/* Each dr has the following structure (from parser)
    		 *   Position:   0   1    2    3      4     5       6     7      8
    		 *   Value:    Name CPU Core Memory Disk Provider Image Status Tasks
    		 */
    		
    		//Check memSize
    		if (dr[3] != null){
				if (dr[3].startsWith("0.")) {
					Float memsize= Float.parseFloat(dr[3]);
					dr[3] = String.valueOf(memsize*1024) + " MB";
				} else if (!dr[3].isEmpty()) {
					dr[3] = dr[3] + " GB";
				} else {
					dr[3] = "-";
				}
			} else {
				dr[3] = "-";
			}
    		
    		//Check dataSize
			if (dr[4] != null){
				if (dr[4].startsWith("0.")) {
					Float disksize= Float.parseFloat(dr[4]);
					dr[4] = String.valueOf(disksize*1024) + " MB";
				} else if (!dr[4].isEmpty()) {
					dr[4] = dr[4] + " GB";
				} else {
					dr[4] = "-";
				}
			} else {
				dr[4] = "-";
			}
    		Resource r = new Resource(dr);
    		resources.add(r);
    	}
    	logger.debug("Resources ViewModel updated");
    }
    
    @Command
    @NotifyChange("resources")
    public void clear() {
    	resources.clear();
    }

}
