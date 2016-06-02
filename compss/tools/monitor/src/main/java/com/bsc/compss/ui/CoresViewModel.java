package com.bsc.compss.ui;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import org.apache.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.ListModelList;


public class CoresViewModel {
	private List<Core> cores;
	private static final Logger logger = Logger.getLogger("compssMonitor.TasksVM");

    @Init
    public void init() {
    	cores = new LinkedList<Core>();   	
    }
    
    public List<Core> getCores() {
    	return new ListModelList<Core>(this.cores);
    }
  
    @Command
    @NotifyChange("cores")
    public void update(List<String[]> newCoreData) {
    	logger.debug("Updating Tasks ViewModel...");
    	//Erase all current resources
    	cores.clear();
    	
    	//Import new resources
    	for (String[] dc : newCoreData) {
    		//Check color
    		int taskId = Integer.parseInt(dc[0]) + 1;	// +1 To shift according to COLORS and tracing
    		int colorId = taskId % Constants.CORE_COLOR_MAX;
    		String color = File.separator + "images" + File.separator + "colors" + File.separator + colorId + ".png";
			
			//                color, name,  params, avgExecTime, executedCount)
    		Core c = new Core (color, dc[1], dc[2], dc[3], dc[4]);
    		cores.add(c);
    	}
    	logger.debug("Tasks ViewModel updated");
    }
    
    @Command
    @NotifyChange("cores")
    public void clear() {
    	cores.clear();
    }

}
