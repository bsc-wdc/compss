package com.bsc.compss.ui;

import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import org.zkoss.bind.annotation.Command;
import org.zkoss.bind.annotation.Init;
import org.zkoss.bind.annotation.NotifyChange;
import org.zkoss.zul.ListModelList;


public class StatisticsViewModel {
	private List<StatisticParameter> statistics;
	private static final Logger logger = Logger.getLogger("compssMonitor.StatisticsVM");

    @Init
    public void init () {
    	statistics = new LinkedList<StatisticParameter>();
    	
    	//Add accumulated cost
    	StatisticParameter accumulatedCost = new StatisticParameter("Accumulated Cost", "0.0", "0.0");
    	statistics.add(accumulatedCost);
    }
    
    public List<StatisticParameter> getStatistics () {
    	return new ListModelList<StatisticParameter>(this.statistics);
    }
  
    @Command
    @NotifyChange("statistics")
    public void update (String[] statisticsParameters) {
    	logger.debug("Updating Statistics ViewModel...");
    	//Erase all current resources
    	for (StatisticParameter param : statistics) {
    		param.reset();
    	}
    	
    	//Import new values
    	for (int i = 0; i < statistics.size(); ++i) {
    		statistics.get(i).setValue(statisticsParameters[i]);
    	}
    	
    	logger.debug("Statistics ViewModel updated");
    }
    
    @Command
    @NotifyChange("statistics")
    public void clear() {
    	//Erase all current resources
    	for (StatisticParameter param : statistics) {
    		param.reset();
    	}
    }

}
