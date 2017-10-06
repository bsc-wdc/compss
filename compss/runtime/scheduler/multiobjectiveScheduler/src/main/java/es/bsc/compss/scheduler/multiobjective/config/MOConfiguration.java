package es.bsc.compss.scheduler.multiobjective.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;

public class MOConfiguration {

    public static enum OptimizationParameter {
        TIME,
        COST,
        ENERGY
    }

    //Optimization Parameter
    private static OptimizationParameter OP_PARAMETER = OptimizationParameter.TIME;

    /*
     * ***************************************************************************************************************
     * BOUNDARIES MANAGEMENT
     * ***************************************************************************************************************
     */
    private static long TIME_BOUNDARY = Long.MAX_VALUE;
    private static long ENERGY_BOUNDARY = Long.MAX_VALUE;
    private static long MONETARY_BOUNDARY = Long.MAX_VALUE;
    private static long POWER_BOUNDARY = Long.MAX_VALUE;
    private static long PRICE_BOUNDARY = Long.MAX_VALUE;

    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    
    public static void load(){
    		String configFile = System.getProperty(COMPSsConstants.INPUT_PROFILE);
    		if (configFile!=null && !configFile.isEmpty()){
    			LOGGER.debug("[Scheduler config] Reading Multi-objective from file "+ configFile);
    			try {
					readConfiguration(configFile);
				} catch (ConfigurationException e) {
					ErrorManager.warn("Exception reading configuration. Continuing with default values.", e);
				}
    		}
    }	

    
    private static void readConfiguration(String configFile) throws ConfigurationException {
    	PropertiesConfiguration config = new PropertiesConfiguration(configFile);
    	OP_PARAMETER = OptimizationParameter.valueOf(config.getString("optimization.parameter", OptimizationParameter.TIME.toString()));
    	TIME_BOUNDARY = config.getLong("time.boundary", Long.MAX_VALUE);
    	POWER_BOUNDARY = config.getLong("power.boundary", Long.MAX_VALUE);
    	PRICE_BOUNDARY = config.getLong("price_boundary", Long.MAX_VALUE);
    	ENERGY_BOUNDARY = config.getLong("energy.boundary", Long.MAX_VALUE);
    	MONETARY_BOUNDARY = config.getLong("monetary.boundary", Long.MAX_VALUE);
    }

    public static double getMonetaryBoundary() {
        return MONETARY_BOUNDARY;
    }

    public static double getPowerBoundary() {
        return POWER_BOUNDARY;
    }

    public static double getPriceBoundary() {
        return PRICE_BOUNDARY;
    }


	public static OptimizationParameter getSchedulerOptimization() {
		return OP_PARAMETER;
	}
}
