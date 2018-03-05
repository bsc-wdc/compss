package es.bsc.compss.scheduler.multiobjective.config;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;


public class MOConfiguration {

    public static enum OptimizationParameter {
        TIME, // Time param
        COST, // Cost param
        ENERGY // Energy param
    }

    public static final long DATA_TRANSFER_DELAY = 10;
    public static final double DEFAULT_IDLE_POWER = 1;
    public static final double DEFAULT_IDLE_PRICE = 0;
    
    // Optimization Parameter
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
    private static final boolean IS_DEBUG = LOGGER.isDebugEnabled();
    protected static final String LOG_PREFIX = "[MOSchedulerConfig] ";


    public static void load() {
        String configFile = System.getProperty(COMPSsConstants.SCHEDULER_CONFIG_FILE);
        if (configFile != null && !configFile.isEmpty()) {
            if (IS_DEBUG){
            	LOGGER.debug(LOG_PREFIX + "Reading Multi-objective from file " + configFile);
            }
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
        PRICE_BOUNDARY = config.getLong("price.boundary", Long.MAX_VALUE);
        ENERGY_BOUNDARY = config.getLong("energy.boundary", Long.MAX_VALUE);
        MONETARY_BOUNDARY = config.getLong("monetary.boundary", Long.MAX_VALUE);
        if (IS_DEBUG){
        	LOGGER.debug(LOG_PREFIX+" LOADED OPTIMIZATION_PARAMETER: " + OP_PARAMETER);
        	LOGGER.debug(LOG_PREFIX+" LOADED BOUNDARIES: [" + TIME_BOUNDARY+ ", "+ POWER_BOUNDARY + ", "+ PRICE_BOUNDARY +", "+ ENERGY_BOUNDARY +", " + MONETARY_BOUNDARY+"]");
        }
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

    public static double getTimeBoundary() {
        return TIME_BOUNDARY;
    }

    public static double getEnergyBoundary() {
        return ENERGY_BOUNDARY;
    }

    public static OptimizationParameter getSchedulerOptimization() {
        return OP_PARAMETER;
    }
    
    
    
}
