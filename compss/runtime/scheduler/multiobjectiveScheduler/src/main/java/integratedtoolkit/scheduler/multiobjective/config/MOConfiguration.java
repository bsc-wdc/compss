package integratedtoolkit.scheduler.multiobjective.config;

public class MOConfiguration {

    public static enum OptimizationParameter {

        TIME,
        COST,
        ENERGY
    }

    //Optimization Parameter
    private static final OptimizationParameter OP_PARAMETER = OptimizationParameter.TIME;

    /*
     * ***************************************************************************************************************
     * BOUNDARIES MANAGEMENT
     * ***************************************************************************************************************
     */
    private static final long TIME_BOUNDARY = Long.MAX_VALUE;
    private static final long ENERGY_BOUNDARY = Long.MAX_VALUE;
    private static final long MONETARY_BOUNDARY = Long.MAX_VALUE;
    private static final long POWER_BOUNDARY = Long.MAX_VALUE;
    private static final long PRICE_BOUNDARY = Long.MAX_VALUE;

    public static OptimizationParameter getSchedulerOptimization() {
        return OP_PARAMETER;
    }

    public static long getTimeBoundary() {
        return TIME_BOUNDARY;
    }

    public static double getEconomicBoundary() {
        return ENERGY_BOUNDARY;
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
}
