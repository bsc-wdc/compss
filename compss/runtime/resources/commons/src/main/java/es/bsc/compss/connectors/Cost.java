package es.bsc.compss.connectors;

import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;


public interface Cost {

    /**
     * Returns the total instance cost
     * 
     * @return
     */
    public Float getTotalCost();

    /**
     * Returns the cost per hour
     * 
     * @return
     */
    public Float currentCostPerHour();

    /**
     * Returns the cost per hour for a set of machines
     * 
     * @param rc
     * @return
     */
    public Float getMachineCostPerHour(CloudMethodResourceDescription rc);

}
