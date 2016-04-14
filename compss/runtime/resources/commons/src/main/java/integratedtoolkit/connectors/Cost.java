package integratedtoolkit.connectors;

import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


public interface Cost {

    public Float getTotalCost();

    public Float currentCostPerHour();

    public Float getMachineCostPerHour(CloudMethodResourceDescription rc);
    
}
