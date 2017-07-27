package es.bsc.compss.connectors;

import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;


public interface Connector {

    /**
     * Starts a resource
     * 
     * @param name
     * @param rR
     * @return
     */
    public boolean turnON(String name, ResourceCreationRequest rR);

    /**
     * Sets the stop flag
     * 
     */
    public void stopReached();

    /**
     * Returns the expected creation time for next request
     * 
     * @return
     * @throws ConnectorException
     */
    public Long getNextCreationTime() throws ConnectorException;

    /**
     * Returns the time slot size
     * 
     * @return
     */
    public long getTimeSlot();

    /**
     * Terminates an specific machine
     * 
     * @param worker
     * @param reduction
     */
    public void terminate(CloudMethodWorker worker, CloudMethodResourceDescription reduction);

    /**
     * Terminates all instances
     * 
     */
    public void terminateAll();

}
