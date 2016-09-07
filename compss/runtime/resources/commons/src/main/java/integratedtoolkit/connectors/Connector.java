package integratedtoolkit.connectors;

import integratedtoolkit.log.Loggers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


public interface Connector {

    public final Logger logger = LogManager.getLogger(Loggers.CONNECTORS_IMPL);
    public final boolean debug = logger.isDebugEnabled();


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
