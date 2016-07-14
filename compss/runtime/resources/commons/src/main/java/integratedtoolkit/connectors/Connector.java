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

    public boolean turnON(String name, ResourceCreationRequest rR);

    public void stopReached();

    public Long getNextCreationTime() throws ConnectorException;
    
    public long getTimeSlot();

    public void terminate(CloudMethodWorker worker, CloudMethodResourceDescription reduction);

    public void terminateAll();

}
