package integratedtoolkit.connectors;

import org.apache.log4j.Logger;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;


public interface Connector {

    public final Logger logger = Logger.getLogger(Loggers.CONNECTORS_IMPL);
    public final boolean debug = logger.isDebugEnabled();

    public boolean turnON(String name, ResourceCreationRequest rR);

    public void stopReached();

    public Long getNextCreationTime() throws ConnectorException;
    
    public long getTimeSlot();

    public void terminate(CloudMethodWorker worker, CloudMethodResourceDescription reduction);

    public void terminateAll();

}
