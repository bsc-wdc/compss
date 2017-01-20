package integratedtoolkit.types;

import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.exceptions.InitNodeException;
import integratedtoolkit.exceptions.UnstartedNodeException;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Abstract representation of a COMPSs Node. Can be a master, a worker or a service
 *
 */
public abstract class COMPSsNode implements Comparable<COMPSsNode> {

    // Log and debug
    protected static final Logger logger = LogManager.getLogger(Loggers.COMM);
    public static final boolean debug = logger.isDebugEnabled();

    protected static final String DELETE_ERR = "Error deleting intermediate files";
    protected static final String URI_CREATION_ERR = "Error creating new URI";


    /**
     * Creates a new node
     * 
     */
    public COMPSsNode() {
        // Nothing to do since there are no attributes to initialize
    }

    /**
     * Returns the node name
     * 
     * @return
     */
    public abstract String getName();

    /**
     * Starts the node process
     * 
     * @throws InitNodeException
     */
    public abstract void start() throws InitNodeException;

    /**
     * Sets the internal URI of the given URIs
     * 
     * @param u
     * @throws UnstartedNodeException
     */
    public abstract void setInternalURI(MultiURI u) throws UnstartedNodeException;

    /**
     * Adds a new job to the node
     * 
     * @param taskId
     * @param taskparams
     * @param impl
     * @param res
     * @param listener
     * @return
     */
    public abstract Job<?> newJob(int taskId, TaskDescription taskparams, Implementation<?> impl, Resource res, 
            List<String> slaveWorkersNodeNames, JobListener listener);

    /**
     * Sends an specific data to the node
     * 
     * @param srcData
     * @param loc
     * @param target
     * @param tgtData
     * @param reason
     * @param listener
     */
    public abstract void sendData(LogicalData srcData, DataLocation loc, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener);

    /**
     * Retrieves an specific data from the node
     * 
     * @param srcData
     * @param source
     * @param target
     * @param tgtData
     * @param reason
     * @param listener
     */
    public abstract void obtainData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener);

    /**
     * Stops the node process
     * 
     * @param sl
     */
    public abstract void stop(ShutdownListener sl);

    /**
     * Returns the complete path of the data with name @name within the node
     * 
     * @param type
     * @param name
     * @return
     */
    public abstract SimpleURI getCompletePath(DataType type, String name);

    /**
     * Deletes the temporary node folder
     * 
     */
    public abstract void deleteTemporary();

    /**
     * Generates the tracing package in the node
     * 
     * @return
     */
    public abstract boolean generatePackage();

    /**
     * Generates the debug information in the node
     * 
     * @return
     */
    public abstract boolean generateWorkersDebugInfo();

    @Override
    public int compareTo(COMPSsNode host) {
        return getName().compareTo(host.getName());
    }
    
    @Override
    public boolean equals (Object obj) {
        if (!(obj instanceof COMPSsNode) || obj == null) {
            return false;
        }
        
        COMPSsNode host = (COMPSsNode) obj;
        return getName().equals(host.getName());
    }
    
    @Override
    public int hashCode() {
        return getName().hashCode();
    }

}
