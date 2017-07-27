package es.bsc.compss.types;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Abstract representation of a COMPSs Node. Can be a master, a worker or a
 * service
 *
 */
public abstract class COMPSsNode implements Comparable<COMPSsNode> {

    // Log and debug
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    public static final boolean DEBUG = LOGGER.isDebugEnabled();

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
     * @param slaveWorkersNodeNames
     * @param listener
     * @return
     */
    public abstract Job<?> newJob(int taskId, TaskDescription taskparams, Implementation impl, Resource res,
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
     * Shuts down the execution manager of the node
     *
     * @param sl
     */
    public abstract void shutdownExecutionManager(ExecutorShutdownListener sl);

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
    public boolean equals(Object obj) {
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
