/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.types;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Abstract representation of a COMPSs Node. Can be a master, a worker or a service.
 */
public abstract class COMPSsNode implements Comparable<COMPSsNode> {

    // Log and debug
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    public static final boolean DEBUG = LOGGER.isDebugEnabled();

    protected static final String DELETE_ERR = "Error deleting intermediate files";
    protected static final String URI_CREATION_ERR = "Error creating new URI";

    // Master name (included here to be visible from the different packages)
    private static final String MASTER_NAME_PROPERTY = System.getProperty(COMPSsConstants.MASTER_NAME);
    private static final String UNDEFINED_MASTER_NAME = "master";
    protected static final String MASTER_NAME;

    static {
        // Initializing host attributes
        String hostName = "";
        if ((MASTER_NAME_PROPERTY != null) && (!MASTER_NAME_PROPERTY.equals(""))
            && (!MASTER_NAME_PROPERTY.equals("null"))) {
            // Set the hostname from the defined property
            hostName = MASTER_NAME_PROPERTY;
        } else {
            // The MASTER_NAME_PROPERTY has not been defined, try load from machine
            try {
                InetAddress localHost = InetAddress.getLocalHost();
                hostName = localHost.getHostName();
            } catch (UnknownHostException e) {
                // Sets a default hsotName value
                ErrorManager.warn("ERROR_UNKNOWN_HOST: " + e.getLocalizedMessage());
                hostName = UNDEFINED_MASTER_NAME;
            }
        }
        MASTER_NAME = hostName;
    }

    private NodeMonitor monitor;


    /**
     * Creates a new node.
     *
     * @param monitor element monitoring changes on the node
     */
    public COMPSsNode(NodeMonitor monitor) {
        this.monitor = monitor;
    }

    /**
     * Returns the node name.
     *
     * @return The node name.
     */
    public abstract String getName();

    /**
     * Returns the master name.
     *
     * @return The master name.
     */
    public static String getMasterName() {
        return MASTER_NAME;
    }

    /**
     * Name of the adaptor used to connect with this node.
     * 
     * @return Name of the adaptor used to connect with this node.
     */
    public abstract String getAdaptor();

    /**
     * Information related to the project's configuration of the node.
     * 
     * @return Information related to the project's configuration of the node.
     */
    public abstract Object getProjectProperties();

    /**
     * Information related to the resources' configuration of the node.
     * 
     * @return Information related to the resources' configuration of the node.
     */
    public abstract Object getResourcesProperties();

    /**
     * Starts the node process.
     *
     * @throws InitNodeException Error starting node.
     */
    public abstract void start() throws InitNodeException;

    /**
     * Sets the internal URI of the given URIs.
     *
     * @param u MultiURI containing the URIs to setup with the internal node URI.
     * @throws UnstartedNodeException If the current node has not been started yet.
     */
    public abstract void setInternalURI(MultiURI u) throws UnstartedNodeException;

    /**
     * Adds a new job to the node.
     *
     * @param taskId Task id.
     * @param taskparams Task parameters.
     * @param impl Task implementation.
     * @param res Resource.
     * @param slaveWorkersNodeNames Slave node names.
     * @param listener Job listener.
     * @return New job instance.
     */
    public abstract Job<?> newJob(int taskId, TaskDescription taskparams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors);

    /**
     * Commands the worker to send a data value.
     *
     * @param srcData Source data.
     * @param loc Source data location.
     * @param target Target data.
     * @param tgtData Target data location.
     * @param reason Transferring reason.
     * @param listener Transfer listener.
     */
    public abstract void sendData(LogicalData srcData, DataLocation loc, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener);

    /**
     * Retrieves an specific data from the node.
     *
     * @param srcData Source data.
     * @param source Source data location.
     * @param target Target data location.
     * @param tgtData Target data.
     * @param reason Transferring reason.
     * @param listener Transfer listener.
     */
    public abstract void obtainData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener);

    /**
     * Orders the complete a data value retrieval already commanded.
     *
     * @param reason Transferable action type
     * @param listener Event listener
     */
    public abstract void enforceDataObtaining(Transferable reason, EventListener listener);

    /**
     * Stops the node process.
     *
     * @param sl Listener to wait until resource has been stopped.
     */
    public abstract void stop(ShutdownListener sl);

    /**
     * Returns the complete path of the data with name @{code name} within the node.
     *
     * @param type Data type.
     * @param name Data name.
     * @return Complete URI of the given data inside the current resource.
     */
    public abstract SimpleURI getCompletePath(DataType type, String name);

    /**
     * Returns the target data location.
     *
     * @param resource Resource.
     * @param type Data type.
     * @param name Data name.
     * @return Complete URI of the given data inside the current resource.
     */
    public DataLocation getTargetLocation(Resource resource, DataType type, String name) {
        LOGGER
            .debug("Getting target location of " + name + " with type " + type + " in resource " + resource.getName());
        SimpleURI workingPath = this.getCompletePath(type, name);
        DataLocation target = null;
        if (workingPath == null) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " URI is null");
        } else {
            try {
                target = DataLocation.createLocation(resource, workingPath);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + workingPath, e);
            }
        }
        return target;
    }

    /**
     * Returns the expected data target path in the node.
     *
     * @param tgtName expected data target name
     * @param param Dependency parameter
     * @return data target path
     */
    public String getOutputDataTarget(String tgtName, DependencyParameter param) {
        DataType type = param.getType();
        tgtName = param.generateDataTargetName(tgtName);
        if (type.equals(DataType.PSCO_T) || type.equals(DataType.EXTERNAL_PSCO_T)) {
            // For PSCO_T and EXTERNAL_PSCO_T we do not have to add the node path
            if (DEBUG) {
                LOGGER.debug("Generated data target for param: " + tgtName);
            }
            return tgtName;
        } else {
            if (DEBUG) {
                LOGGER.debug("Generated data target for param: " + this.getCompletePath(type, tgtName).getPath());
            }
        }
        return this.getCompletePath(param.getType(), tgtName).getPath();
    }

    /**
     * Deletes the temporary folder of a node.
     */
    public abstract void deleteTemporary();

    /**
     * Tells the worker to generate the analysis files (everything aside from logs that wants to be colected post
     * mortem) and returns the path of such files.
     *
     * @return a Set of paths to the analysis files generated by the worker.
     */
    public abstract Set<String> generateWorkerAnalysisFiles();

    /**
     * Tells the worker to generate the debug files (log files) and returns the path of such files.
     *
     * @return a Set of paths to the log files generated by the worker.
     */
    public abstract Set<String> generateWorkerDebugFiles();

    /**
     * Shuts down the execution manager of the node.
     *
     * @param sl Listener to wait until the executor has been stopped.
     */
    public abstract void shutdownExecutionManager(ExecutorShutdownListener sl);

    @Override
    public int compareTo(COMPSsNode host) {
        return getName().compareTo(host.getName());
    }

    /*
     * Removed because COMPSsNodes could have the same name.
     * 
     * @Override public boolean equals(Object obj) { if (obj != null && (obj instanceof COMPSsNode)) { COMPSsNode host =
     * (COMPSsNode) obj; return getName().equals(host.getName()); } return false; }
     * 
     * @Override public int hashCode() { return getName().hashCode(); }
     */
    /**
     * Increases the computing capabilities of the node.
     *
     * @param description New resource description.
     */
    public abstract void increaseComputingCapabilities(ResourceDescription description);

    /**
     * Decreases the computing capabilities of the node.
     *
     * @param description New resource description.
     */
    public abstract void reduceComputingCapabilities(ResourceDescription description);

    public abstract void removeObsoletes(List<MultiURI> obsoletes);

    /**
     * Sets an element that will monitor the changes on the node.
     * 
     * @param monitor element monitoring the changes on the node
     */
    public void setMonitor(NodeMonitor monitor) {
        this.monitor = monitor;
    }

    /**
     * Notifies the detection of idle resources assigned to an already-running task.
     * 
     * @param resources detected idle resources
     */
    public void idleReservedResourcesDetected(ResourceDescription resources) {
        this.monitor.idleReservedResourcesDetected(resources);
    }

    /**
     * Notifies the detection of activity on resources assigned to an already-running task previously notified to be
     * idle.
     * 
     * @param resources reactivated resources
     */
    public void reactivatedReservedResourcesDetected(ResourceDescription resources) {
        this.monitor.reactivatedReservedResourcesDetected(resources);
    }

    public abstract void verifyNodeIsRunning();

    public final void disruptedConnection() {
        this.monitor.lostNode();
    }
}
