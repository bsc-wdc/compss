/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.nio.master;

import es.bsc.comm.Connection;
import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer.Destination;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOMessageHandler;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.NIOURI;
import es.bsc.compss.nio.commands.NIOData;
import es.bsc.compss.nio.commands.workerFiles.CommandWorkerDebugFilesDone;
import es.bsc.compss.nio.dataRequest.DataRequest;
import es.bsc.compss.nio.dataRequest.MasterDataRequest;
import es.bsc.compss.nio.exceptions.SerializedObjectException;
import es.bsc.compss.nio.master.configuration.NIOConfiguration;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.Job.JobHistory;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.BindingDataManager;
import es.bsc.compss.util.ErrorManager;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NIOAdaptor extends NIOAgent implements CommAdaptor {

    // Logging
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean WORKER_DEBUG = LogManager.getLogger(Loggers.WORKER).isDebugEnabled();

    public static final int MAX_SEND = 1_000;
    public static final int MAX_RECEIVE = 1_000;

    public static final int MAX_SEND_WORKER = 5;
    public static final int MAX_RECEIVE_WORKER = 5;

    /*
     * The master port can be: 1. Given by the MASTER_PORT property 2. A BASE_MASTER_PORT plus a random number
     */
    private static final int BASE_MASTER_PORT = 43_000;
    private static final int MAX_RANDOM_VALUE = 1_000;
    private static final int RANDOM_VALUE = new Random().nextInt(MAX_RANDOM_VALUE);
    private static final int MASTER_PORT_CALCULATED = BASE_MASTER_PORT + RANDOM_VALUE;
    private static final String MASTER_PORT_PROPERTY = System.getProperty(COMPSsConstants.MASTER_PORT);
    public static final int MASTER_PORT = (MASTER_PORT_PROPERTY != null && !MASTER_PORT_PROPERTY.isEmpty())
            ? Integer.valueOf(MASTER_PORT_PROPERTY)
            : MASTER_PORT_CALCULATED;

    // Final jobs log directory
    private static final String JOBS_DIR = System.getProperty(COMPSsConstants.APP_LOG_DIR) + "jobs" + File.separator;

    private static final String TERM_ERR = "Error terminating";
    // private static final String SER_RCV_ERR =
    // "Error serializing received object";

    private static final Set<NIOWorkerNode> NODES = new HashSet<>();

    private static final ConcurrentMap<Integer, NIOJob> RUNNING_JOBS = new ConcurrentHashMap<>();

    private static final Map<Integer, LinkedList<Copy>> GROUP_TO_COPY = new HashMap<>();

    private static final Map<Connection, ClosingWorker> STOPPING_NODES = new HashMap<>();

    private static final Map<Connection, ClosingExecutor> STOPPING_EXECUTORS = new HashMap<>();

    private static final Map<Connection, Semaphore> PENDING_MODIFICATIONS = new HashMap<>();

    private final boolean persistentC;

    private final Semaphore tracingGeneration;
    private final Semaphore workersDebugInfo;


    /**
     * New NIOAdaptor instance.
     */
    public NIOAdaptor() {
        super(MAX_SEND, MAX_RECEIVE, MASTER_PORT);

        // Setting persistentC flag
        String persistentCStr = System.getProperty(COMPSsConstants.WORKER_PERSISTENT_C);
        if (persistentCStr == null || persistentCStr.isEmpty() || persistentCStr.equals("null")) {
            persistentCStr = COMPSsConstants.DEFAULT_PERSISTENT_C;
        }
        this.persistentC = Boolean.parseBoolean(persistentCStr);

        // Initialize tracing and workers debug semaphores
        this.tracingGeneration = new Semaphore(0);
        this.workersDebugInfo = new Semaphore(0);

        // Create jobs directory
        File file = new File(JOBS_DIR);
        if (!file.exists()) {
            file.mkdir();
        }
    }

    @Override
    public void init() {
        LOGGER.info("Initializing NIO Adaptor...");
        this.masterNode = new NIONode(null, MASTER_PORT);

        // Instantiate the NIO Message Handler
        final NIOMessageHandler mhm = new NIOMessageHandler(this);

        // Init the Transfer Manager
        LOGGER.debug("  Initializing the TransferManager structures...");
        try {
            TM.init(NIO_EVENT_MANAGER_CLASS, null, mhm);
        } catch (CommException ce) {
            String errMsg = "Error initializing the TransferManager";
            ErrorManager.error(errMsg, ce);
        }

        /* Init tracing values */
        this.tracing = System.getProperty(COMPSsConstants.TRACING) != null
                && Integer.parseInt(System.getProperty(COMPSsConstants.TRACING)) > 0;
        this.tracingLevel = Integer.parseInt(System.getProperty(COMPSsConstants.TRACING));

        // Start the server
        LOGGER.debug("  Starting transfer server...");
        try {
            TM.startServer(masterNode);
        } catch (CommException ce) {
            String errMsg = "Error starting transfer server";
            ErrorManager.error(errMsg, ce);
        }
    }

    @Override
    public Configuration constructConfiguration(Object projectProperties, Object resourcesProperties)
            throws ConstructConfigurationException {

        final NIOConfiguration config = new NIOConfiguration(this.getClass().getName());

        es.bsc.compss.types.project.jaxb.NIOAdaptorProperties propsProject = (es.bsc.compss.types.project.jaxb.NIOAdaptorProperties) projectProperties;
        es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties propsResources = (es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties) resourcesProperties;

        // Get ports
        int minProject = (propsProject != null) ? propsProject.getMinPort() : -1;
        int minResources = -1;
        if (propsResources != null) {
            minResources = propsResources.getMinPort();
        } else {
            // MinPort on resources is mandatory
            throw new ConstructConfigurationException("Resources file doesn't contain a minimum port value");
        }
        int maxProject = (propsProject != null) ? propsProject.getMaxPort() : -1;
        int maxResources = (propsResources != null) ? propsResources.getMaxPort() : -1;

        // Merge port ranges
        int minFinal = -1;
        if (minProject < 0) {
            minFinal = minResources;
        } else {
            if (minProject < minResources) {
                LOGGER.warn("resources.xml MinPort is more restrictive than project.xml. Loading resources.xml values");
                minFinal = minResources;
            } else {
                minFinal = minProject;
            }
        }

        int maxFinal = -1;
        if (maxProject < 0) {
            if (maxResources < 0) {
                // No max port defined
                LOGGER.warn("MaxPort not defined in resources.xml/project.xml. Loading no limit");
            } else {
                LOGGER.warn("resources.xml MaxPort is more restrictive than project.xml. Loading resources.xml values");
                maxFinal = maxResources;
            }
        } else {
            if (maxResources < 0) {
                maxFinal = maxProject;
            } else {
                if (maxProject < maxResources) {
                    maxFinal = maxProject;
                } else {
                    LOGGER.warn(
                            "resources.xml MaxPort is more restrictive than project.xml. Loading resources.xml values");
                    maxFinal = maxResources;
                }
            }
        }

        LOGGER.info("NIO Min Port: " + minFinal);
        LOGGER.info("NIO MAX Port: " + maxFinal);
        config.setMinPort(minFinal);
        config.setMaxPort(maxFinal);

        // Add remote execution command
        String remoteExecutionCommand = propsResources.getRemoteExecutionCommand();
        if (remoteExecutionCommand == null || remoteExecutionCommand.isEmpty()) {
            remoteExecutionCommand = NIOConfiguration.DEFAULT_REMOTE_EXECUTION_COMMAND;
        }

        if (!NIOConfiguration.AVAILABLE_REMOTE_EXECUTION_COMMANDS.contains(remoteExecutionCommand)) {
            throw new ConstructConfigurationException("Invalid remote execution command on resources file");
        }
        config.setRemoteExecutionCommand(remoteExecutionCommand);

        return config;
    }

    @Override
    public NIOWorkerNode initWorker(String workerName, Configuration config) {
        LOGGER.debug("Init NIO Worker Node named " + workerName);
        NIOWorkerNode worker = new NIOWorkerNode(workerName, (NIOConfiguration) config, this);
        NODES.add(worker);
        return worker;
    }

    @Override
    public boolean isPersistentCEnabled() {
        return this.persistentC;
    }

    /**
     * Notice the removal of a worker.
     *
     * @param worker Worker to remove.
     */
    public void removedNode(NIOWorkerNode worker) {
        LOGGER.debug("Remove worker " + worker.getName());
        NODES.remove(worker);
    }

    @Override
    public void stop() {
        LOGGER.debug("NIO Adaptor stopping workers...");
        Set<NIOWorkerNode> workers = new HashSet<>();
        workers.addAll(NODES);

        Semaphore sem = new Semaphore(0);
        ShutdownListener sl = new ShutdownListener(sem);
        for (NIOWorkerNode worker : workers) {
            LOGGER.debug("- Stopping worker " + worker.getName());
            sl.addOperation();
            worker.stop(sl);
        }

        LOGGER.debug("- Waiting for workers to shutdown...");
        sl.enable();
        try {
            sem.acquire();
        } catch (Exception e) {
            LOGGER.error("ERROR: Exception raised on worker shutdown");
        }
        LOGGER.debug("- Workers stopped");

        LOGGER.debug("- Shutting down TM...");
        TM.shutdown(null);
        LOGGER.debug("NIO Adaptor stop completed!");
    }

    protected static void submitTask(NIOJob job) throws Exception {
        LOGGER.debug("NIO submitting new job " + job.getJobId());
        Resource res = job.getResource();
        NIOWorkerNode worker = (NIOWorkerNode) res.getNode();

        LogicalData[] obsoletes = res.pollObsoletes();
        List<String> obsoleteRenamings = new LinkedList<>();
        for (LogicalData ld : obsoletes) {
            obsoleteRenamings.add(worker.getWorkingDir() + File.separator + ld.getName());
        }
        RUNNING_JOBS.put(job.getJobId(), job);
        worker.submitTask(job, obsoleteRenamings);
    }

    @Override
    public void receivedNewTask(NIONode master, NIOTask t, List<String> obsoleteFiles) {
        // Can not run any task. Do nothing
    }

    @Override
    public void receivedNewDataFetchOrder(NIOParam data, int transferId) {
        //Only the master commands other nodes to fetch a data value
    }

    @Override
    public void setMaster(NIONode master) {
        // this is called on NIOWorker
        // Setting Master on Adaptor --> Nothing to be done
    }

    @Override
    public boolean isMyUuid(String uuid, String nodeName) {
        // This is used on NIOWorker to check sent UUID against worker UUID
        return false;
    }

    @Override
    public void setWorkerIsReady(String nodeName) {
        LOGGER.info("Notifying that worker is ready '" + nodeName + "'");
        WorkerStarter ws = WorkerStarter.getWorkerStarter(nodeName);
        if (ws != null) {
            ws.setWorkerIsReady();
        } else {
            LOGGER.warn("WARN: worker starter for worker " + nodeName + " is null.");
        }
    }

    @Override
    public void receivedNIOTaskDone(Connection c, NIOTaskResult tr, boolean successful) {
        int jobId = tr.getJobId();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Received Task done message for Task " + jobId);
        }

        // Update running jobs
        NIOJob nj = RUNNING_JOBS.remove(jobId);
        if (nj != null) {
            int taskId = nj.getTaskId();

            // Update information
            List<DataType> taskResultTypes = tr.getParamTypes();
            for (int i = 0; i < taskResultTypes.size(); ++i) {
                DataType newType = taskResultTypes.get(i);
                switch (newType) {
                    case PSCO_T:
                    case EXTERNAL_PSCO_T:
                        String pscoId = (String) tr.getParamValue(i);
                        DependencyParameter dp = (DependencyParameter) nj.getTaskParams().getParameters()[i];
                        updateParameter(newType, pscoId, dp);
                        break;
                    default:
                        // We only update information about PSCOs or EXTERNAL_PSCO
                        break;
                }
            }

            // Update NIO Job
            // Mark task as finished and release waiters
            JobHistory prevJobHistory = nj.getHistory();
            nj.taskFinished(successful);
            // JobHistory newJobHistory = nj.getHistory();

            // Retrieve files if required
            if (WORKER_DEBUG || !successful) {
                String jobOut = JOBS_DIR + "job" + jobId + "_" + prevJobHistory + ".out";
                String jobErr = JOBS_DIR + "job" + jobId + "_" + prevJobHistory + ".err";
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Requesting JobOut " + jobOut + " for Task " + taskId);
                    LOGGER.debug("Requesting JobErr " + jobErr + " for Task " + taskId);
                }
                c.receiveDataFile(jobOut);
                c.receiveDataFile(jobErr);
            }

        }

        // Close connection
        c.finishConnection();
    }

    private void updateParameter(DataType newType, String pscoId, DependencyParameter dp) {
        DataType previousType = dp.getType();
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Updating parameter " + dp.getDataTarget() + " from type " + previousType + " to type "
                    + newType + " with id " + pscoId);
        }

        switch (previousType) {
            case PSCO_T:
            case EXTERNAL_PSCO_T:
                if (previousType.equals(newType)) {
                    // The parameter was already a PSCO, we only update the information just in case
                    dp.setDataTarget(pscoId);
                } else {
                    // The parameter types do not match, log exception
                    LOGGER.warn("WARN: Cannot update parameter " + dp.getDataTarget()
                            + " because types are not compatible");
                }
                break;
            default:
                // The parameter was an OBJECT or a FILE, we change its type and value and register its new location
                registerUpdatedParameter(newType, pscoId, dp);
                break;
        }
    }

    private void registerUpdatedParameter(DataType newType, String pscoId, DependencyParameter dp) {
        // The parameter was an OBJECT or a FILE, we change its type and value and register its new location
        String renaming = dp.getDataTarget();

        // Update COMM information
        switch (newType) {
            case PSCO_T:
                Comm.registerPSCO(renaming, pscoId);
                break;
            case EXTERNAL_PSCO_T:
                if (renaming.contains("/")) {
                    renaming = renaming.substring(renaming.lastIndexOf('/') + 1);
                }
                Comm.registerExternalPSCO(renaming, pscoId);
                break;
            default:
                LOGGER.warn("WARN: Invalid new type " + newType + " for parameter " + renaming);
                break;
        }

        // Update Task information
        dp.setType(newType);
        dp.setDataTarget(pscoId);
    }

    /**
     * Registers a new copy.
     * 
     * @param c New copy to register.
     */
    public void registerCopy(Copy c) {
        for (EventListener el : c.getEventListeners()) {
            Integer groupId = el.getId();
            LinkedList<Copy> copies = GROUP_TO_COPY.get(groupId);
            if (copies == null) {
                copies = new LinkedList<Copy>();
                GROUP_TO_COPY.put(groupId, copies);
            }
            copies.add(c);
        }
    }

    @Override
    protected void handleDataToSendNotAvailable(Connection c, NIOData d) {
        // Finish the connection asap. The comm library will notify this error
        // upwards as a ClosedChannelError.
        c.finishConnection();
    }

    @Override
    public void handleRequestedDataNotAvailableError(List<DataRequest> failedRequests, String dataId) {
        for (DataRequest dr : failedRequests) {
            MasterDataRequest mdr = (MasterDataRequest) dr;
            Copy c = (Copy) mdr.getOperation();
            c.getSourceData().finishedCopy(c);
            c.end(DataOperation.OpEndState.OP_FAILED); // Notify the copy has failed
        }
    }

    @Override
    public void receivedValue(Destination type, String dataId, Object object, List<DataRequest> achievedRequests) {
        for (DataRequest dr : achievedRequests) {
            MasterDataRequest mdr = (MasterDataRequest) dr;
            Copy c = (Copy) mdr.getOperation();
            DataLocation actualLocation = c.getSourceData().finishedCopy(c);
            LogicalData tgtData = c.getTargetData();
            if (tgtData != null) {
                tgtData.addLocation(actualLocation);
                if (object != null) {
                    tgtData.setValue(object);
                }
            }
            c.end(DataOperation.OpEndState.OP_OK);
        }

        if (NIOTracer.extraeEnabled()) {
            NIOTracer.emitDataTransferEvent(NIOTracer.TRANSFER_END);
        }
    }

    @Override
    public void copiedData(int transferGroupId) {
        LOGGER.debug("Notifying copied Data to master");
        LinkedList<Copy> copies = GROUP_TO_COPY.remove(transferGroupId);
        if (copies == null) {
            LOGGER.debug("No copies to process");
            return;
        }
        for (Copy c : copies) {
            LOGGER.debug("Treating copy " + c.getName());
            if (!c.isRegistered()) {
                LOGGER.debug("No registered copy " + c.getName());
                continue;
            }
            DataLocation actualLocation = c.getSourceData().finishedCopy(c);
            if (actualLocation != null) {
                LOGGER.debug("Actual Location " + actualLocation.getPath());
            } else {
                LOGGER.debug("Actual Location is null");
            }
            LogicalData tgtData = c.getTargetData();
            if (tgtData != null) {
                LOGGER.debug("targetData is not null");
                switch (actualLocation.getType()) {
                    case PERSISTENT:
                        LOGGER.debug("Persistent location no need to update location for " + tgtData.getName());
                        break;
                    case BINDING:
                    case PRIVATE:
                        LOGGER.debug("Adding location:" + actualLocation.getPath() + " to " + tgtData.getName());
                        tgtData.addLocation(actualLocation);
                        break;
                    case SHARED:
                        LOGGER.debug("Shared location no need to update location for " + tgtData.getName());
                        break;
                }
                LOGGER.debug("Locations for " + tgtData.getName() + " are: " + tgtData.getURIs());

            } else {
                LOGGER.warn("No target Data defined for copy " + c.getName());
            }
        }
    }

    // Return the data that a worker should be obtaining and has not yet confirmed
    @Override
    public List<DataOperation> getPending() {
        return new LinkedList<DataOperation>();
    }

    @Override
    public Object getObject(String name) throws SerializedObjectException {
        LogicalData ld = Comm.getData(name);
        Object o = ld.getValue();

        // Check if the object has been serialized meanwhile
        if (o == null) {
            for (MultiURI loc : ld.getURIs()) {
                if (!loc.getProtocol().equals(Protocol.OBJECT_URI) && loc.getHost().equals(Comm.getAppHost())) {
                    // The object is null because it has been serialized by the master, raise exception
                    throw new SerializedObjectException(name);
                }
            }
        }

        // If we arrive to this return means:
        // 1- The object has been found or
        // 2- The object is really null (no exception thrown)
        return o;
    }

    @Override
    public String getObjectAsFile(String name) {
        LogicalData ld = Comm.getData(name);

        // Get a Master location
        for (MultiURI loc : ld.getURIs()) {
            if (!loc.getProtocol().equals(Protocol.OBJECT_URI) && loc.getHost().equals(Comm.getAppHost())) {
                return loc.getPath();
            }
        }

        // No location found in master
        return null;
    }

    @Override
    public String getWorkingDir() {
        return "";
    }

    @Override
    public void stopSubmittedJobs() {
        synchronized (RUNNING_JOBS) {
            for (Job<?> job : RUNNING_JOBS.values()) {
                try {
                    job.stop();
                } catch (Exception e) {
                    LOGGER.error(TERM_ERR, e);
                }
            }
        }
    }

    @Override
    public void completeMasterURI(MultiURI u) {
        u.setInternalURI(ID, new NIOURI(masterNode, u.getPath(), u.getProtocol()));
    }

    /**
     * Requests a new data.
     * 
     * @param c Associated copy.
     * @param paramType Data type.
     * @param d NIOData to request.
     * @param path Target path.
     */
    public void requestData(Copy c, DataType paramType, NIOData d, String path) {
        DataRequest dr = new MasterDataRequest(c, paramType, d, path);
        addTransferRequest(dr);
        requestTransfers();

    }

    /**
     * Marks the worker to shutdown.
     * 
     * @param worker Worker node.
     * @param c Connection.
     * @param listener Listener.
     */
    public void shuttingDown(NIOWorkerNode worker, Connection c, ShutdownListener listener) {
        STOPPING_NODES.put(c, new ClosingWorker(worker, listener));
    }

    /**
     * Marks the worker to shutdown only the execution manager.
     * 
     * @param worker Worker node.
     * @param c Connection.
     * @param listener Listener.
     */
    public void shuttingDownEM(NIOWorkerNode worker, Connection c, ExecutorShutdownListener listener) {
        STOPPING_EXECUTORS.put(c, new ClosingExecutor(listener));
    }

    @Override
    public void shutdownNotification(Connection c) {
        ClosingWorker closing = STOPPING_NODES.remove(c);
        NIOWorkerNode worker = closing.worker;
        removedNode(worker);
        ShutdownListener listener = closing.listener;
        listener.notifyEnd();
    }

    @Override
    public void shutdown(Connection closingConnection) {
        // Master side, nothing to do
    }

    @Override
    public void shutdownExecutionManager(Connection closingConnection) {
        // Master side, nothing to do

    }

    @Override
    public void shutdownExecutionManagerNotification(Connection c) {
        ClosingExecutor closing = STOPPING_EXECUTORS.remove(c);
        ExecutorShutdownListener listener = closing.listener;
        listener.notifyEnd();
    }

    @Override
    public void waitUntilTracingPackageGenerated() {
        try {
            tracingGeneration.acquire();
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for package generation");
        }

    }

    @Override
    public void notifyTracingPackageGeneration() {
        tracingGeneration.release();
    }

    @Override
    public void waitUntilWorkersDebugInfoGenerated() {
        try {
            workersDebugInfo.acquire();
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for package generation");
        }

    }

    @Override
    public void notifyWorkersDebugInfoGeneration() {
        workersDebugInfo.release();
    }

    @Override
    public void generateWorkersDebugInfo(Connection c) {
        c.sendCommand(new CommandWorkerDebugFilesDone());
        c.finishConnection();
    }

    @Override
    public void increaseResources(MethodResourceDescription description) {
        // Should never receive this message. Ignore
    }

    @Override
    public void reduceResources(MethodResourceDescription description) {
        // Should never receive this message. Ignore
    }

    @Override
    public void performedResourceUpdate(Connection c) {
        c.finishConnection();
        Semaphore sem = PENDING_MODIFICATIONS.get(c);
        if (sem != null) {
            sem.release();
        }
    }

    /**
     * Registers a pending modification of the current worker node.
     * 
     * @param c Connection.
     * @param sem Semaphore to wait until the modification is performed.
     */
    public void registerPendingResourceUpdateConfirmation(Connection c, Semaphore sem) {
        PENDING_MODIFICATIONS.put(c, sem);
    }


    private class ClosingWorker {

        private final NIOWorkerNode worker;
        private final ShutdownListener listener;


        public ClosingWorker(NIOWorkerNode w, ShutdownListener l) {
            worker = w;
            listener = l;
        }
    }

    private class ClosingExecutor {

        private final ExecutorShutdownListener listener;


        public ClosingExecutor(ExecutorShutdownListener l) {
            listener = l;
        }
    }


    @Override
    public void receivedBindingObjectAsFile(String filename, String target) {
        // Load from file
        if (filename.contains("#")) {
            // Filename contains binding object
            filename = BindingObject.generate(filename).getId();
        }
        if (target.contains("#")) {
            BindingObject bo = BindingObject.generate(target);
            BindingDataManager.loadFromFile(bo.getName(), filename, bo.getType(), bo.getElements());
        } else {
            ErrorManager.error("Incorrect target format for binding object.(" + target + ")");
        }

    }

    @Override
    protected boolean isMaster() {
        return true;
    }

}
