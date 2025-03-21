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
package es.bsc.compss.nio.master;

import es.bsc.comm.Connection;
import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer.Destination;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.comm.CommAdaptor;
import es.bsc.compss.data.BindingDataManager;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.log.LoggerManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOData;
import es.bsc.compss.nio.NIOMessageHandler;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTaskProfile;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.NIOUri;
import es.bsc.compss.nio.commands.CommandCancelTask;
import es.bsc.compss.nio.commands.CommandDataReceived;
import es.bsc.compss.nio.commands.CommandExecutorShutdown;
import es.bsc.compss.nio.commands.CommandExecutorShutdownACK;
import es.bsc.compss.nio.commands.CommandNIOTaskDone;
import es.bsc.compss.nio.commands.CommandNewTask;
import es.bsc.compss.nio.commands.CommandRemoveObsoletes;
import es.bsc.compss.nio.commands.CommandShutdown;
import es.bsc.compss.nio.commands.CommandShutdownACK;
import es.bsc.compss.nio.commands.tracing.CommandGenerateAnalysisFiles;
import es.bsc.compss.nio.commands.tracing.CommandGenerateAnalysisFilesDone;
import es.bsc.compss.nio.commands.workerfiles.CommandGenerateDebugFiles;
import es.bsc.compss.nio.commands.workerfiles.CommandGenerateDebugFilesDone;
import es.bsc.compss.nio.exceptions.SerializedObjectException;
import es.bsc.compss.nio.master.configuration.NIOConfiguration;
import es.bsc.compss.nio.master.types.TransferGroup;
import es.bsc.compss.nio.requests.DataRequest;
import es.bsc.compss.nio.requests.MasterDataRequest;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.operation.OperationEndState;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobHistory;
import es.bsc.compss.types.project.ProjectFile;
import es.bsc.compss.types.project.jaxb.ExternalAdaptorProperties;
import es.bsc.compss.types.project.jaxb.NIOAdaptorProperties;
import es.bsc.compss.types.project.jaxb.PropertyAdaptorType;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ResourcesPropertyAdaptorType;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;
import es.bsc.conn.types.StarterCommand;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
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
    private static final int DEFAULT_SPAWNER_PORT = 22;
    public static final int MASTER_PORT;

    // Final jobs log directory
    private static final String JOBS_DIR = LoggerManager.getJobsLogDir();

    private static final String TERM_ERR = "Error terminating";

    private static final Set<NIOWorkerNode> NODES = new HashSet<>();

    private static final ConcurrentMap<Integer, NIOJob> RUNNING_JOBS = new ConcurrentHashMap<>();

    private static final ConcurrentMap<String, NIOWorkerNode> ONGOING_WORKER_PINGS = new ConcurrentHashMap<>();

    private static final Map<Integer, TransferGroup> PENDING_TRANSFER_GROUPS = new HashMap<>();

    private static final Map<Connection, ClosingWorker> STOPPING_NODES = new HashMap<>();

    private static final Map<Connection, ClosingExecutor> STOPPING_EXECUTORS = new HashMap<>();

    private static final Map<Connection, Semaphore> PENDING_MODIFICATIONS = new HashMap<>();

    private final boolean persistentC;

    private final Semaphore tracingGeneration;
    private final Semaphore workersDebugInfo;
    private Set<String> workerLogFilesPaths;
    private Set<String> workerTracingFilesPaths;

    static {
        int masterPort;
        String masterPortProp = System.getProperty(COMPSsConstants.MASTER_PORT);
        if (masterPortProp != null && !masterPortProp.isEmpty()) {
            masterPort = Integer.valueOf(masterPortProp);
        } else {
            int random = new SecureRandom().nextInt(MAX_RANDOM_VALUE);
            masterPort = BASE_MASTER_PORT + random;
        }
        MASTER_PORT = masterPort;
    }


    /**
     * New NIOAdaptor instance.
     */
    public NIOAdaptor() {
        super(MAX_SEND, MAX_RECEIVE, MASTER_PORT);

        // Setting persistentC flag
        String persistentCStr = System.getProperty(COMPSsConstants.WORKER_PERSISTENT_C);
        if (persistentCStr == null || persistentCStr.isEmpty() || persistentCStr.equals("null")) {
            persistentCStr = COMPSsDefaults.PERSISTENT_C;
        }
        this.persistentC = Boolean.parseBoolean(persistentCStr);

        // Initialize tracing and workers debug semaphores
        this.tracingGeneration = new Semaphore(0);
        this.workersDebugInfo = new Semaphore(0);
        this.workerLogFilesPaths = null;
        this.workerTracingFilesPaths = null;

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
            && Boolean.parseBoolean(System.getProperty(COMPSsConstants.TRACING));
        this.tracingTaskDependencies =
            Boolean.parseBoolean(System.getProperty(COMPSsConstants.TRACING_TASK_DEPENDENCIES));
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
    public Configuration constructConfiguration(Map<String, Object> projectProperties,
        Map<String, Object> resourcesProperties) throws ConstructConfigurationException {
        final NIOConfiguration config = new NIOConfiguration(this.getClass().getName());
        es.bsc.compss.types.project.jaxb.NIOAdaptorProperties propsProject =
            loadProjectProperties(projectProperties, config);
        es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties propsResources =
            loadResourcesProperties(resourcesProperties, config);

        getPorts(propsProject, propsResources, config);
        getRemoteExecutionCommand(propsResources, config);

        return config;
    }

    private void getRemoteExecutionCommand(ResourcesNIOAdaptorProperties propsResources, NIOConfiguration config)
        throws ConstructConfigurationException {
        String remoteExecutionCommand = propsResources.getRemoteExecutionCommand();
        if (remoteExecutionCommand == null || remoteExecutionCommand.isEmpty()) {
            remoteExecutionCommand = NIOConfiguration.DEFAULT_REMOTE_EXECUTION_COMMAND;
        }

        if (!NIOConfiguration.getAvailableRemoteExecutionCommands().contains(remoteExecutionCommand)) {
            throw new ConstructConfigurationException("Invalid remote execution command on resources file");
        }
        config.setRemoteExecutionCommand(remoteExecutionCommand);

    }

    private void getPorts(NIOAdaptorProperties propsProject, ResourcesNIOAdaptorProperties propsResources,
        NIOConfiguration config) throws ConstructConfigurationException {
        // Get ports
        if (propsResources == null) {
            throw new ConstructConfigurationException("Resources file doesn't contain a minimum port value");
        }
        int minProject = (propsProject != null) ? propsProject.getMinPort() : -1;
        int minResources = propsResources.getMinPort();
        int maxProject = (propsProject != null) ? propsProject.getMaxPort() : -1;
        int maxResources = propsResources.getMaxPort();
        int minFinal = calculateMinPort(minProject, minResources);
        int maxFinal = calculateMaxPort(maxProject, maxResources);
        LOGGER.info("NIO Min Port: " + minFinal);
        LOGGER.info("NIO MAX Port: " + maxFinal);
        config.setMinPort(minFinal);
        config.setMaxPort(maxFinal);
        int spawnerPort = (propsResources.getSpawnerPort() != null && propsResources.getSpawnerPort() > 0)
            ? propsResources.getSpawnerPort()
            : DEFAULT_SPAWNER_PORT;
        config.setSpawnerPort(spawnerPort);

    }

    private int calculateMinPort(int minProject, int minResources) {
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
        return minFinal;
    }

    private int calculateMaxPort(int maxProject, int maxResources) {
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
        return maxFinal;

    }

    private ResourcesNIOAdaptorProperties loadResourcesProperties(Map<String, Object> resourcesProperties,
        NIOConfiguration config) throws ConstructConfigurationException {
        if (resourcesProperties == null) {
            return null;
        }
        es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties propsResources =
            (es.bsc.compss.types.resources.jaxb.ResourcesNIOAdaptorProperties) resourcesProperties.get("Ports");

        ResourcesExternalAdaptorProperties reap =
            (ResourcesExternalAdaptorProperties) resourcesProperties.get("Properties");
        if (reap != null) {
            for (ResourcesPropertyAdaptorType prop : reap.getProperty()) {
                config.addProperty(prop.getName(), prop.getValue());
            }
        }
        return propsResources;
    }

    private NIOAdaptorProperties loadProjectProperties(Map<String, Object> projectProperties, NIOConfiguration config)
        throws ConstructConfigurationException {
        if (projectProperties == null) {
            return null;
        }
        es.bsc.compss.types.project.jaxb.NIOAdaptorProperties propsProject =
            (es.bsc.compss.types.project.jaxb.NIOAdaptorProperties) projectProperties.get("Ports");
        ExternalAdaptorProperties eap = (ExternalAdaptorProperties) projectProperties.get(ProjectFile.PROPERTIES);
        if (eap != null) {
            for (PropertyAdaptorType prop : eap.getProperty()) {
                config.addProperty(prop.getName(), prop.getValue());
            }
        }
        return propsProject;
    }

    @Override
    public COMPSsWorker initWorker(Configuration config, NodeMonitor monitor) {
        NIOConfiguration nioCfg = (NIOConfiguration) config;
        LOGGER.debug("Init NIO Worker Node named " + nioCfg.getHost());

        NIOWorkerNode worker = new NIOWorkerNode(nioCfg, this, monitor);
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
        TM.shutdown(true, null);
        LOGGER.debug("NIO Adaptor stop completed!");
    }

    protected static void submitTask(NIOJob job) throws UnstartedNodeException {
        int transferGroupId = job.getTransferGroupId();
        TransferGroup group = PENDING_TRANSFER_GROUPS.get(transferGroupId);
        if (group == null) {
            group = new TransferGroup(transferGroupId);
            PENDING_TRANSFER_GROUPS.put(transferGroupId, group);
        }
        group.bindToJob(job);

        LOGGER.debug("NIO submitting new job " + job.getJobId());
        Resource res = job.getResource();

        List<MultiURI> obsoletes = res.pollObsoletes();
        List<String> obsoleteRenamings = new LinkedList<>();
        for (MultiURI u : obsoletes) {
            obsoleteRenamings.add(u.getPath());
        }
        RUNNING_JOBS.put(job.getJobId(), job);

        NIOWorkerNode worker = (NIOWorkerNode) res.getNode();
        worker.submitTask(job, obsoleteRenamings);
    }

    protected static boolean registerOngoingWorkerPing(NIOWorkerNode workerNode) {
        if (ONGOING_WORKER_PINGS.get(workerNode.getName()) != null) {
            return false;
        }
        LOGGER.debug("Registering Worker Ping: " + workerNode.getName());
        ONGOING_WORKER_PINGS.put(workerNode.getName(), workerNode);
        return true;
    }

    protected static void cancelTask(NIOJob job) throws UnstartedNodeException {
        LOGGER.debug("NIO cancelling running job " + job.getJobId());
        Resource res = job.getResource();
        NIOWorkerNode worker = (NIOWorkerNode) res.getNode();

        worker.cancelTask(job);
    }

    @Override
    public void receivedNewTask(NIONode master, NIOTask t, List<String> obsoleteFiles) {
        // Can not run any task. Do nothing
    }

    @Override
    public void cancelRunningTask(NIONode node, int jobId) {
        // Can not stop running tasks.
    }

    @Override
    public void receivedNewDataFetchOrder(NIOParam data, int transferId) {
        // Only the master commands other nodes to fetch a data value
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
        ONGOING_WORKER_PINGS.remove(nodeName);
    }

    @Override
    public void receivedNIOTaskDone(Connection c, NIOTaskResult ntr, NIOTaskProfile profile, boolean successful,
        Exception e) {
        int jobId = ntr.getJobId();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Received Task done message for Job " + jobId);
        }

        // Update running jobs
        NIOJob nj = RUNNING_JOBS.remove(jobId);
        if (nj != null) {
            nj.profileArrivalAt(profile.getArrivalTime());
            nj.fetchedAllInputDataAt(profile.getFetchedDataTime());
            nj.executionStartedAt(profile.getExecutionStartTime());
            nj.executionEndsAt(profile.getExecutionEndTime());
            nj.profileEndNotificationAt(profile.getEndTime());

            int taskId = nj.getTaskId();

            // Mark task as finished and release waiters
            JobHistory prevJobHistory = nj.getHistory();
            finishJob(nj, successful, ntr, e);
            // Retrieve files if required
            retrieveAdditionalJobFiles(c, successful, jobId, taskId, prevJobHistory);

        }

        // Close connection
        c.finishConnection();
    }

    // needed as a different funciton to be overrided by commAgentAdaptor
    protected void finishJob(NIOJob nj, boolean successful, NIOTaskResult ntr, Exception e) {
        nj.taskFinished(successful, ntr, e);
    }

    private void produceFailOnTask(NIOTask task) {
        int jobId = task.getJobId();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Received Task done message for Job " + jobId);
        }

        // Update running jobs
        NIOJob nj = RUNNING_JOBS.remove(jobId);
        if (nj != null) {

            // Update NIO Job
            // Mark task as finished and release waiters
            JobHistory prevJobHistory = nj.getHistory();
            nj.taskFinished(false, null, null);

            // Retrieve files if required
            generateFailedJobFiles(jobId, prevJobHistory, "Error sending new task command");

        }

    }

    private void generateFailedJobFiles(int jobId, JobHistory history, String message) {
        String jobOut = JOBS_DIR + "job" + jobId + "_" + history + ".out";
        String jobErr = JOBS_DIR + "job" + jobId + "_" + history + ".err";
        writeJobFile(jobOut, message);
        writeJobFile(jobErr, message);
    }

    private void writeJobFile(String taskFileName, String message) {
        File taskFile = new File(taskFileName);
        if (!taskFile.exists()) {
            try (FileOutputStream stream = new FileOutputStream(taskFile)) {
                stream.write(message.getBytes());
            } catch (IOException ioe) {
                LOGGER.error("IOException writing file: " + taskFile, ioe);
            }
        }

    }

    /**
     * Retrieves from connection {@code connection} the files containing additional job information.
     *
     * @param connection connection to request/receive information
     * @param success {@literal true}, if job ended successfully
     * @param jobId Id of the executed job
     * @param taskId Id of the executed task
     * @param history job history tag
     */
    protected void retrieveAdditionalJobFiles(Connection connection, boolean success, int jobId, int taskId,
        JobHistory history) {
        if (WORKER_DEBUG || !success) {
            String jobOut = JOBS_DIR + "job" + jobId + "_" + history + ".out";
            String jobErr = JOBS_DIR + "job" + jobId + "_" + history + ".err";
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Requesting JobOut " + jobOut + " for Task " + taskId);
                LOGGER.debug("Requesting JobErr " + jobErr + " for Task " + taskId);
            }
            connection.receiveDataFile(jobOut);
            connection.receiveDataFile(jobErr);
        }
    }

    /**
     * Registers a new copy.
     *
     * @param c New copy to register.
     */
    public void registerCopy(Copy c) {
        for (EventListener el : c.getEventListeners()) {
            Integer groupId = el.getId();
            TransferGroup copies = PENDING_TRANSFER_GROUPS.get(groupId);
            if (copies == null) {
                copies = new TransferGroup(groupId);
                PENDING_TRANSFER_GROUPS.put(groupId, copies);
            }
            copies.addCopy(c);
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
            c.end(OperationEndState.OP_FAILED); // Notify the copy has failed
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
                if (object != null) {
                    tgtData.setValue(object);
                    SimpleURI uri = new SimpleURI("object://" + tgtData.getName());
                    try {
                        actualLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
                        tgtData.addLocation(actualLocation);
                    } catch (Exception e) {
                        c.end(OperationEndState.OP_FAILED, e);
                    }
                } else {
                    tgtData.addLocation(actualLocation);
                }
            }
            c.end(OperationEndState.OP_OK);
        }

        if (Tracer.isActivated()) {
            String nameId = (new File(dataId)).getName();
            NIOTracer.emitDataTransferEvent(nameId, true);
        }
    }

    protected void updateCopiedData(LogicalData tgtData, DataLocation actualLocation) {
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
    }

    @Override
    public final void copiedData(int transferGroupId) {
        LOGGER.debug("Notifying copied Data to master");
        TransferGroup group = PENDING_TRANSFER_GROUPS.remove(transferGroupId);
        if (group == null) {
            LOGGER.debug("Group " + transferGroupId + " had no pending copies");
            return;
        }
        List<Copy> copies = group.getCopies();
        for (Copy c : copies) {
            LOGGER.debug("Treating copy " + c.getName());
            if (!c.isRegistered()) {
                LOGGER.debug("No registered copy " + c.getName());
                continue;
            }
            DataLocation actualLocation = c.getSourceData().finishedCopy(c);
            if (actualLocation != null) {
                LOGGER.debug("Actual Location " + actualLocation.getPath());
                LogicalData tgtData = c.getTargetData();
                if (tgtData != null) {
                    LOGGER.debug("targetData is not null");
                    updateCopiedData(tgtData, actualLocation);
                    LOGGER.debug("Locations for " + tgtData.getName() + " are: " + tgtData.getURIs());

                } else {
                    LOGGER.warn("No target Data defined for copy " + c.getName());
                }
            } else {
                LOGGER.debug("Actual Location is null");
            }
        }
        NIOJob job = group.getJob();
        job.fetchedAllInputData();
    }

    // Return the data that a worker should be obtaining and has not yet confirmed
    @Override
    public List<DataOperation> getPending() {
        return new LinkedList<>();
    }

    @Override
    public Object getObject(String name) throws SerializedObjectException {
        LogicalData ld = Comm.getData(name);
        Object o = ld.getValue();

        // Check if the object has been serialized meanwhile
        if (o == null) {
            for (MultiURI loc : ld.getURIs()) {
                if (!loc.getProtocol().equals(ProtocolType.OBJECT_URI) && loc.getHost().equals(Comm.getAppHost())) {
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
            if (!loc.getProtocol().equals(ProtocolType.OBJECT_URI) && loc.getHost().equals(Comm.getAppHost())) {
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
                    job.cancel();
                } catch (Exception e) {
                    LOGGER.error(TERM_ERR, e);
                }
            }
        }
    }

    @Override
    public void completeMasterURI(MultiURI u) {
        u.setInternalURI(ID, new NIOUri(masterNode, u.getPath(), u.getProtocol()));
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
    public void generateAnalysisFiles(Connection c) {
        // Master side, nothing to do
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

    // @Override
    /**
     * Waits until the tracing package is generated.
     */
    public Set<String> waitForAnalysisFiles() {
        try {
            tracingGeneration.acquire();
            return workerTracingFilesPaths;
        } catch (InterruptedException ex) {
            LOGGER.error("Error waiting for package generation");
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        }
        return null;

    }

    @Override
    public void notifyAnalysisFilesDone(Set<String> tracingFilesPaths) {
        this.workerTracingFilesPaths = tracingFilesPaths;
        tracingGeneration.release();
    }

    /**
     * Waits until the Debug Info is generated.
     */
    public Set<String> waitUntilWorkersDebugInfoGenerated() {
        try {
            workersDebugInfo.acquire();
            return workerLogFilesPaths;
        } catch (InterruptedException ex) {
            // Restore interrupted state...
            Thread.currentThread().interrupt();
        }
        return null;

    }

    @Override
    public void notifyDebugFilesDone(Set<String> logPaths) {
        this.workerLogFilesPaths = logPaths;
        workersDebugInfo.release();
    }

    @Override
    public void generateDebugFiles(Connection c) {
        c.sendCommand(new CommandGenerateDebugFilesDone(null));
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
    protected String getPossiblyRenamedFileName(File originalFile, NIOData d) {
        return Comm.getAppHost().getCompleteRemotePath(DataType.FILE_T, d.getDataMgmtId()).getPath();

    }


    private class ClosingWorker {

        private final NIOWorkerNode worker;
        private final ShutdownListener listener;


        public ClosingWorker(NIOWorkerNode w, ShutdownListener l) {
            this.worker = w;
            this.listener = l;
        }
    }

    private class ClosingExecutor {

        private final ExecutorShutdownListener listener;


        public ClosingExecutor(ExecutorShutdownListener l) {
            this.listener = l;
        }
    }


    @Override
    public void unhandeledError(Connection c) {
        LOGGER.fatal("Unhandeled error in connection " + c.hashCode() + ". Stopping the runtime...");
        ErrorManager.fatal("Unhandeled error in connection " + c.hashCode() + ".");
    }

    @Override
    public void handleCancellingTaskCommandError(Connection c, CommandCancelTask commandCancelTask) {
        if (commandCancelTask.canRetry()) {
            commandCancelTask.increaseRetries();
            resendCommand((NIONode) c.getNode(), commandCancelTask);
        } else {
            LOGGER.warn("Error sending cancel tasks after retries. Nothing else to do.");
        }
    }

    @Override
    public void handleDataReceivedCommandError(Connection c, CommandDataReceived commandDataReceived) {
        // Nothing to do at master
        LOGGER.warn("Error receiving task done command. Not handeled");
    }

    @Override
    public void handleExecutorShutdownCommandError(Connection c, CommandExecutorShutdown commandExecutorShutdown) {
        LOGGER.error("Error sending Executor Shutdown command.");
        ClosingExecutor closing = STOPPING_EXECUTORS.remove(c);
        ExecutorShutdownListener listener = closing.listener;
        listener.notifyFailure(new Exception("Error sending Executor Shutdown command."));
    }

    @Override
    public void handleExecutorShutdownCommandACKError(Connection c,
        CommandExecutorShutdownACK commandExecutorShutdownACK) {
        // Nothing to do at master
        LOGGER.warn("Error receiving executor shutdown ACK. Not handeled");
    }

    @Override
    public void handleTaskDoneCommandError(Connection c, CommandNIOTaskDone commandNIOTaskDone) {
        // Nothing to do at master
        LOGGER.warn("Error receiving task done notification. Not handeled");
    }

    @Override
    public void handleNewTaskCommandError(Connection c, CommandNewTask commandNewTask) {
        if (commandNewTask.canRetry()) {
            commandNewTask.increaseRetries();
            resendCommand((NIONode) c.getNode(), commandNewTask);
        } else {
            produceFailOnTask(commandNewTask.getTask());
        }

    }

    @Override
    public void handleShutdownCommandError(Connection c, CommandShutdown commandShutdown) {
        ClosingWorker closing = STOPPING_NODES.remove(c);
        NIOWorkerNode worker = closing.worker;
        removedNode(worker);
        ShutdownListener listener = closing.listener;
        listener.notifyFailure(new Exception("Error sending Executor Shutdown command."));
    }

    @Override
    public void handleShutdownACKCommandError(Connection c, CommandShutdownACK commandShutdownACK) {
        // Nothing to do at master
        LOGGER.warn("Error receiving shutdown ACK. Not handeled");
    }

    @Override
    public void handleTracingGenerateDoneCommandError(Connection c,
        CommandGenerateAnalysisFilesDone commandGenerateAnalysisFilesDone) {
        // Nothing to do at master
        LOGGER.warn("Error receiving tracing generate done. Not handeled");
    }

    @Override
    public void handleTracingGenerateCommandError(Connection c,
        CommandGenerateAnalysisFiles commandGenerateAnalysisFiles) {
        LOGGER.error("Error sending tracing generate command.");
        notifyAnalysisFilesDone(null);

    }

    @Override
    public void handleGenerateWorkerDebugCommandError(Connection c,
        CommandGenerateDebugFiles commandGenerateDebugFiles) {
        LOGGER.error("Error sending generate worker debug command.");
        notifyDebugFilesDone(null);
    }

    @Override
    public void handleGenerateWorkerDebugDoneCommandError(Connection c,
        CommandGenerateDebugFilesDone commandGenerateDebugFilesDone) {
        // Nothing to do at master
        LOGGER.warn("Error receiving generate worker debug done. Not handeled");

    }

    @Override
    public void receivedRemoveObsoletes(NIONode node, List<String> obsolete) {
        // Nothing to do at master
    }

    @Override
    public void handleRemoveObsoletesCommandError(Connection c, CommandRemoveObsoletes commandRemoveObsoletes) {
        if (commandRemoveObsoletes.canRetry()) {
            commandRemoveObsoletes.increaseRetries();
            resendCommand((NIONode) c.getNode(), commandRemoveObsoletes);
        } else {
            LOGGER.warn("Error sending command remove obsoletes after retries. Nothing else to do.");
        }

    }

    @Override
    public void workerPongReceived(String nodeName) {
        LOGGER.debug("Worker Pong received: " + nodeName);
        ONGOING_WORKER_PINGS.remove(nodeName);
    }

    @Override
    public void handleNodeIsDownError(String nodeName) {
        LOGGER.warn("Handling node is down due to lost connection: " + nodeName);
        NIOWorkerNode node = ONGOING_WORKER_PINGS.get(nodeName);
        node.disruptedConnection();
    }

    @Override
    public StarterCommand getStarterCommand(String workerName, int workerPort, String masterName, String workingDir,
        String installDir, String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        String envScriptFromFile, String pythonInterpreterFromFile, int totalCPU, int totalGPU, int totalFPGA,
        int limitOfTasks, String hostId) {

        return new NIOStarterCommand(workerName, workerPort, masterName, workingDir, installDir, appDir,
            classpathFromFile, pythonpathFromFile, libPathFromFile, envScriptFromFile, pythonInterpreterFromFile,
            totalCPU, totalGPU, totalFPGA, limitOfTasks, hostId);
    }

}
