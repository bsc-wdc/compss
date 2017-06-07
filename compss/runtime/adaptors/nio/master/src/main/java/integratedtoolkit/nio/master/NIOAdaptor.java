package integratedtoolkit.nio.master;

import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer.Destination;

import integratedtoolkit.ITConstants;

import integratedtoolkit.comm.Comm;
import integratedtoolkit.comm.CommAdaptor;
import integratedtoolkit.exceptions.ConstructConfigurationException;

import integratedtoolkit.log.Loggers;

import integratedtoolkit.types.resources.ExecutorShutdownListener;
import integratedtoolkit.util.ErrorManager;

import integratedtoolkit.nio.NIOAgent;
import integratedtoolkit.nio.NIOMessageHandler;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTaskResult;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.nio.NIOURI;
import integratedtoolkit.nio.commands.Data;
import integratedtoolkit.nio.commands.workerFiles.CommandWorkerDebugFilesDone;
import integratedtoolkit.nio.dataRequest.DataRequest;
import integratedtoolkit.nio.dataRequest.MasterDataRequest;
import integratedtoolkit.nio.exceptions.SerializedObjectException;
import integratedtoolkit.nio.master.configuration.NIOConfiguration;

import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.data.operation.copy.Copy;
import integratedtoolkit.types.job.Job.JobHistory;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.resources.configuration.Configuration;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NIOAdaptor extends NIOAgent implements CommAdaptor {

    public static final int MAX_SEND = 1_000;
    public static final int MAX_RECEIVE = 1_000;

    public static final int MAX_SEND_WORKER = 5;
    public static final int MAX_RECEIVE_WORKER = 5;

    // Logging
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean WORKER_DEBUG = LogManager.getLogger(Loggers.WORKER).isDebugEnabled();

    /*
     * The master port can be: 1. Given by the IT_MASTER_PORT property 2. A BASE_MASTER_PORT plus a random number
     */
    private static final int BASE_MASTER_PORT = 43_000;
    private static final int MAX_RANDOM_VALUE = 1_000;
    private static final int RANDOM_VALUE = new Random().nextInt(MAX_RANDOM_VALUE);
    private static final int MASTER_PORT_CALCULATED = BASE_MASTER_PORT + RANDOM_VALUE;
    private static final String MASTER_PORT_PROPERTY = System.getProperty(ITConstants.IT_MASTER_PORT);
    public static final int MASTER_PORT = (MASTER_PORT_PROPERTY != null && !MASTER_PORT_PROPERTY.isEmpty())
            ? Integer.valueOf(MASTER_PORT_PROPERTY) : MASTER_PORT_CALCULATED;

    // Final jobs log directory
    private static final String JOBS_DIR = System.getProperty(ITConstants.IT_APP_LOG_DIR) + "jobs" + File.separator;

    private static final String TERM_ERR = "Error terminating";
    // private static final String SER_RCV_ERR =
    // "Error serializing received object";

    private static final HashSet<NIOWorkerNode> NODES = new HashSet<>();

    private static final ConcurrentHashMap<Integer, NIOJob> RUNNING_JOBS = new ConcurrentHashMap<>();

    private static final HashMap<Integer, LinkedList<Copy>> GROUP_TO_COPY = new HashMap<>();

    private static final HashMap<Connection, ClosingWorker> STOPPING_NODES = new HashMap<>();

    private static final HashMap<Connection, ClosingExecutor> STOPPING_EXECUTORS = new HashMap<>();

    private Semaphore tracingGeneration = new Semaphore(0);
    private Semaphore workersDebugInfo = new Semaphore(0);


    public NIOAdaptor() {
        super(MAX_SEND, MAX_RECEIVE, MASTER_PORT);
        File file = new File(JOBS_DIR);
        if (!file.exists()) {
            file.mkdir();
        }
    }

    @Override
    public void init() {
        LOGGER.info("Initializing NIO Adaptor...");
        masterNode = new NIONode(null, MASTER_PORT);

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
        tracing = System.getProperty(ITConstants.IT_TRACING) != null && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;
        tracing_level = Integer.parseInt(System.getProperty(ITConstants.IT_TRACING));

        // Start the server
        LOGGER.debug("  Starting transfer server...");
        try {
            TM.startServer(masterNode);
        } catch (CommException ce) {
            String errMsg = "Error starting transfer server";
            ErrorManager.error(errMsg, ce);
        }

        // Start the Transfer Manager thread (starts the EventManager)
        LOGGER.debug("  Starting TransferManager Thread");
        TM.start();
    }

    @Override
    public Configuration constructConfiguration(Object project_properties, Object resources_properties)
            throws ConstructConfigurationException {

        NIOConfiguration config = new NIOConfiguration(this.getClass().getName());

        integratedtoolkit.types.project.jaxb.NIOAdaptorProperties props_project = (integratedtoolkit.types.project.jaxb.NIOAdaptorProperties) project_properties;
        integratedtoolkit.types.resources.jaxb.NIOAdaptorProperties props_resources = (integratedtoolkit.types.resources.jaxb.NIOAdaptorProperties) resources_properties;

        // Get ports
        int min_project = (props_project != null) ? props_project.getMinPort() : -1;
        int min_resources = -1;
        if (props_resources != null) {
            min_resources = props_resources.getMinPort();
        } else {
            // MinPort on resources is mandatory
            throw new ConstructConfigurationException("Resources file doesn't contain a minimum port value");
        }
        int max_project = (props_project != null) ? props_project.getMaxPort() : -1;
        int max_resources = (props_resources != null) ? props_resources.getMaxPort() : -1;

        // Merge port ranges
        int min_final = -1;
        if (min_project < 0) {
            min_final = min_resources;
        } else if (min_project < min_resources) {
            LOGGER.warn("resources.xml MinPort is more restrictive than project.xml. Loading resources.xml values");
            min_final = min_resources;
        } else {
            min_final = min_project;
        }

        int max_final = -1;
        if (max_project < 0) {
            if (max_resources < 0) {
                // No max port defined
                LOGGER.warn("MaxPort not defined in resources.xml/project.xml. Loading no limit");
            } else {
                LOGGER.warn("resources.xml MaxPort is more restrictive than project.xml. Loading resources.xml values");
                max_final = max_resources;
            }
        } else if (max_resources < 0) {
            max_final = max_project;
        } else if (max_project < max_resources) {
            max_final = max_project;
        } else {
            LOGGER.warn("resources.xml MaxPort is more restrictive than project.xml. Loading resources.xml values");
            max_final = max_resources;
        }

        LOGGER.info("NIO Min Port: " + min_final);
        LOGGER.info("NIO MAX Port: " + max_final);
        config.setMinPort(min_final);
        config.setMaxPort(max_final);

        // Add remote execution command
        String remoteExecutionCommand = props_resources.getRemoteExecutionCommand();
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

    public void removedNode(NIOWorkerNode worker) {
        LOGGER.debug("Remove worker " + worker.getName());
        NODES.remove(worker);
    }

    @Override
    public void stop() {
        LOGGER.debug("NIO Adaptor stopping workers...");
        HashSet<NIOWorkerNode> workers = new HashSet<NIOWorkerNode>();
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

        LinkedList<LogicalData> obsoletes = res.clearObsoletes();
        LinkedList<String> obsoleteRenamings = new LinkedList<>();
        for (LogicalData ld : obsoletes) {
            obsoleteRenamings.add(ld.getName());
        }

        RUNNING_JOBS.put(job.getJobId(), job);
        worker.submitTask(job, obsoleteRenamings);
    }

    @Override
    public void receivedNewTask(NIONode master, NIOTask t, LinkedList<String> obsoleteFiles) {
        // Can not run any task. Do nothing
    }

    @Override
    public void setMaster(NIONode master) {
        // this is called on NIOWorker
        // Setting Master on Adaptor --> Nothing to be done
    }

    @Override
    public boolean isMyUuid(String uuid) {
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
    public void receivedTaskDone(Connection c, NIOTaskResult tr, boolean successful) {
        NIOJob nj = RUNNING_JOBS.remove(tr.getTaskId());

        // Update information
        List<DataType> taskResultTypes = tr.getParamTypes();
        for (int i = 0; i < taskResultTypes.size(); ++i) {
            switch (taskResultTypes.get(i)) {
                case PSCO_T:
                case EXTERNAL_OBJECT_T:
                    String pscoId = (String) tr.getParamValue(i);
                    DependencyParameter dp = (DependencyParameter) nj.getTaskParams().getParameters()[i];
                    updateParameter(pscoId, dp);
                    break;
                default:
                    // We only update information about PSCOs or EXTERNAL_OBJECTS
                    break;
            }
        }

        // Update out/err files
        if (nj != null) {
            JobHistory h = nj.getHistory();
            nj.taskFinished(successful);
            if (WORKER_DEBUG) {
                c.receiveDataFile(JOBS_DIR + "job" + nj.getJobId() + "_" + h + ".out");
                c.receiveDataFile(JOBS_DIR + "job" + nj.getJobId() + "_" + h + ".err");
            } else {
                if (!successful) {
                    c.receiveDataFile(JOBS_DIR + "job" + nj.getJobId() + "_" + nj.getHistory() + ".out");
                    c.receiveDataFile(JOBS_DIR + "job" + nj.getJobId() + "_" + nj.getHistory() + ".err");
                }
            }
        }

        // Close connection
        c.finishConnection();
    }

    private void updateParameter(String pscoId, DependencyParameter dp) {
        switch (dp.getType()) {
            case PSCO_T:
            case EXTERNAL_OBJECT_T:
                // The parameter was already a PSCO, we only update the information just in case
                dp.setDataTarget(pscoId);
                break;
            default:
                // The parameter was an OBJECT or a FILE, we change its type and value and register its new location
                String renaming = dp.getDataTarget();

                // Update COMM information
                String targetPath = Protocol.PERSISTENT_URI.getSchema() + pscoId;
                SimpleURI targetURI = new SimpleURI(targetPath);
                try {
                    DataLocation loc = DataLocation.createLocation(Comm.getAppHost(), targetURI);
                    Comm.registerLocation(renaming, loc);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath + " for " + renaming, e);
                }

                // Update Task information
                dp.setType(DataType.PSCO_T);
                dp.setDataTarget(pscoId);
                break;
        }
    }

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
    protected void handleDataToSendNotAvailable(Connection c, Data d) {
        // Finish the connection asap. The comm library will notify this error
        // upwards as a ClosedChannelError.
        c.finishConnection();
    }

    @Override
    public void handleRequestedDataNotAvailableError(LinkedList<DataRequest> failedRequests, String dataId) {
        for (DataRequest dr : failedRequests) {
            MasterDataRequest mdr = (MasterDataRequest) dr;
            Copy c = (Copy) mdr.getOperation();
            c.getSourceData().finishedCopy(c);
            c.end(DataOperation.OpEndState.OP_FAILED); // Notify the copy has failed
        }
    }

    @Override
    public void receivedValue(Destination type, String dataId, Object object, LinkedList<DataRequest> achievedRequests) {
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

        if (NIOTracer.isActivated()) {
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
    public LinkedList<DataOperation> getPending() {
        return new LinkedList<DataOperation>();
    }

    public boolean checkData(Data d) {
        boolean data = false;
        /*
         * for (Entry<String, LogicalData> e : Comm.DC.nameToLogicalData.entrySet()) { if
         * (d.getSourceName().equals(e.getValue().getName())) { data = true; break; } }
         */
        return data;
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
        u.setInternalURI(ID, new NIOURI(masterNode, u.getPath()));
    }

    public void requestData(Copy c, DataType paramType, Data d, String path) {
        DataRequest dr = new MasterDataRequest(c, paramType, d, path);
        addTransferRequest(dr);
        requestTransfers();

    }

    public void shuttingDown(NIOWorkerNode worker, Connection c, ShutdownListener listener) {
        STOPPING_NODES.put(c, new ClosingWorker(worker, listener));
    }

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

}
