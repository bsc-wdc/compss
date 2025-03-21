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
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOData;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.NIOUri;
import es.bsc.compss.nio.commands.CommandCancelTask;
import es.bsc.compss.nio.commands.CommandDataFetch;
import es.bsc.compss.nio.commands.CommandExecutorShutdown;
import es.bsc.compss.nio.commands.CommandNewTask;
import es.bsc.compss.nio.commands.CommandPingWorker;
import es.bsc.compss.nio.commands.CommandRemoveObsoletes;
import es.bsc.compss.nio.commands.CommandResourcesIncrease;
import es.bsc.compss.nio.commands.CommandResourcesReduce;
import es.bsc.compss.nio.commands.CommandShutdown;
import es.bsc.compss.nio.commands.tracing.CommandGenerateAnalysisFiles;
import es.bsc.compss.nio.commands.workerfiles.CommandGenerateDebugFiles;
import es.bsc.compss.nio.master.configuration.NIOConfiguration;
import es.bsc.compss.nio.master.utils.NIOParamFactory;
import es.bsc.compss.nio.requests.DataRequest;
import es.bsc.compss.nio.requests.MasterDataRequest;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.operation.OperationEndState;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.data.operation.copy.DeferredCopy;
import es.bsc.compss.types.data.operation.copy.StorageCopy;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;


public class NIOWorkerNode extends COMPSsWorker {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String DEPLOYMENT_ID = System.getProperty(COMPSsConstants.DEPLOYMENT_ID);

    protected NIONode node;
    private final NIOConfiguration config;
    private final NIOAdaptor commManager;
    protected boolean started = false;
    private WorkerStarter workerStarter;


    /**
     * Creates a new NIOWorkerNode instance.
     *
     * @param config Worker configuration.
     * @param adaptor Worker communication adaptor.
     * @param monitor element monitoring changes on the node
     */
    public NIOWorkerNode(NIOConfiguration config, NIOAdaptor adaptor, NodeMonitor monitor) {
        super(monitor);
        this.config = config;
        this.commManager = adaptor;
    }

    @Override
    public String getName() {
        return this.config.getHost();
    }

    @Override
    public String getAdaptor() {
        return NIOAdaptor.class.getCanonicalName();
    }

    @Override
    public Object getProjectProperties() {
        return null;
    }

    @Override
    public Object getResourcesProperties() {
        return null;
    }

    @Override
    public void start() throws InitNodeException {
        NIONode n = null;
        try {
            this.workerStarter = new WorkerStarter(this);
            synchronized (this.workerStarter) {
                n = this.workerStarter.startWorker();
                this.node = n;
                this.started = true;
            }
        } catch (InitNodeException e) {
            ErrorManager.warn("There was an exception when initiating worker " + getName() + ".", e);
            throw e;
        }

        if (NIOTracer.isActivated()) {
            if (DEBUG) {
                LOGGER.debug("Initializing NIO tracer " + this.getName());
                if (this.getLimitOfTasks() <= 0) {
                    LOGGER.debug("Resource " + this.getName() + " has 0 slots, it won't appear in the trace");
                } else {
                    LOGGER.debug("NIO uri File: " + ProtocolType.ANY_URI.getSchema() + File.separator
                        + NIOTracer.getTraceOutPath());
                }
            }
        }
    }

    @Override
    public String getUser() {
        return this.config.getUser();
    }

    /**
     * Returns the associated communication adaptor.
     *
     * @return The associated communication adaptor.
     */
    public NIOAdaptor getCommManager() {
        return this.commManager;
    }

    /**
     * Returns the hostname of the worker node.
     *
     * @return The hostname of the worker node.
     */
    public String getHost() {
        return this.config.getHost();
    }

    /**
     * Returns the installation directory of the worker node.
     *
     * @return The installation directory of the worker node.
     */
    public String getInstallDir() {
        return this.config.getInstallDir();
    }

    /**
     * Returns the base working directory of the worker node.
     *
     * @return The base working directory of the worker node.
     */
    public String getBaseWorkingDir() {
        return this.config.getWorkingDir();
    }

    /**
     * Returns the sandboxed working directory of the worker node.
     *
     * @return The sandboxed working directory of the worker node.
     */
    public String getWorkingDir() {
        return this.config.getSandboxWorkingDir();
    }

    /**
     * Returns the application directory.
     *
     * @return The application directory.
     */
    public String getAppDir() {
        return this.config.getAppDir();
    }

    /**
     * Returns the library path.
     *
     * @return The library path.
     */
    public String getLibPath() {
        return this.config.getLibraryPath();
    }

    @Override
    public String getClasspath() {
        return this.config.getClasspath();
    }

    @Override
    public String getPythonpath() {
        return this.config.getPythonpath();
    }

    /**
     * Returns the environment script path.
     *
     * @return The environment script path.
     */
    public String getEnvScriptPaht() {
        return this.config.getEnvScript();
    }

    /**
     * Returns the python interpreter.
     *
     * @return The python interpreter.
     */
    public String getPythonInterpreter() {
        return this.config.getPythonInterpreter();
    }

    /**
     * Returns the limit of tasks of the worker node.
     *
     * @return The limit of tasks of the worker node.
     */
    public int getLimitOfTasks() {
        return this.config.getLimitOfTasks();
    }

    /**
     * Returns the total number of CPU computing units of the worker node.
     *
     * @return The total number of CPU computing units of the worker node.
     */
    public int getTotalComputingUnits() {
        return this.config.getTotalComputingUnits();
    }

    /**
     * Returns the total number of GPU computing units of the worker node.
     *
     * @return The total number of GPU computing units of the worker node.
     */
    public int getTotalGPUs() {
        return this.config.getTotalGPUComputingUnits();
    }

    /**
     * Returns the total number of FPGA computing units of the worker node.
     *
     * @return The total number of FPGA computing units of the worker node.
     */
    public int getTotalFPGAs() {
        return this.config.getTotalFPGAComputingUnits();
    }

    /**
     * Returns the worker node configuration.
     *
     * @return The worker node configuration.
     */
    public NIOConfiguration getConfiguration() {
        return this.config;
    }

    @Override
    public void setInternalURI(MultiURI uri) throws UnstartedNodeException {
        if (node == null) {
            throw new UnstartedNodeException();
        }
        NIOUri nio = new NIOUri(node, uri.getPath(), uri.getProtocol());
        uri.setInternalURI(NIOAdaptor.ID, nio);
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {

        return new NIOJob(taskId, taskParams, impl, res, slaveWorkersNodeNames, listener, predecessors, numSuccessors);
    }

    @Override
    public void stop(ShutdownListener sl) {
        if (workerStarter != null) {
            workerStarter.setToStop();
            LOGGER.debug("Worker " + this.getName() + " set to be stopped.");
            synchronized (this.workerStarter) {
                if (started) {
                    LOGGER.debug("Shutting down " + this.getName());
                    if (node == null) {
                        sl.notifyFailure(new UnstartedNodeException());
                        LOGGER.error("Shutdown has failed");
                    }
                    Connection c = NIOAgent.getTransferManager().startConnection(node);
                    commManager.shuttingDown(this, c, sl);
                    CommandShutdown cmd = new CommandShutdown(null);
                    NIOAgent.registerOngoingCommand(c, cmd);
                    c.sendCommand(cmd);
                    c.receive();
                    c.finishConnection();
                } else {
                    LOGGER.debug("Worker " + this.getName() + " has not started.");
                    sl.notifyEnd();
                }
            }
        } else {
            LOGGER.debug("Worker " + this.getName() + " has not been created.");
            sl.notifyEnd();
        }
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener esl) {
        if (started) {
            LOGGER.debug("Shutting down execution manager " + this.getName());
            if (node == null) {
                LOGGER.error("Shutdown execution manager has failed");
                esl.notifyFailure(new UnstartedNodeException());

            }
            Connection c = NIOAgent.getTransferManager().startConnection(node);
            commManager.shuttingDownEM(this, c, esl);

            LOGGER.debug("Sending shutdown command " + this.getName());
            CommandExecutorShutdown cmd = new CommandExecutorShutdown();
            NIOAgent.registerOngoingCommand(c, cmd);
            c.sendCommand(cmd);
            c.receive();
            c.finishConnection();
        } else {
            LOGGER.debug("Worker " + this.getName() + " has not started. Considering execution manager stopped");
            esl.notifyEnd();
        }
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {
        if (DEBUG) {
            LOGGER.debug("Sending data " + ld.getName() + " from worker node " + this.getName());
        }
        if (target.getHosts().contains(Comm.getAppHost())) {
            // Request to master

            // Order petition directly
            /*
             * if (tgtData != null) { MultiURI u = tgtData.alreadyAvailable(Comm.getAppHost()); if (u != null) { if
             * (DEBUG) { LOGGER.debug("Data " + ld.getName() + " already present at the master."); }
             * reason.setDataTarget(u.getPath()); listener.notifyEnd(null); return; } }
             */
            NIOData d = getNIODatafromLogicalData(ld);
            if (source != null) {
                for (MultiURI uri : source.getURIs()) {
                    try {
                        NIOUri nURI = (NIOUri) uri.getInternalURI(NIOAdaptor.ID);
                        if (nURI != null) {
                            d.getSources().add(nURI);
                        }
                    } catch (UnstartedNodeException une) {
                        // Ignore internal URI
                    }
                }
            } else {
                LOGGER.warn(" Source location for data " + ld.getName() + " is null.");
            }
            Copy c = new DeferredCopy(ld, null, target, tgtData, reason, listener);
            String path = target.getURIInHost(Comm.getAppHost()).getPath();
            c.setFinalTarget(path);
            ld.startCopy(c, c.getTargetLoc());
            DataRequest dr = new MasterDataRequest(c, reason.getType(), d, path);
            commManager.addTransferRequest(dr);
            commManager.requestTransfers();
        } else {
            // Request to any other
            if (DEBUG) {
                LOGGER.debug(" Ordering deferred copy for data " + ld.getName());
            }
            orderCopy(new DeferredCopy(ld, source, target, tgtData, reason, listener), Comm.getAppHost().getNode());
        }
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        if (ld == null) {
            LOGGER.debug("Logical data to obtain is null");
            return;
        }

        if (DEBUG) {
            LOGGER.debug("Obtain Data " + ld.getName() + " as " + target);
        }

        // If it is a PSCO -> Order new StorageCopy
        if (ld.getPscoId() != null) {
            orderStorageCopy(new StorageCopy(ld, source, target, tgtData, reason, listener));
        } else {
            if (DEBUG) {
                LOGGER.debug("Ordering deferred copy " + ld.getName());
            }
            orderCopy(new DeferredCopy(ld, source, target, tgtData, reason, listener), this);
        }
    }

    private void orderStorageCopy(StorageCopy sc) {
        LOGGER.info("Order PSCO Copy for " + sc.getSourceData().getName());
        if (DEBUG) {
            LOGGER.debug("LD Target " + sc.getTargetData());
            LOGGER.debug("FROM: " + sc.getPreferredSource());
            LOGGER.debug("TO: " + sc.getTargetLoc());
            LOGGER.debug("MUST PRESERVE: " + sc.mustPreserveSourceData());
        }

        LogicalData source = sc.getSourceData();
        LogicalData target = sc.getTargetData();
        if (target != null) {
            if (target.getName().equals(source.getName())) {
                // The source and target are the same --> IN
                newReplica(sc);
            } else {
                // The source and target are different --> OUT
                newVersion(sc);
            }
        } else {
            // Target doesn't exist yet --> INOUT
            newVersion(sc);
        }
    }

    private void newReplica(StorageCopy sc) {
        String targetHostname = this.getName();
        final LogicalData srcLD = sc.getSourceData();
        final LogicalData targetLD = sc.getTargetData();

        LOGGER.debug("Ask for new Replica of " + srcLD.getName() + " to " + targetHostname);

        // Get the PSCO to replicate
        String pscoId = srcLD.getPscoId();

        // Get the current locations
        List<String> currentLocations;
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(TraceEvent.STORAGE_GETLOCATIONS);
        }
        try {
            currentLocations = StorageItf.getLocations(pscoId);
        } catch (StorageException se) {
            // Cannot obtain current locations from back-end
            sc.end(OperationEndState.OP_FAILED, se);
            return;
        } finally {
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEventEnd(TraceEvent.STORAGE_GETLOCATIONS);
            }
        }

        if (!currentLocations.contains(targetHostname)) {
            // Perform replica
            // TODO: Not doing new replica so we do not emit the event
            // LOGGER.debug("Performing new replica for PSCO " + pscoId);
            // if (NIOTracer.extraeEnabled()) {
            // NIOTracer.emitEvent(TraceEvent.STORAGE_NEWREPLICA.getId(), TraceEvent.STORAGE_NEWREPLICA.getType());
            // }
            // try {
            // TODO: WARN New replica is NOT necessary because we can't prefetch data
            // StorageItf.newReplica(pscoId, targetHostname);
            // } finally {
            // if (NIOTracer.extraeEnabled()) {
            //
            // NIOTracer.emitEvent(NIOTracer.EVENT_END, TraceEvent.STORAGE_NEWREPLICA.getType());
            // }
            // }
        } else {
            LOGGER.debug("PSCO " + pscoId + " already present. Skip replica.");
        }

        NIOData nd = getNIODatafromLogicalData(srcLD);
        sc.setProposedSource(nd);

        // Update information
        sc.setFinalTarget(pscoId);
        if (targetLD != null) {
            targetLD.setPscoId(pscoId);
        }

        // Notify successful end
        sc.end(OperationEndState.OP_OK);
    }

    private void newVersion(StorageCopy sc) {
        String targetHostname = this.getName();
        LogicalData srcLD = sc.getSourceData();
        LogicalData targetLD = sc.getTargetData();
        boolean preserveSource = sc.mustPreserveSourceData();

        if (DEBUG) {
            LOGGER.debug("Ask for new Version of " + srcLD.getName() + " with id " + srcLD.getPscoId() + " to "
                + targetHostname + " with must preserve " + preserveSource);
        }

        // Get the PSCOId to replicate
        String pscoId = srcLD.getPscoId();

        // Perform version
        LOGGER.debug("Performing new version for PSCO " + pscoId);
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(TraceEvent.STORAGE_NEWVERSION);
        }
        try {
            String newId = StorageItf.newVersion(pscoId, preserveSource, targetHostname);
            LOGGER.debug("Register new new version of " + pscoId + " as " + newId);
            sc.setFinalTarget(newId);
            if (targetLD != null) {
                targetLD.setPscoId(newId);
            }
            NIOUri uri = new NIOUri(null, pscoId, ProtocolType.PERSISTENT_URI);
            NIOData nd = new NIOData(srcLD.getName(), uri);
            sc.setProposedSource(nd);
        } catch (Exception e) {
            sc.end(OperationEndState.OP_FAILED, e);
            return;
        } finally {
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEventEnd(TraceEvent.STORAGE_NEWVERSION);
            }
        }

        // Notify successful end
        sc.end(OperationEndState.OP_OK);
    }

    private void orderCopy(DeferredCopy c, COMPSsNode tgtNode) {
        LOGGER.info("Order Copy for " + c.getSourceData().getName());

        Resource tgtRes = null;
        for (Resource r : c.getTargetLoc().getHosts()) {
            if (r.getNode().equals(tgtNode)) {
                tgtRes = r;
                break;
            }
        }
        final LogicalData ld = c.getSourceData();
        final LogicalData tgtData = c.getTargetData();

        synchronized (ld) {
            // Assigning target location to the copy
            String path;
            if (tgtData != null) {
                LOGGER.debug("tgtResName:" + tgtRes.getNode().getName());
                LOGGER.debug("tgtData: " + tgtData.toString());
                MultiURI u = tgtData.alreadyAvailable(tgtRes);
                if (u != null) {
                    path = u.getPath();
                    try {
                        SimpleURI simpleUri =
                            new SimpleURI(u.getProtocol().getSchema(), u.getHost().getName(), u.getPath());
                        DataLocation dt = DataLocation.createLocation(tgtRes, simpleUri);
                        c.setTargetLoc(dt);
                    } catch (Exception e) {
                        c.end(OperationEndState.OP_FAILED, e);
                        return;
                    }
                } else {
                    path = c.getTargetLoc().getURIInHost(tgtRes).getPath();
                }
            } else {
                if (c.getTargetLoc() != null) {
                    path = c.getTargetLoc().getURIInHost(tgtRes).getPath();
                } else {
                    c.end(OperationEndState.OP_FAILED,
                        new Exception(" Target location for copy " + c.getName() + " is null."));
                    return;
                }
            }
            LOGGER.debug("Setting final target in deferred copy " + path);
            c.setFinalTarget(path);

            // Assigning sources to the Copy
            NIOData dataSources = getNIODatafromLogicalData(ld);
            if (dataSources.getSources().isEmpty()) {
                for (Copy inProgressCopy : ld.getCopiesInProgress()) {
                    LOGGER.debug("No source locations for copy " + c.getName() + "." + " Waiting for copy "
                        + inProgressCopy.getName() + " to finish.");
                    prepareCopy(c, dataSources);
                    inProgressCopy.addEventListener(new EventListener() {

                        @Override
                        public void notifyEnd(DataOperation fOp) {
                            c.end(OperationEndState.OP_OK);
                        }

                        @Override
                        public void notifyFailure(DataOperation fOp, Exception e) {
                            c.end(OperationEndState.OP_FAILED, e);
                        }
                    });
                    return;
                }
                c.end(OperationEndState.OP_FAILED,
                    new Exception(" No source location nor copies in progress for copy " + c.getName()));
            }

            try {
                prepareCopy(c, dataSources);
            } catch (Exception e) {
                c.end(OperationEndState.OP_FAILED, e);
            }
        }
        c.end(OperationEndState.OP_OK);
    }

    private void prepareCopy(Copy c, NIOData dataSources) {
        LogicalData srcData = c.getSourceData();

        c.setProposedSource(dataSources);

        // TODO: MISSING CHECK IF FILE IS ALREADY BEEN COPIED IN A SHARED LOCATION
        srcData.startCopy(c, c.getTargetLoc());
        commManager.registerCopy(c);
    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
        NIOParam param = NIOParamFactory.fromParameter((Parameter) reason, this, false);
        CommandDataFetch cmd = new CommandDataFetch(param, listener.getId());
        Connection c = NIOAgent.getTransferManager().startConnection(node);
        NIOAgent.registerOngoingCommand(c, cmd);
        c.sendCommand(cmd);
        c.finishConnection();
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
        // No need to do nothing
    }

    @Override
    public void announceDestruction() {
        // No need to do nothing
    }

    @Override
    public void announceCreation() {
        // No need to do nothing
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        String path = null;
        switch (type) {
            case DIRECTORY_T:
                path = ProtocolType.DIR_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            case FILE_T:
                path = ProtocolType.FILE_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            case OBJECT_T:
                path = ProtocolType.OBJECT_URI.getSchema() + name;
                break;
            case COLLECTION_T:
                path = ProtocolType.OBJECT_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            case DICT_COLLECTION_T:
                path = ProtocolType.OBJECT_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            case STREAM_T:
                path = ProtocolType.STREAM_URI.getSchema() + name;
                break;
            case EXTERNAL_STREAM_T:
                path = ProtocolType.EXTERNAL_STREAM_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            case PSCO_T:
                // Search for the PSCO id
                String id = Comm.getData(name).getPscoId();
                path = ProtocolType.PERSISTENT_URI.getSchema() + id;
                break;
            case EXTERNAL_PSCO_T:
                // The value of the registered object in the runtime is the PSCO Id
                path = ProtocolType.PERSISTENT_URI.getSchema() + name;
                break;
            case BINDING_OBJECT_T:
                path = ProtocolType.BINDING_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            default:
                return null;
        }

        // Switch path to URI
        return new SimpleURI(path);
    }

    @Override
    public void deleteTemporary() {
        // This is only used to clean the master
        // Nothing to do
    }

    /**
     * Sends the NIOWorker CommandGenerateAnalysisFiles and waits for a CommandGenerateAnalysisFilesDone containing the
     * paths of the analysis files.
     */
    @Override
    public Set<String> generateWorkerAnalysisFiles() {
        if (!started) {
            LOGGER.debug("Worker debug files not generated because worker was not started");
            return null;
        }
        LOGGER.debug("Sending command to generated tracing package for " + this.getHost());
        if (node == null) {
            LOGGER.error("ERROR: Package generation for " + this.getHost() + " has failed.");
            return null;
        }
        Connection c = NIOAgent.getTransferManager().startConnection(node);
        CommandGenerateAnalysisFiles cmd = new CommandGenerateAnalysisFiles();
        NIOAgent.registerOngoingCommand(c, cmd);
        c.sendCommand(cmd);
        c.receive();
        c.finishConnection();
        Set<String> traceFilesPaths = commManager.waitForAnalysisFiles();
        LOGGER.debug("Worker analysis files generated");
        return traceFilesPaths;

    }

    /**
     * Sends the NIOWorker CommandGenerateDebugFiles and waits for a CommandGenerateDebugFilesDone containing the paths
     * of the debug files.
     */
    @Override
    public Set<String> generateWorkerDebugFiles() {
        if (!started) {
            LOGGER.debug("Worker debug files not generated because worker was not started");
            return null;
        }
        LOGGER.debug("Sending command to generate worker debug files for " + this.getHost());
        if (node == null) {
            LOGGER.error("Worker debug files generation has failed.");
            return null;
        }

        Connection c = NIOAgent.getTransferManager().startConnection(node);
        CommandGenerateDebugFiles cmd = new CommandGenerateDebugFiles();
        NIOAgent.registerOngoingCommand(c, cmd);
        c.sendCommand(cmd);
        c.receive();
        c.finishConnection();
        Set<String> logPath = commManager.waitUntilWorkersDebugInfoGenerated();
        LOGGER.debug("Worker debug files generated");
        return logPath;
    }

    /**
     * Submits a new task to the worker.
     *
     * @param job Job to submitJob.
     * @param obsolete List of obsolete objects to delete.
     * @throws UnstartedNodeException If the node has not started yet.
     */
    public void submitTask(NIOJob job, List<String> obsolete) throws UnstartedNodeException {
        if (node == null) {
            throw new UnstartedNodeException();
        }
        NIOTask t = job.createNIOTask();
        CommandNewTask cmd = new CommandNewTask(t, obsolete);
        Connection c = NIOAgent.getTransferManager().startConnection(node);
        NIOAgent.registerOngoingCommand(c, cmd);
        c.sendCommand(cmd);
        c.finishConnection();
    }

    /**
     * Cancels a running task in the worker.
     *
     * @param job Job to submitJob.
     */
    public void cancelTask(NIOJob job) throws UnstartedNodeException {
        if (node == null) {
            throw new UnstartedNodeException();
        }
        LOGGER.debug("Sending task cancellation command to worker");
        CommandCancelTask cmd = new CommandCancelTask(job.getJobId());
        Connection c = NIOAgent.getTransferManager().startConnection(node);
        NIOAgent.registerOngoingCommand(c, cmd);
        c.sendCommand(cmd);
        c.finishConnection();
    }

    /**
     * Marks the worker start status.
     *
     * @param b New worker start status.
     */
    public void setStarted(boolean b) {
        this.started = b;
    }

    @Override
    public void increaseComputingCapabilities(ResourceDescription description) {
        Semaphore sem = new Semaphore(0);
        MethodResourceDescription mrd = (MethodResourceDescription) description;
        CommandResourcesIncrease cmd = new CommandResourcesIncrease(mrd);
        Connection c = NIOAgent.getTransferManager().startConnection(this.node);
        this.commManager.registerPendingResourceUpdateConfirmation(c, sem);
        NIOAgent.registerOngoingCommand(c, cmd);
        c.sendCommand(cmd);
        c.receive();
        try {
            sem.acquire();
        } catch (InterruptedException ie) {
            // Do nothing
        }
    }

    @Override
    public void reduceComputingCapabilities(ResourceDescription description) {
        Semaphore sem = new Semaphore(0);
        MethodResourceDescription mrd = (MethodResourceDescription) description;
        CommandResourcesReduce cmd = new CommandResourcesReduce(mrd);
        Connection c = NIOAgent.getTransferManager().startConnection(this.node);
        this.commManager.registerPendingResourceUpdateConfirmation(c, sem);
        NIOAgent.registerOngoingCommand(c, cmd);
        c.sendCommand(cmd);
        c.receive();
        try {
            sem.acquire();
        } catch (InterruptedException ie) {
            // Do nothing
        }
    }

    protected NIOData getNIODatafromLogicalData(LogicalData ld) {
        NIOData data = new NIOData(ld.getName());

        for (MultiURI uri : ld.getURIs()) {
            try {
                Object o = uri.getInternalURI(NIOAgent.ID);
                if (o != null) {
                    data.addSource((NIOUri) o);
                }
            } catch (UnstartedNodeException une) {
                // Ignore internal URI.
            }
        }
        return data;
    }

    @Override
    public void removeObsoletes(List<MultiURI> obsoletes) {
        LOGGER.debug("Sending command to remove obsoletes for " + this.getHost());
        List<String> obsoleteRenamings = new LinkedList<>();
        for (MultiURI u : obsoletes) {
            obsoleteRenamings.add(u.getPath());
        }
        Connection c = NIOAgent.getTransferManager().startConnection(node);
        CommandRemoveObsoletes cmd = new CommandRemoveObsoletes(obsoleteRenamings);
        NIOAgent.registerOngoingCommand(c, cmd);
        c.sendCommand(cmd);
        c.finishConnection();

    }

    @Override
    public void verifyNodeIsRunning() {
        if (this.started && NIOAdaptor.registerOngoingWorkerPing(this)) {
            CommandPingWorker cmd = new CommandPingWorker(DEPLOYMENT_ID, this.getName());
            // Send command check
            Connection c = NIOAdaptor.getTransferManager().startConnection(this.node);
            NIOAgent.registerOngoingCommand(c, cmd);
            c.sendCommand(cmd);
            c.receive();
            c.finishConnection();
        }
    }

}
