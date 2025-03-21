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
package es.bsc.compss.gos.master;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.gos.master.configuration.GOSConfiguration;
import es.bsc.compss.gos.master.exceptions.GOSWarningException;
import es.bsc.compss.gos.master.monitoring.GOSMonitoring;
import es.bsc.compss.gos.master.sshutils.SSHHost;

import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Representation of a GAT Worker node for the Runtime.
 */
public class GOSWorkerNode extends COMPSsWorker {

    private static final String WORKER_SCRIPT_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system"
        + File.separator + "adaptors" + File.separator + "gos" + File.separator;

    protected static final String BATCH_OUTPUT_DIR = "BatchOutput";
    protected static final String CANCEL_JOB_DIR = "CancelJobsScript";
    protected static final String SSH_RESPONSE_DIR = "JobsStatus";
    private static final String INIT_SCRIPT = "init.sh";
    private static final String END_SCRIPT = "clean.sh";
    private static final String COMPRESS_SCRIPT = "compressWorkingDir.sh";
    private static final String COMPRESS_FILENAME = "compressed-remote-files.tar";

    private final GOSConfiguration config;
    private final SSHHost host;
    private final GOSAdaptor adaptor;

    protected final Map<String, GOSJob> runningJobs;

    private final String dbgPrefix;
    private final List<NodeMonitor> monitorList = new ArrayList<>();


    /**
     * New worker.
     *
     * @param monitor element monitoring changes on the node
     * @param config configuration
     */
    public GOSWorkerNode(NodeMonitor monitor, GOSConfiguration config, GOSAdaptor adaptor) {
        super(monitor);
        this.config = config;
        this.adaptor = adaptor;
        this.runningJobs = new HashMap<>();
        this.host = config.getAdaptor().getHost(config.getUser(), config.getHost());
        this.host.setPort(config.getPort());
        this.dbgPrefix = "[GOSWorkerNode " + host.getFullHostName() + "] ";
        monitorList.add(monitor);

    }

    public GOSMonitoring getGosMonitoring() {
        return config.getMonitoring();
    }

    @Override
    public String getName() {
        return config.getHost();
    }

    @Override
    public void start() throws InitNodeException {
        LOGGER.debug(dbgPrefix + "Starting and creating sandbox directory");
        initWorkingDir();
    }

    private void initWorkingDir() throws InitNodeException {
        String initScriptPath = config.getInstallDir() + WORKER_SCRIPT_PATH + INIT_SCRIPT;
        String wkDir = config.getSandboxWorkingDir();
        String args = wkDir + " " + SSH_RESPONSE_DIR + " " + CANCEL_JOB_DIR + " " + BATCH_OUTPUT_DIR;
        LOGGER.info(dbgPrefix + "Init working directory. Command: " + initScriptPath + " " + args);
        host.executeStarterCommand(initScriptPath + " " + args);
    }

    @Override
    public void setInternalURI(MultiURI uri) {
        GOSUri gosUri = new GOSUri(config.getUser(), uri.getHost(), uri.getPath(), uri.getProtocol());
        uri.setInternalURI(GOSAdaptor.ID, gosUri);
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {
        return new GOSJob(taskId, taskParams, impl, res, slaveWorkersNodeNames, listener, predecessors, numSuccessors);
    }

    @Override
    public void sendData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.info(dbgPrefix + "send data");
        }
        GOSCopy copy = new GOSCopy(srcData, source, target, tgtData, reason, listener, this, "send");
        config.getAdaptor().enqueueCopy(copy);
    }

    @Override
    public void obtainData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.info(dbgPrefix + "obtain data");
        }
        GOSCopy copy = new GOSCopy(srcData, source, target, tgtData, reason, listener, this, "obtain");
        config.getAdaptor().enqueueCopy(copy);
    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
        // Do Nothing
    }

    @Override
    public void stop(ShutdownListener sl) {
        LOGGER.info(dbgPrefix + "Stopping WorkerNode.");
        sl.notifyEnd();
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {

        String path;
        switch (type) {
            case FILE_T:
            case OBJECT_T:
            case STREAM_T:
            case EXTERNAL_STREAM_T:
            case PSCO_T:
            case EXTERNAL_PSCO_T:
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                path = ProtocolType.FILE_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            case BINDING_OBJECT_T:
                path = ProtocolType.BINDING_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            default:
                LOGGER.warn("Unrecognized type: " + type);
                return null;
        }
        // Convert path to URI
        return new SimpleURI(path);
    }

    @Override
    public void deleteTemporary() {
        LOGGER.info("GOSWorker deleteTemporary");

    }

    @Override
    public Set<String> generateWorkerAnalysisFiles() {
        LOGGER.info("GOSWorker generatePackage");
        return null;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        LOGGER.info(dbgPrefix + "Shutting down execution manager of worker");
        final String wk = getConfig().getSandboxWorkingDir();
        String ignoreBringFolder = BATCH_OUTPUT_DIR + " " + CANCEL_JOB_DIR + " " + SSH_RESPONSE_DIR;
        String compressFolderPath;
        boolean runEndCommand = true;
        if (LOGGER.isDebugEnabled()) {
            String uuid = System.getProperty(COMPSsConstants.DEPLOYMENT_ID);
            compressFolderPath = wk + config.getHost() + "-CF-" + uuid + ".tar";
            String compressCommand = config.getInstallDir() + WORKER_SCRIPT_PATH + COMPRESS_SCRIPT + " " + wk + " "
                + compressFolderPath + " " + ignoreBringFolder;

            try {
                host.executeBlockingCommand(compressCommand);
                String dst = System.getProperty(COMPSsConstants.LOG_DIR) + COMPRESS_FILENAME;
                host.getFile(compressFolderPath, dst);
            } catch (GOSWarningException e) {
                LOGGER.warn(
                    dbgPrefix + "Could not compress and/or bring remote working dir. Skipping directory removal", e);
                runEndCommand = false;
            } catch (JSchException e) {
                LOGGER.warn(dbgPrefix + "Error in during node shutdown.");
                runEndCommand = false;
            } catch (SftpException e) {
                LOGGER.warn(dbgPrefix + "Could not retrieve compress filed from remote machine.");
                runEndCommand = false;
            }
        }
        if (runEndCommand) {
            String endCommand = config.getInstallDir() + WORKER_SCRIPT_PATH + END_SCRIPT + " " + wk;
            try {
                host.executeBlockingCommand(endCommand);
            } catch (Exception e) {
                LOGGER.warn(dbgPrefix + "Could not cleanly remove remote working dir " + wk, e);
            }
        }

        host.releaseAllResources();
        sl.notifyEnd();
    }

    @Override
    public Set<String> generateWorkerDebugFiles() {
        // This feature is only for persistent workers (NIO)
        LOGGER.info(dbgPrefix + "Worker debug files not supported on GOS Adaptor");
        return null;
    }

    @Override
    public void increaseComputingCapabilities(ResourceDescription description) {

    }

    @Override
    public void reduceComputingCapabilities(ResourceDescription description) {

    }

    @Override
    public void removeObsoletes(List<MultiURI> obsoletes) {

    }

    @Override
    public void verifyNodeIsRunning() {
        // Do Nothing
    }

    @Override
    public String getUser() {
        return config.getUser();
    }

    @Override
    public String getClasspath() {
        return config.getClasspath();
    }

    @Override
    public String getPythonpath() {
        return config.getPythonpath();
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

    public GOSConfiguration getConfig() {
        return this.config;
    }

    /**
     * Returns the library path.
     *
     * @return The library path, or the string "null" if its null or empty.
     */
    public String getLibPath() {
        String libPath = this.config.getLibraryPath();
        libPath = (libPath == null || libPath.isEmpty()) ? "null" : libPath;
        return libPath;
    }

    /**
     * Gets the environment script path.
     *
     * @return The environment script path, or the string "null" if its null or empty.
     */
    public String getEnvScriptPath() {
        String envScriptPath = this.config.getEnvScript();
        envScriptPath = (envScriptPath == null || envScriptPath.isEmpty()) ? "null" : envScriptPath;
        return envScriptPath;
    }

    public void addRunningJob(GOSJob job) {
        runningJobs.put(job.getCompositeID(), job);
    }

    public void removeRunningJob(GOSJob job) {
        runningJobs.remove(job.getCompositeID());
    }

    public SSHHost getSSHHost() {
        return host;
    }

    public String getAdaptor() {
        return GOSAdaptor.class.getCanonicalName();
    }

    @Override
    public Map<String, Object> getProjectProperties() {
        return config.getProjectProperty();
    }

    @Override
    public Map<String, Object> getResourcesProperties() {
        return config.getResourcesProperties();
    }

    public void addMonitor(NodeMonitor monitor) {
        LOGGER.debug(dbgPrefix + "Added monitor " + monitor.getClass());
        monitorList.add(monitor);
    }

    public GOSAdaptor getAdaptorClass() {
        return adaptor;
    }
}
