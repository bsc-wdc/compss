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
package es.bsc.compss.gat.master;

import es.bsc.compss.exceptions.AnnounceException;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.gat.master.configuration.GATConfiguration;
import es.bsc.compss.gat.master.utils.GATScriptExecutor;
import es.bsc.compss.gat.master.utils.SSHManager;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.copy.Copy;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gridlab.gat.GATContext;
import org.gridlab.gat.URI;


/**
 * Representation of a GAT Worker node for the Runtime.
 */
public class GATWorkerNode extends COMPSsWorker {

    private static final String GAT_SCRIPT_PATH = File.separator + "Runtime" + File.separator + "scripts"
        + File.separator + "system" + File.separator + "adaptors" + File.separator + "gat" + File.separator;
    private static final String CLEANER_SCRIPT_NAME = "clean.sh";
    private static final String INIT_SCRIPT_NAME = "init.sh";

    private GATConfiguration config;
    private org.gridlab.gat.resources.Job tracingJob;


    /**
     * New GAT Worker Node with name @name and configuration @{code config}.
     *
     * @param config Adaptor configuration.
     * @param monitor element monitoring changes on the node
     */
    public GATWorkerNode(GATConfiguration config, NodeMonitor monitor) {
        super(monitor);
        this.config = config;
    }

    @Override
    public String getName() {
        return this.config.getHost();
    }

    @Override
    public String getAdaptor() {
        return GATAdaptor.class.getCanonicalName();
    }

    @Override
    public Map<String, Object> getProjectProperties() {
        Map<String, Object> project = new HashMap<>();
        Object brokerAdaptorName = this.config.getContext().getPreferences().get("ResourceBroker.adaptor.name");
        project.put("BrokerAdaptor", brokerAdaptorName);
        return project;
    }

    @Override
    public Map<String, Object> getResourcesProperties() {
        Map<String, Object> resources = new HashMap<>();
        Object brokerAdaptorName = this.config.getContext().getPreferences().get("ResourceBroker.adaptor.name");
        resources.put("BrokerAdaptor", brokerAdaptorName);
        return resources;
    }

    @Override
    public void start() throws InitNodeException {
        initWorkingDir();
        if (GATTracer.isActivated()) {
            LOGGER.debug("Starting GAT tracer " + this.getName());
            tracingJob = GATTracer.startTracing(this);
            waitForTracingReady();
        }
    }

    private void initWorkingDir() throws InitNodeException {
        LinkedList<URI> traceScripts = new LinkedList<>();
        LinkedList<String> traceParams = new LinkedList<>();
        String host = getHost();
        String installDir = getInstallDir();
        String workingDir = getWorkingDir();

        String user = getUser();
        if (user == null || user.isEmpty()) {
            user = "";
        } else {
            user += "@";
        }

        try {
            String initScriptPath = ProtocolType.ANY_URI.getSchema() + user + host + File.separator + installDir
                + GAT_SCRIPT_PATH + INIT_SCRIPT_NAME;
            traceScripts.add(new URI(initScriptPath));
        } catch (URISyntaxException e) {
            new InitNodeException("Error addind initScript");
        }

        String pars = workingDir;

        traceParams.add(pars);

        // Use cleaner to run the trace script and generate the package
        LOGGER.debug("Initializing working dir " + workingDir + "  in host " + getName());
        boolean result = new GATScriptExecutor(this).executeScript(traceScripts, traceParams, "init_" + host);
        if (!result) {
            throw new InitNodeException(
                "Error executing init script for initializing working dir " + workingDir + " in host " + getName());
        }
    }

    private void waitForTracingReady() {
        if (GATTracer.isActivated()) {
            GATTracer.waitForTracing(tracingJob);
        }
    }

    @Override
    public String getUser() {
        return this.config.getUser();
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
     * Returns the hostname.
     *
     * @return The hostname.
     */
    public String getHost() {
        return this.config.getHost();
    }

    /**
     * Returns the installation directory.
     *
     * @return The installation directory.
     */
    public String getInstallDir() {
        return this.config.getInstallDir();
    }

    /**
     * Returns the working directory.
     *
     * @return The working directory.
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
        String appDir = this.config.getAppDir();
        appDir = (appDir == null || appDir.isEmpty()) ? "null" : appDir;

        return appDir;
    }

    /**
     * Returns the library path.
     * 
     * @return The library path
     */
    public String getLibPath() {
        String libPath = this.config.getLibraryPath();
        libPath = (libPath == null || libPath.isEmpty()) ? "null" : libPath;

        return libPath;
    }

    /**
     * Gets the environment script path.
     * 
     * @return The environment script path
     */
    public String getEnvScriptPath() {
        String envScriptPath = this.config.getEnvScript();
        envScriptPath = (envScriptPath == null || envScriptPath.isEmpty()) ? "null" : envScriptPath;

        return envScriptPath;
    }

    /**
     * Returns the total number of computing units.
     *
     * @return The total number of computing units
     */
    public int getTotalComputingUnits() {
        return this.config.getTotalComputingUnits();
    }

    /**
     * Returns the GAT context.
     *
     * @return The GAT context
     */
    public GATContext getContext() {
        return this.config.getContext();
    }

    /**
     * Returns whether globus is enabled or not.
     *
     * @return {@literal true} if globus is enabled, {@literal false} otherwise.
     */
    public boolean isUsingGlobus() {
        return this.config.isUsingGlobus();
    }

    /**
     * Returns whether the user is needed to login the worker or not.
     *
     * @return {@literal true} if the user is needed to login the worker, {@literal false} otherwise.
     */
    public boolean isUserNeeded() {
        return this.config.isUserNeeded();
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {

        return new GATJob(taskId, taskParams, impl, res, listener, config.getContext(), config.isUserNeeded(),
            config.isUsingGlobus(), slaveWorkersNodeNames, predecessors, numSuccessors);
    }

    @Override
    public void setInternalURI(MultiURI uri) {
        String scheme = uri.getProtocol().getSchema();
        String user = this.config.getUser().isEmpty() ? "" : this.config.getUser() + "@";
        String host = this.config.getHost();
        String filePath = uri.getPath();

        String s = (scheme + user + host + File.separator + filePath);
        org.gridlab.gat.URI gat;
        try {
            gat = new org.gridlab.gat.URI(s);
            uri.setInternalURI(GATAdaptor.ID, gat);
        } catch (URISyntaxException e) {
            LOGGER.error(URI_CREATION_ERR, e);
        }
    }

    @Override
    public void stop(ShutdownListener sl) {
        try {
            String workingDir = this.config.getWorkingDir();
            if (workingDir != null && !workingDir.isEmpty()) {
                File workingDirRoot = new File(workingDir);
                File[] filesInFolder = workingDirRoot.listFiles();
                if (filesInFolder != null) {
                    for (File c : filesInFolder) {
                        delete(c);
                    }
                }
            }
        } catch (FileNotFoundException e) {
            LOGGER.warn("Could not remove clean node working dir\n" + e);
        }
        sl.notifyEnd();
    }

    private void delete(File f) throws FileNotFoundException {
        if (f.isDirectory()) {
            for (File c : f.listFiles()) {
                delete(c);
            }
        }
        if (!f.delete()) {
            throw new FileNotFoundException("Failed to delete file: " + f);
        }
    }

    @Override
    public void sendData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        Copy c = new GATCopy(srcData, source, target, tgtData, reason, listener);
        GATAdaptor.enqueueCopy(c);
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        Copy c = new GATCopy(ld, source, target, tgtData, reason, listener);
        GATAdaptor.enqueueCopy(c);
    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
        // Copy already done on obtainData()
        listener.notifyEnd(null);
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
        if (GATTracer.isActivated()) {
            LOGGER.error("Tracing system and Cloud do not work together");
        }
    }

    @Override
    public void announceCreation() throws AnnounceException {
        try {
            SSHManager.registerWorker(this);
            SSHManager.announceCreation(this);
        } catch (IOException ioe) {
            throw new AnnounceException(ioe);
        }
    }

    @Override
    public void announceDestruction() throws AnnounceException {
        try {
            SSHManager.removeKey(this);
            SSHManager.announceDestruction(this);
            SSHManager.removeWorker(this);
        } catch (IOException ioe) {
            throw new AnnounceException(ioe);
        }
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        String path = null;
        switch (type) {
            case FILE_T:
            case OBJECT_T:
            case STREAM_T:
            case EXTERNAL_STREAM_T:
            case PSCO_T:
            case EXTERNAL_PSCO_T:
                path = ProtocolType.FILE_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            case BINDING_OBJECT_T:
                path = ProtocolType.BINDING_URI.getSchema() + this.config.getSandboxWorkingDir() + name;
                break;
            default:
                return null;
        }

        // Convert path to URI
        return new SimpleURI(path);
    }

    @Override
    public void deleteTemporary() {
        LinkedList<URI> traceScripts = new LinkedList<URI>();
        LinkedList<String> traceParams = new LinkedList<String>();
        String host = getHost();
        String installDir = getInstallDir();
        String workingDir = getWorkingDir();

        String user = getUser();
        if (user == null) {
            user = "";
        } else {
            user += "@";
        }

        try {
            traceScripts.add(new URI(ProtocolType.ANY_URI.getSchema() + user + host + File.separator + installDir
                + GAT_SCRIPT_PATH + CLEANER_SCRIPT_NAME));
        } catch (URISyntaxException e) {
            LOGGER.error("Error deleting working dir " + workingDir + " in host " + getName(), e);
            return;
        }
        String pars = workingDir;

        traceParams.add(pars);

        // Use cleaner to run the trace script and generate the package
        LOGGER.debug("Deleting working dir " + workingDir + "  in host " + getName());
        boolean result = new GATScriptExecutor(this).executeScript(traceScripts, traceParams, "clean_" + host);
        if (!result) {
            LOGGER
                .error("Error executing clean script for deleting working dir " + workingDir + " in host " + getName());
        }
    }

    @Override
    public Set<String> generateWorkerAnalysisFiles() {
        LOGGER.debug("Generating GAT tracing package");
        return GATTracer.generatePackage(this);
    }

    @Override
    public Set<String> generateWorkerDebugFiles() {
        // This feature is only for persistent workers (NIO)
        LOGGER.info("Worker debug files not supported on GAT Adaptor");
        return null;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        // GAT has no execution managers, release listener immediately
        sl.notifyEnd();
    }

    @Override
    public void increaseComputingCapabilities(ResourceDescription description) {
        // Does not apply.
        // Workers are created with all the resources to run a task. After that the worker dies
    }

    @Override
    public void reduceComputingCapabilities(ResourceDescription description) {
        // Does not apply.
        // Workers are created with all the resources to run a task. After that the worker dies
    }

    @Override
    public void removeObsoletes(List<MultiURI> obsoletes) {
        // TODO Nothing done at this version

    }

    @Override
    public void verifyNodeIsRunning() {
        // TODO should be verified that the worker is up.
    }

}
