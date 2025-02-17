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
package es.bsc.compss.http.master;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.InitNodeException;
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
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.configuration.HTTPConfiguration;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import java.util.List;
import java.util.Set;


public class HTTPInstance extends COMPSsWorker {

    private final String httpWorkerName;
    private final HTTPConfiguration config;


    /**
     * Set the documentation for this schema.
     * 
     * @param config the configuration
     * @param monitor the monitor
     * @param httpWorkerName the http worker name
     */
    public HTTPInstance(HTTPConfiguration config, NodeMonitor monitor, String httpWorkerName) {
        super(monitor);
        this.config = config;
        this.httpWorkerName = httpWorkerName;
    }

    @Override
    public String getName() {
        return this.httpWorkerName;
    }

    @Override
    public String getAdaptor() {
        return HTTPAdaptor.class.getCanonicalName();
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
        // Do nothing
    }

    @Override
    public void setInternalURI(MultiURI uri) {

    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {

        return new HTTPJob(taskId, taskParams, impl, res, listener, predecessors, numSuccessors);
    }

    @Override
    public void stop(ShutdownListener sl) {
        // No need to do anything
        sl.notifyEnd();
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {
        // Never sends Data
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {

        // Delegate on the master to obtain the data value
        String path = target.getProtocol().getSchema() + target.getPath();
        DataLocation tgtLoc = null;
        try {
            SimpleURI uri = new SimpleURI(path);
            tgtLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
        }

        COMPSsNode node = Comm.getAppHost().getNode();
        node.obtainData(ld, source, tgtLoc, tgtData, reason, listener);
    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
        // Copy already done on obtainData()
        listener.notifyEnd(null);
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
        // No need to do anything
    }

    @Override
    public void announceDestruction() {
        // No need to do anything
    }

    @Override
    public void announceCreation() {
        // No need to do anything
    }

    @Override
    public String getUser() {
        return "";
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        // The path of the data is the same than in the master
        String path = null;
        switch (type) {
            case COLLECTION_T:
            case FILE_T:
                path = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkingDirectory() + name;
                break;
            case OBJECT_T:
                path = ProtocolType.OBJECT_URI.getSchema() + name;
                break;
            case STREAM_T:
                path = ProtocolType.STREAM_URI.getSchema() + name;
                break;
            case EXTERNAL_STREAM_T:
                path = ProtocolType.EXTERNAL_STREAM_URI.getSchema() + Comm.getAppHost().getWorkingDirectory() + name;
                break;
            case PSCO_T:
                path = ProtocolType.PERSISTENT_URI.getSchema() + name;
                break;
            case EXTERNAL_PSCO_T:
                path = ProtocolType.PERSISTENT_URI.getSchema() + name;
                break;
            default:
                return null;
        }

        // Switch path to URI
        return new SimpleURI(path);
    }

    @Override
    public DataLocation getTargetLocation(Resource resource, DataType type, String name) {
        SimpleURI workingPath = this.getCompletePath(type, name);
        DataLocation target = null;
        try {
            target = DataLocation.createLocation(Comm.getAppHost(), workingPath);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + workingPath.toString(), e);
        }
        return target;
    }

    @Override
    public void deleteTemporary() {
    }

    @Override
    public Set<String> generateWorkerAnalysisFiles() {
        return null;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        sl.notifyEnd();
    }

    @Override
    public Set<String> generateWorkerDebugFiles() {
        return null;
    }

    @Override
    public String getClasspath() {
        // No classpath for http
        return "";
    }

    @Override
    public String getPythonpath() {
        // No pythonpath for http
        return "";
    }

    @Override
    public void increaseComputingCapabilities(ResourceDescription description) {
        // Does not apply.
        // The computing capabilities of a http is not controlled by the http user
    }

    @Override
    public void reduceComputingCapabilities(ResourceDescription description) {
        // Does not apply.
        // The computing capabilities of a http is not controlled by the http user
    }

    @Override
    public void removeObsoletes(List<MultiURI> obsoletes) {
        // No need to do anything
    }

    @Override
    public void verifyNodeIsRunning() {
        // todo: make sure nothing to do
    }

    public HTTPConfiguration getConfig() {
        return config;
    }
}
