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
package es.bsc.compss.test.dummyadaptor;

import es.bsc.compss.exceptions.AnnounceException;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;

import java.util.List;
import java.util.Set;


/**
 * Dummy Worker Node for integration tests.
 */
public class DummyWorkerNode extends COMPSsWorker {

    private final String name;


    /**
     * New DummyWorker node with name {@code name} and configuration {@code config}.
     *
     * @param config Adaptor configuration.
     * @param monitor element monitoring chages on the node
     */
    public DummyWorkerNode(MethodConfiguration config, NodeMonitor monitor) {
        super(monitor);
        this.name = config.getHost();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getAdaptor() {
        return DummyAdaptor.class.getCanonicalName();
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
    }

    @Override
    public String getUser() {
        return this.name;
    }

    @Override
    public String getClasspath() {
        return this.name;
    }

    @Override
    public String getPythonpath() {
        return this.name;
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {

        return null;
    }

    @Override
    public void setInternalURI(MultiURI uri) {
    }

    @Override
    public void stop(ShutdownListener sl) {
    }

    @Override
    public void sendData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {
    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
    }

    @Override
    public void announceCreation() throws AnnounceException {
    }

    @Override
    public void announceDestruction() throws AnnounceException {
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        return null;
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
    }

    @Override
    public Set<String> generateWorkerDebugFiles() {
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
    }

}
