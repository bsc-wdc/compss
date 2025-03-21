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
package es.bsc.compss.types.fake;

import es.bsc.compss.exceptions.AnnounceException;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
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
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import java.util.List;
import java.util.Set;


public class FakeNode extends COMPSsWorker {

    private final String name;


    public FakeNode(String name, NodeMonitor monitor) {
        super(monitor);
        this.name = name;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getAdaptor() {
        return null;
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
    public String getUser() {
        return "";
    }

    @Override
    public String getClasspath() {
        return "";
    }

    @Override
    public String getPythonpath() {
        return "";
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {

    }

    @Override
    public void announceDestruction() throws AnnounceException {

    }

    @Override
    public void announceCreation() throws AnnounceException {

    }

    @Override
    public void start() throws InitNodeException {

    }

    @Override
    public void setInternalURI(MultiURI u) throws UnstartedNodeException {

    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskparams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {
        return null;
    }

    @Override
    public void sendData(LogicalData srcData, DataLocation loc, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {

    }

    @Override
    public void obtainData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {

    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
    }

    @Override
    public void stop(ShutdownListener sl) {

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
    public void increaseComputingCapabilities(ResourceDescription descripton) {

    }

    @Override
    public void reduceComputingCapabilities(ResourceDescription descripton) {

    }

    @Override
    public void removeObsoletes(List<MultiURI> obsoletes) {

    }

    @Override
    public void verifyNodeIsRunning() {
    }

}
