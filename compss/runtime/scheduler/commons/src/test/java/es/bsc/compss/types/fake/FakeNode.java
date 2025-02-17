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

import es.bsc.compss.types.COMPSsNode;
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


public class FakeNode extends COMPSsNode {

    public FakeNode(NodeMonitor monitor) {
        super(monitor);
    }

    @Override
    public String getName() {
        return "a";
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
    public void setInternalURI(MultiURI uri) {

    }

    @Override
    public void sendData(LogicalData ld, DataLocation dl, DataLocation dl1, LogicalData ld1, Transferable t,
        EventListener el) {
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation dl, DataLocation dl1, LogicalData ld1, Transferable t,
        EventListener el) {
    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
    }

    @Override
    public Job<?> newJob(int i, TaskDescription tp, Implementation i1, Resource rsrc,
        List<String> slaveWorkersNodeNames, JobListener jl, List<Integer> predecessors, Integer numSuccessors) {

        return null;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop(ShutdownListener sl) {
    }

    @Override
    public SimpleURI getCompletePath(DataType pt, String string) {
        return new SimpleURI("");
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
