/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

package es.bsc.compss.agent.comm;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.agent.comm.messages.types.CommResource;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
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
import java.util.List;


/**
 * CommAgentWorker represents a remote remote Comm agent and implements the necessary methods to send/retrieve data
 * to/from such agent.
 */
class CommAgentWorker extends NIOWorkerNode {

    private final CommResource remoteResource;


    public CommAgentWorker(String name, int port, CommAgentAdaptor adaptor, NodeMonitor monitor) {
        super(null, adaptor, monitor);
        remoteResource = new CommResource(name, port);
        node = new NIONode(name, port);
        started = true;
    }

    /**
     * Returns the hostname of the worker node.
     *
     * @return The hostname of the worker node.
     */
    public String getHost() {
        return this.remoteResource.getName();
    }

    @Override
    public String getUser() {
        // Comm agents need no user to interact
        return "";
    }

    @Override
    public String getClasspath() {
        // Comm agents are already configured no need to specify classpath
        return "";
    }

    @Override
    public String getPythonpath() {
        // Comm agents are already configured no need to specify python path
        return "";
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
        // This method is useless.
    }

    @Override
    public void announceDestruction() {
        // No need to announce the destruction of an agent.
    }

    @Override
    public void announceCreation() {
        // No need to announce the creation of an agent.
    }

    @Override
    public String getName() {
        return remoteResource.getName();
    }

    @Override
    public void start() throws InitNodeException {
        // Comm agents are already running when they are configured.
    }

    @Override
    public void setInternalURI(MultiURI u) throws UnstartedNodeException {
        CommAgentURI nu = new CommAgentURI(remoteResource, this.node, u.getPath(), ProtocolType.ANY_URI);
        u.setInternalURI(CommAgentAdaptor.ID, nu);
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {
        return new CommAgentJob(taskId, taskParams, impl, res, slaveWorkersNodeNames, listener, predecessors,
            numSuccessors);
    }

    @Override
    public void stop(ShutdownListener sl) {
        // Comm agents are to remain running when another agent ends using them.
        sl.notifyEnd();
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        // With agents the path shouldn't matter
        // Checking on LD it should find out where to find the data on the device
        SimpleURI uri = new SimpleURI(name);
        return uri;
    }

    @Override
    public void deleteTemporary() {
        // CommAgents are independent and they manage their own temporary files individually.
    }

    @Override
    public boolean generatePackage() {
        // Agent do not support tracing yet
        return false;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        // CommAgents are independent and they manage their own shutdown
        sl.notifyEnd();
    }

    @Override
    public boolean generateWorkersDebugInfo() {
        // Comm agents debug info remains on the agent node.
        return false;
    }

    @Override
    public void increaseComputingCapabilities(ResourceDescription description) {
        // CommAgents are independent and they manage their own resources
    }

    @Override
    public void reduceComputingCapabilities(ResourceDescription description) {
        // CommAgents are independent and they manage their own resources
    }

}
