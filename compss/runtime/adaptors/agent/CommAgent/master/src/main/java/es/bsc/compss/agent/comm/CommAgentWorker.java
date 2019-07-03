/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.comm.Node;
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.agent.comm.messages.types.CommResource;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.nio.NIOUri;
import es.bsc.compss.nio.master.NIOWorkerNode;
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

    private final CommResource localResource;
    private final Node node;

    public CommAgentWorker(String name, int port, CommAgentAdaptor adaptor) {
        super(null, adaptor);
        localResource = new CommResource(name, port);
        node = new NIONode(name, port);
    }

    @Override
    public String getUser() {
        // Comm agents need no user to interact
        return null;
    }

    @Override
    public String getClasspath() {
        // Comm agents are already configured no need to specify classpath
        return null;
    }

    @Override
    public String getPythonpath() {
        // Comm agents are already configured no need to specify python path
        return null;
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
        return localResource.getName();
    }

    @Override
    public void start() throws InitNodeException {
        // Comm agents are already running when they are configured.
    }

    @Override
    public void setInternalURI(MultiURI u) throws UnstartedNodeException {
        RemoteDataLocation rdl = new RemoteDataLocation(localResource, u.getPath());
        System.out.println("Setting internal URI " + rdl + " to " + u);
        NIOUri nu = new NIOUri((NIONode) this.node, rdl.getPath(), ProtocolType.ANY_URI);
        u.setInternalURI(CommAgentAdaptor.ID, nu);
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskparams, Implementation impl, Resource res,
            List<String> slaveWorkersNodeNames, JobListener listener) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void stop(ShutdownListener sl) {
        // Comm agents are to remain running when another agent ends using them.
        sl.notifyEnd();
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        throw new UnsupportedOperationException("Not supported yet.");
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
