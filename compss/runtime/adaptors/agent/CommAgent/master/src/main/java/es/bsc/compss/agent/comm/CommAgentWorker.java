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

package es.bsc.compss.agent.comm;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.agent.comm.messages.types.CommResource;
import es.bsc.compss.agent.types.PrivateRemoteDataLocation;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.agent.types.SharedRemoteDataLocation;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOData;
import es.bsc.compss.nio.NIOUri;
import es.bsc.compss.nio.master.NIOWorkerNode;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.NodeMonitor;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.location.SharedDisk;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * CommAgentWorker represents a remote Comm agent and implements the necessary methods to send/retrieve data to/from
 * such agent.
 */
public class CommAgentWorker extends NIOWorkerNode {

    private final CommResource remoteResource;


    /**
     * Creates a CommAgentWorker.
     */
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
    protected NIOData getNIODatafromLogicalData(LogicalData ld) {
        CommData data = new CommData(ld.getName());
        for (MultiURI uri : ld.getURIs()) {
            try {
                NIOUri o = (NIOUri) uri.getInternalURI(NIOAgent.ID);
                if (o != null) {
                    data.addSource((NIOUri) o);
                }
            } catch (UnstartedNodeException une) {
                // Ignore internal URI.
            }
        }
        this.appendRDLfromLD(ld, data.getRemoteLocations());
        return data;
    }

    private void appendRDLfromLD(LogicalData ld, Collection<RemoteDataLocation> locations) {

        boolean isLocal = false;
        for (DataLocation loc : ld.getLocations()) {
            boolean localLocation = isLocal(loc);
            isLocal = isLocal || localLocation;
            RemoteDataLocation rdl = createRemoteDLFromLocation(loc, localLocation);
            if (rdl != null) {
                locations.add(rdl);
            }
        }
        // this is done to prevent ConcurrentModificationException when iterating ld.getKnownAlias()
        // and contain the performance loss to the less frequent part of the code (this one)
        boolean done = false;
        if (isLocal) {
            Collection<RemoteDataLocation> localLocations;
            while (!done) {
                localLocations = new ArrayList<>();
                try {
                    for (String alias : ld.getKnownAlias()) {
                        localLocations.add(new PrivateRemoteDataLocation(CommAgentAdaptor.LOCAL_RESOURCE, alias));
                    }
                } catch (ConcurrentModificationException cme) {
                    LOGGER.warn("Logical data was modified while constructing it's remote data location"
                        + " to send as a result");
                }
                locations.addAll(localLocations);
                done = true;
            }
        }
    }

    private RemoteDataLocation createRemoteDLFromLocation(DataLocation loc, boolean isLocal) {
        RemoteDataLocation rdl = null;
        switch (loc.getType()) {
            case PRIVATE:
                if (!isLocal) {
                    for (MultiURI uri : loc.getURIs()) {
                        es.bsc.compss.agent.types.Resource<?, ?> hostResource;
                        if (uri.getHost() == Comm.getAppHost()) {
                            hostResource = CommAgentAdaptor.LOCAL_RESOURCE;
                        } else {
                            hostResource = createRemoteResourceFromResource(uri.getHost());
                        }
                        String pathInHost = uri.getPath();
                        if (hostResource != null) {
                            rdl = new PrivateRemoteDataLocation(hostResource, pathInHost);
                        }
                    }
                }
                break;
            case SHARED:
                SharedDisk sd = loc.getSharedDisk();
                String diskName = sd.getName();
                Map<Resource, String> sdMountpoints = sd.getAllMountpoints();
                SharedRemoteDataLocation.Mountpoint[] srdlMountpoints;
                srdlMountpoints = new SharedRemoteDataLocation.Mountpoint[sdMountpoints.size()];
                int i = 0;
                for (Map.Entry<es.bsc.compss.types.resources.Resource, String> sdMp : sdMountpoints.entrySet()) {
                    es.bsc.compss.types.resources.Resource host = sdMp.getKey();
                    es.bsc.compss.agent.types.Resource r;
                    if (host == Comm.getAppHost()) {
                        r = CommAgentAdaptor.LOCAL_RESOURCE;
                    } else {
                        r = createRemoteResourceFromResource(host);
                    }
                    String mountpoint = sdMp.getValue();
                    srdlMountpoints[i++] = new SharedRemoteDataLocation.Mountpoint(r, mountpoint);
                }
                rdl = new SharedRemoteDataLocation(diskName, loc.getPath(), srdlMountpoints);
                break;
            default:
        }
        return rdl;
    }

    private es.bsc.compss.agent.types.Resource<?, ?>
        createRemoteResourceFromResource(es.bsc.compss.types.resources.Resource res) {
        COMPSsNode node = res.getNode();

        String name = node.getName();
        String adaptor = node.getAdaptor();
        Object project = node.getProjectProperties();
        Object resources = node.getResourcesProperties();

        if (resources == null) {
            return null;
        } else {
            es.bsc.compss.agent.types.Resource<?, ?> remoteResource =
                new es.bsc.compss.agent.types.Resource<>(name, null, adaptor, project, resources);
            return remoteResource;
        }

    }

    private boolean isLocal(DataLocation dl) {
        for (es.bsc.compss.types.resources.Resource host : dl.getHosts()) {
            if (host == Comm.getAppHost()) {
                return true;
            }
        }
        return false;
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
    public Set<String> generateWorkerAnalysisFiles() {
        // Agents don't need to transfer files post mortem
        return null;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        // CommAgents are independent and they manage their own shutdown
        sl.notifyEnd();
    }

    @Override
    public Set<String> generateWorkerDebugFiles() {
        // Agents don't need to transfer files post mortem
        return null;
    }

    @Override
    public void increaseComputingCapabilities(ResourceDescription description) {
        // CommAgents are independent and they manage their own resources
    }

    @Override
    public void reduceComputingCapabilities(ResourceDescription description) {
        // CommAgents are independent and they manage their own resources
    }

    /**
     * Returns the adaptor of the node.
     */
    @Override
    public String getAdaptor() {
        return this.remoteResource.getAdaptor();
    }

    /**
     * Return a map{"Properties":project_properties_of_the_node}.
     */
    @Override
    public Object getProjectProperties() {
        return this.remoteResource.getProjectConf();
    }

    /**
     * Return a map{"Properties":resource_properties_of_the_node}.
     */
    @Override
    public Object getResourcesProperties() {
        return this.remoteResource.getResourceConf();
    }

    public String toString() {
        return "CommAgentWorker=[remoteResource=" + this.remoteResource.toString() + "; node" + this.node.toString()
            + "]";
    }

}
