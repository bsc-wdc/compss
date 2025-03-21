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
package es.bsc.compss.agent.rest.master;

import es.bsc.compss.comm.Comm;
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
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.OperationEndState;
import es.bsc.compss.types.data.operation.copy.DeferredCopy;
import es.bsc.compss.types.data.operation.copy.StorageCopy;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.ResourcesExternalAdaptorPropertiesSerializable;
import es.bsc.compss.types.resources.ResourcesPropertyAdaptorTypeSerializable;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ResourcesPropertyAdaptorType;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;

import java.util.List;
import java.util.Set;

import org.glassfish.jersey.client.ClientConfig;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


/**
 * Class containing the handling of the interactions with a Remote REST agent.
 */
public class RemoteRESTAgent extends COMPSsWorker {

    private final String name;
    private final AgentConfiguration config;
    // REST endpoint for the node
    private final WebTarget target;


    /**
     * Constructs a new RemoteRESTAgent using the configuration passed in as parameter.
     *
     * @param config configuration values to interact with the remote agent.
     * @param monitor element monitoring changes on the node
     */
    public RemoteRESTAgent(AgentConfiguration config, NodeMonitor monitor) {
        super(monitor);
        this.config = config;
        this.name = this.config.getHost();

        String host = config.getHost();
        String port = config.getProperty("Port");
        System.out.println("Adding resource:" + host + " through port " + port);
        if (!host.startsWith("http://")) {
            host = "http://" + host + ":" + port;
        }
        Client client = ClientBuilder.newClient(new ClientConfig());
        this.target = client.target(host);
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getAdaptor() {
        return Adaptor.class.getCanonicalName();
    }

    @Override
    public Object getProjectProperties() {
        return null;
    }

    @Override
    public ResourcesExternalAdaptorProperties getResourcesProperties() {
        ResourcesExternalAdaptorProperties properties = new ResourcesExternalAdaptorPropertiesSerializable();
        ResourcesPropertyAdaptorType property = new ResourcesPropertyAdaptorTypeSerializable();
        property.setName("Port");
        String port = config.getProperty("Port");
        property.setValue(port);
        properties.getProperty().add(property);
        return properties;
    }

    public WebTarget getTarget() {
        return this.target;
    }

    @Override
    public String getUser() {
        return "";
    }

    @Override
    public String getClasspath() {
        // No classpath for agents
        return "";
    }

    @Override
    public String getPythonpath() {
        // No pythonpath for agents
        return "";
    }

    @Override
    public void announceCreation() {
        // No need to do anything
    }

    @Override
    public void announceDestruction() {
        // No need to do anything
    }

    @Override
    public void updateTaskCount(int i) {
        // TODO: Support this operation
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void start() throws InitNodeException {
        // Should already have been started on the devices
    }

    @Override
    public void setInternalURI(MultiURI muri) throws UnstartedNodeException {

    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res,
        List<String> slaveWorkersNodeNames, JobListener listener, List<Integer> predecessors, Integer numSuccessors) {

        return new RemoteRESTAgentJob(this, taskId, taskParams, impl, res, listener, predecessors, numSuccessors);
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {
        if (target.getHosts().contains(Comm.getAppHost())) {
            // Request to master
            System.out.println("[DATA] Trying to fetch data " + ld.getName());
            // Order petition directly
            if (tgtData != null) {
                MultiURI u = ld.alreadyAvailable(Comm.getAppHost());
                if (u != null) { // Already present at the master
                    System.out.println("[DATA]  Already available!");
                    reason.setDataTarget(u.getPath());
                    listener.notifyEnd(null);
                    return;
                }
            }

        } else {
            // Request to any other
            System.out.println("[DATA] Trying to order a copy for data " + ld.getName() + " across workers");
        }
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData,
        Transferable reason, EventListener listener) {
        if (ld == null) {
            return;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Obtain Data " + ld.getName() + " as " + target);
        }

        // If it has a PSCO location, it is a PSCO -> Order new StorageCopy
        if (ld.getPscoId() != null) {
            orderStorageCopy(new StorageCopy(ld, source, target, tgtData, reason, listener));
        } else {
            if (ld.isInMemory() && ld.getValue() instanceof StubItf) {
                StubItf stub = (StubItf) ld.getValue();
                stub.makePersistent(ld.getName());
                String pscoId = stub.getID();
                System.out.println("Object " + ld.getName() + " registered as " + pscoId);
                ld.setPscoId(pscoId);
                orderStorageCopy(new StorageCopy(ld, source, target, tgtData, reason, listener));
            } else {
                listener.notifyFailure(new DeferredCopy(ld, source, target, tgtData, reason, listener),
                    new Exception("Regular objects are not supported yet"));
            }
        }
    }

    @Override
    public void enforceDataObtaining(Transferable reason, EventListener listener) {
        // Copy already done on obtainData()
        listener.notifyEnd(null);
    }

    @Override
    public void stop(ShutdownListener sl) {
        // Demon should be stopped
        sl.notifyEnd();
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        // The path of the data is the same than in the master
        String path = null;
        switch (type) {
            case DIRECTORY_T:
                path = ProtocolType.DIR_URI.getSchema() + Comm.getAppHost().getWorkingDirectory() + name;
                break;
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
    public void deleteTemporary() {
    }

    @Override
    public Set<String> generateWorkerAnalysisFiles() {
        // Agents don't need to transfer files post mortem
        return null;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        sl.notifyEnd();
    }

    @Override
    public Set<String> generateWorkerDebugFiles() {
        // Agents don't need to transfer files post mortem
        return null;
    }

    protected void orderStorageCopy(StorageCopy sc) {
        LOGGER.info("Order PSCO Copy for " + sc.getSourceData().getName());
        if (LOGGER.isDebugEnabled()) {
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
        LogicalData srcLD = sc.getSourceData();

        System.out.println("STAGE IN Requesting Storage to place a new replica of " + srcLD.getPscoId() + " on "
            + targetHostname + ")");
        LOGGER.debug("Ask for new Replica of " + srcLD.getName() + " to " + targetHostname);

        // Get the PSCO to replicate
        String pscoId = srcLD.getPscoId();

        // Get the current locations
        List<String> currentLocations;
        try {
            currentLocations = StorageItf.getLocations(pscoId);
        } catch (StorageException se) {
            // Cannot obtain current locations from back-end
            sc.end(OperationEndState.OP_FAILED, se);
            return;
        }

        if (!currentLocations.contains(targetHostname)) {
            // Perform replica
            LOGGER.debug("Performing new replica for PSCO " + pscoId);

            // TODO: WARN New replica is NOT necessary because we can't prefetch data
            // StorageItf.newReplica(pscoId, targetHostname);
        } else {
            LOGGER.debug("PSCO " + pscoId + " already present. Skip replica.");
        }

        // Update information
        sc.setFinalTarget(pscoId);
        LogicalData targetLD = sc.getTargetData();
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
        // Get the PSCOId to replicate
        String pscoId = srcLD.getPscoId();

        System.out
            .println("STAGE IN Requesting Storage to create a new Version of " + pscoId + "(" + srcLD.getName() + ")");
        if (DEBUG) {
            LOGGER.debug("Ask for new Version of " + srcLD.getName() + " with id " + pscoId + " to " + targetHostname
                + " with must preserve " + preserveSource);
        }

        // Perform version
        LOGGER.debug("Performing new version for PSCO " + pscoId);
        try {
            String newId = StorageItf.newVersion(pscoId, preserveSource, Comm.getAppHost().getName());
            LOGGER.debug("Register new new version of " + pscoId + " as " + newId);
            sc.setFinalTarget(newId);
            if (targetLD != null) {
                targetLD.setPscoId(newId);
            }
        } catch (Exception e) {
            sc.end(OperationEndState.OP_FAILED, e);
            return;
        }

        // Notify successful end
        sc.end(OperationEndState.OP_OK);
    }

    @Override
    public void increaseComputingCapabilities(ResourceDescription description) {
        // Do nothing its managed internally by the remote agent
    }

    @Override
    public void reduceComputingCapabilities(ResourceDescription description) {
        // Do nothing its managed internally by the remote agent
    }

    @Override
    public void removeObsoletes(List<MultiURI> obsoletes) {
        // TODO: Nothing done at this moment

    }

    @Override
    public void verifyNodeIsRunning() {
        // TODO should be verified that the worker is up.
    }

}
