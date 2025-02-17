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
package es.bsc.compss.types;

import es.bsc.compss.connectors.Connector;
import es.bsc.compss.connectors.ConnectorException;
import es.bsc.compss.connectors.Cost;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.listeners.ResourceCreationListener;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.util.CloudImageManager;
import es.bsc.compss.util.CloudTypeManager;
import es.bsc.compss.util.CoreManager;

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CloudProvider {

    private static final String WARN_NO_VALID_INSTANCE = "WARN: Cannot find a containing/contained instanceType";
    private static final String WARN_NO_VALID_IMAGE = "WARN: Cannot find a containing/contained instanceType";
    private static final String WARN_CANNOT_TURN_ON = "WARN: Connector cannot turn on resource";

    private final String name;
    private final Integer limitOfVMs;

    private final Set<CloudMethodWorker> hostedWorkers;
    private final CloudImageManager imgManager;
    private final CloudTypeManager typeManager;

    private final Connector connector;
    private final Cost cost;

    private int currentVMCount;
    private final List<ResourceCreationRequest> pendingRequests;
    private int[] pendingCoreCount;

    // Loggers
    private static final Logger LOGGER = LogManager.getLogger(Loggers.CM_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();


    /**
     * Creates a new CloudProvider instance.
     * 
     * @param providerName Cloud Provider name.
     * @param limitOfVMs Number of maximum VMs.
     * @param runtimeConnectorClass Runtime connector fully qualified class name.
     * @param connectorJarPath Path to the external connector JAR.
     * @param connectorMainClass External connector fully qualified class name.
     * @param connectorProperties Specific connector properties.
     * @throws ConnectorException When an internal connector exception occurs.
     */
    public CloudProvider(String providerName, Integer limitOfVMs, String runtimeConnectorClass, String connectorJarPath,
        String connectorMainClass, Map<String, String> connectorProperties) throws ConnectorException {

        this.name = providerName;
        this.limitOfVMs = limitOfVMs;
        this.currentVMCount = 0;
        this.hostedWorkers = new HashSet<>();
        this.imgManager = new CloudImageManager();
        this.typeManager = new CloudTypeManager();

        // Load Runtime connector implementation that will finally load the
        // infrastructure dependent connector
        try {
            Class<?> conClass = Class.forName(runtimeConnectorClass);
            Class<?>[] parameterTypes = new Class<?>[] { CloudProvider.class,
                String.class,
                String.class,
                Map.class };
            Constructor<?> ctor = conClass.getConstructor(parameterTypes);
            Object conector = ctor.newInstance(this, connectorJarPath, connectorMainClass, connectorProperties);
            this.connector = (Connector) conector;
            this.cost = (Cost) conector;
        } catch (Exception e) {
            throw new ConnectorException(e);
        }
        this.pendingRequests = new LinkedList<>();
        this.pendingCoreCount = new int[CoreManager.getCoreCount()];
    }

    /*
     * ------- Cloud Provider Builders --------
     */

    /**
     * Adds an image description to the Cloud Provider.
     *
     * @param cid Description of the features offered by that image.
     */
    public void addCloudImage(CloudImageDescription cid) {
        this.imgManager.add(cid);
    }

    /**
     * Adds an instance type description to a Cloud Provider.
     *
     * @param rd Description of the features offered by that instance type.
     */
    public void addInstanceType(CloudInstanceTypeDescription rd) {
        this.typeManager.addType(rd);
    }

    /**
     * Add new core elements.
     * 
     * @param newCores List of new core elements.
     */
    public void newCoreElementsDetected(List<Integer> newCores) {
        this.typeManager.newCoreElementsDetected(newCores);
        this.pendingCoreCount = new int[CoreManager.getCoreCount()];
        for (ResourceCreationRequest creationRequest : this.pendingRequests) {
            int[][] newRequestedSimultaneousTaskCount = this.computeSimultaneousCounts(creationRequest.getRequested());
            creationRequest.updateRequestedSimultaneousTaskCount(newRequestedSimultaneousTaskCount);

            for (int coreId = 0; coreId < newRequestedSimultaneousTaskCount.length; coreId++) {
                int coreSlots = 0;
                for (int implId = 0; implId < newRequestedSimultaneousTaskCount[coreId].length; implId++) {
                    coreSlots = Math.max(coreSlots, newRequestedSimultaneousTaskCount[coreId][implId]);
                }
                this.pendingCoreCount[coreId] += coreSlots;
            }
        }
    }

    /*
     * ------------- Basic Queries -------------
     */

    /**
     * Returns the Cloud Provider name.
     * 
     * @return The Cloud Provider name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns the current cost per hour.
     * 
     * @return The current cost per hour.
     */
    public float getCurrentCostPerHour() {
        return this.cost.currentCostPerHour();
    }

    /**
     * Returns the total accumulated cost.
     * 
     * @return The total accumulated cost.
     */
    public float getTotalCost() {
        return this.cost.getTotalCost();
    }

    /**
     * Returns all the instance types available at the Cloud Provider.
     * 
     * @return All the instance types available at the Cloud Provider.
     */
    public Collection<CloudInstanceTypeDescription> getAllTypes() {
        return this.typeManager.getAllTypes();
    }

    /**
     * Returns all the names of the available instance types at the Cloud Provider.
     * 
     * @return All the names of the available instance types at the Cloud Provider.
     */
    public Set<String> getAllInstanceTypeNames() {
        return this.typeManager.getAllTypeNames();
    }

    /**
     * Returns the instance type with the given name {@code name}.
     * 
     * @param name Instance type name.
     * @return The associated instance type.
     */
    public CloudInstanceTypeDescription getInstanceType(String name) {
        return this.typeManager.getType(name);
    }

    /**
     * Returns the compatible instance types with the given requirements {@code requirements}.
     * 
     * @param requirements Method Description requirements.
     * @return All the compatible instance types with the given requirements {@code requirements}.
     */
    public List<CloudInstanceTypeDescription> getCompatibleTypes(MethodResourceDescription requirements) {
        return this.typeManager.getCompatibleTypes(requirements);
    }

    /**
     * Returns all the available images.
     * 
     * @return All the available images.
     */
    public Collection<CloudImageDescription> getAllImages() {
        return this.imgManager.getAllImages();
    }

    /**
     * Returns all the names of the available images.
     * 
     * @return All the names of the available images.
     */
    public Set<String> getAllImageNames() {
        return this.imgManager.getAllImageNames();
    }

    /**
     * Returns the image with the given name {@code name}.
     * 
     * @param name Image name.
     * @return The image with the given name {@code name}.
     */
    public CloudImageDescription getImage(String name) {
        return this.imgManager.getImage(name);
    }

    /**
     * Returns a list of the compatible images with the given requirements {@code requirements}.
     * 
     * @param requirements Method Resource requirements.
     * @return A list of the compatible images with the given requirements {@code requirements}.
     */
    public List<CloudImageDescription> getCompatibleImages(MethodResourceDescription requirements) {
        return this.imgManager.getCompatibleImages(requirements);
    }

    /**
     * Returns the cost per hour of the given instance.
     * 
     * @param instanceDescription Instance description.
     * @return The cost per hour of the given instance.
     */
    public Float getInstanceCostPerHour(CloudMethodResourceDescription instanceDescription) {
        return this.cost.getMachineCostPerHour(instanceDescription);
    }

    /**
     * Returns the simultaneous implementations of each core that can be run in the given instance type.
     * 
     * @param type Instance type.
     * @return The number of simultaneous runs of each implementation of each core.
     */
    public int[][] getSimultaneousImpls(String type) {
        return this.typeManager.getSimultaneousImpls(type);
    }

    /**
     * Returns whether the connector supports automatic scaling or not.
     * 
     * @return {@literal true} if the connector supports automatic scaling, {@literal false} otherwise.
     */
    public boolean isAutomaticScalingEnabled() {
        return this.connector.isAutomaticScalingEnabled();
    }

    /**
     * Returns the time until the next VM creation.
     * 
     * @return The time until the next VM creation.
     * @throws Exception When an internal connector exception occurs.
     */
    public long getNextCreationTime() throws Exception {
        return this.connector.getNextCreationTime();
    }

    /**
     * Returns the time slot.
     * 
     * @return The time slot.
     * @throws Exception When an internal connector exception occurs.
     */
    public long getTimeSlot() throws Exception {
        return this.connector.getTimeSlot();
    }

    /**
     * Returns a list of the pending requests.
     * 
     * @return A list of the pending requests.
     */
    public List<ResourceCreationRequest> getPendingRequests() {
        return this.pendingRequests;
    }

    /**
     * Returns the pending core counts.
     * 
     * @return The pending core counts.
     */
    public int[] getPendingCoreCounts() {
        return this.pendingCoreCount;
    }

    /**
     * Returns the current number of VMs.
     * 
     * @return The current number of VMs.
     */
    public int getCurrentVMCount() {
        return this.currentVMCount;
    }

    /**
     * Returns the hosted workers.
     * 
     * @return The hosted workers.
     */
    public Set<CloudMethodWorker> getHostedWorkers() {
        return this.hostedWorkers;
    }

    /*
     * ------------- State Changes -------------
     */

    /**
     * Marks that the Cloud Provider is stopped.
     */
    public void stopReached() {
        this.connector.stopReached();
    }

    /**
     * Terminates all current VMs.
     */
    public void terminateAll() {
        this.currentVMCount = 0;
        this.hostedWorkers.clear();
        this.connector.terminateAll();
    }

    /**
     * Requests for a new resource creation.
     * 
     * @param instanceDescription Resource description to create.
     * @param listener listener to be notified progress on the resource creation
     * @return The new ResourceCreationRequest.
     */
    public ResourceCreationRequest requestResourceCreation(CloudMethodResourceDescription instanceDescription,
        ResourceCreationListener listener) {

        int[][] simultaneousCounts = computeSimultaneousCounts(instanceDescription);
        String requestID = "compss" + UUID.randomUUID().toString();
        ResourceCreationRequest rcr =
            new ResourceCreationRequest(instanceDescription, simultaneousCounts, this, requestID, listener);
        if (DEBUG) {
            LOGGER.debug("[Cloud Manager] Asking for resource creation with image "
                + instanceDescription.getImage().getImageName());
        }

        boolean isRequestAccepted = this.connector.turnON(requestID, rcr);
        if (isRequestAccepted) {
            CloudMethodResourceDescription cmrd = rcr.getRequested();
            for (int[] typeCount : cmrd.getTypeComposition().values()) {
                this.currentVMCount += typeCount[0];
            }
            this.pendingRequests.add(rcr);
            for (int coreId = 0; coreId < simultaneousCounts.length; coreId++) {
                int coreSlots = 0;
                for (int implId = 0; implId < simultaneousCounts[coreId].length; implId++) {
                    coreSlots = Math.max(coreSlots, simultaneousCounts[coreId][implId]);
                }
                this.pendingCoreCount[coreId] += coreSlots;
            }
            return rcr;
        } else {
            LOGGER.warn(WARN_CANNOT_TURN_ON);
            return null;
        }
    }

    private int[][] computeSimultaneousCounts(CloudMethodResourceDescription cloudDescription) {
        int coreCount = CoreManager.getCoreCount();
        int[][] simultaneousCounts = new int[coreCount][];

        for (int coreId = 0; coreId < coreCount; coreId++) {
            int implCount = CoreManager.getNumberCoreImplementations(coreId);
            simultaneousCounts[coreId] = new int[implCount];
        }
        for (java.util.Map.Entry<CloudInstanceTypeDescription, int[]> typeEntry : cloudDescription.getTypeComposition()
            .entrySet()) {
            CloudInstanceTypeDescription citd = typeEntry.getKey();
            int count = typeEntry.getValue()[0];
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (int implId = 0; implId < simultaneousCounts[coreId].length; implId++) {
                    simultaneousCounts[coreId][implId] += citd.getSlotsImpl()[coreId][implId] * count;
                }
            }
        }

        return simultaneousCounts;
    }

    /**
     * Refuses the given resource creation request.
     * 
     * @param rcr Resource Creation Request to refuse.
     */
    public void refusedCreation(ResourceCreationRequest rcr) {
        CloudMethodResourceDescription cmrd = rcr.getRequested();
        for (int[] typeCount : cmrd.getTypeComposition().values()) {
            this.currentVMCount -= typeCount[0];
        }
        this.pendingRequests.remove(rcr);
        int[][] simultaneousCounts = rcr.requestedSimultaneousTaskCount();
        for (int coreId = 0; coreId < simultaneousCounts.length; coreId++) {
            int coreSlots = 0;
            for (int implId = 0; implId < simultaneousCounts[coreId].length; implId++) {
                coreSlots = Math.max(coreSlots, simultaneousCounts[coreId][implId]);
            }
            this.pendingCoreCount[coreId] -= coreSlots;
        }
    }

    /**
     * Confirms the given resource creation request.
     * 
     * @param rcr Resource Creation Request to accept.
     * @param worker Associated cloud worker.
     * @param granted Resource description of the granted VM.
     */
    public void confirmedCreation(ResourceCreationRequest rcr, CloudMethodWorker worker,
        CloudMethodResourceDescription granted) {
        CloudMethodResourceDescription cmrd = rcr.getRequested();
        for (int[] typeCount : cmrd.getTypeComposition().values()) {
            this.currentVMCount -= typeCount[0];
        }
        for (int[] typeCount : granted.getTypeComposition().values()) {
            this.currentVMCount += typeCount[0];
        }
        this.pendingRequests.remove(rcr);
        int[][] simultaneousCounts = rcr.requestedSimultaneousTaskCount();
        for (int coreId = 0; coreId < simultaneousCounts.length; coreId++) {
            int coreSlots = 0;
            for (int implId = 0; implId < simultaneousCounts[coreId].length; implId++) {
                coreSlots = Math.max(coreSlots, simultaneousCounts[coreId][implId]);
            }
            this.pendingCoreCount[coreId] -= coreSlots;
        }
        this.hostedWorkers.add(worker);
    }

    /**
     * Requests the reduction of a given worker.
     * 
     * @param worker Worker to reduce.
     * @param reduction Reduction to perform.
     */
    public void requestResourceReduction(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        LOGGER.debug("[Cloud Manager] Destroying resource " + worker.getName() + " for reduction");
        Map<CloudInstanceTypeDescription, int[]> composition = reduction.getTypeComposition();
        for (int[] typeCount : composition.values()) {
            this.currentVMCount -= typeCount[0];
        }
        if (worker.getDescription().getTypeComposition().isEmpty()) {
            this.hostedWorkers.remove(worker);
        }
        this.connector.terminate(worker, reduction);
    }

    /**
     * Returns the hosted worker with the given name.
     * 
     * @param name Name of the hosted worker.
     * @return The associated worker.
     */
    public CloudMethodWorker getHostedWorker(String name) {
        for (CloudMethodWorker vm : this.hostedWorkers) {
            if (vm.getName().equals(name)) {
                return vm;
            }
        }
        return null;
    }

    /*
     * ------- Recommendation Queries ----------
     */

    /**
     * Returns whether the Cloud Provider can host more instances or not.
     * 
     * @return {@literal true} if the Cloud Provider can host more instances, {@literal false} otherwise.
     */
    public boolean canHostMoreInstances() {
        if (this.limitOfVMs == null) {
            return true;
        }
        if (this.limitOfVMs == -1) {
            return true;
        }
        return this.currentVMCount < this.limitOfVMs;
    }

    /**
     * Returns the resource description of the combination of the given instance type and image.
     * 
     * @param instanceTypeName Instance type name.
     * @param imageName Image name.
     * @return Resource description of the combination of the given instance type and image.
     */
    public CloudMethodResourceDescription getResourceDescription(String instanceTypeName, String imageName) {
        CloudMethodResourceDescription result = null;
        CloudInstanceTypeDescription type = this.typeManager.getType(instanceTypeName);
        if (type != null) {
            CloudImageDescription image = this.imgManager.getImage(imageName);
            if (image != null) {
                result = new CloudMethodResourceDescription(type, image);
                result.setValue(this.cost.getMachineCostPerHour(result));
            } else {
                LOGGER.warn(WARN_NO_VALID_IMAGE);
            }
        } else {
            LOGGER.warn(WARN_NO_VALID_INSTANCE);
        }
        return result;
    }

    /*
     * ------------- Debug Queries -------------
     */

    /**
     * Dumps the current state to a string leaded by the given prefix.
     * 
     * @param prefix Indentation prefix.
     * @return A string containing a dump of the current state.
     */
    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("PROVIDER = [").append("\n");
        sb.append(prefix).append("\t").append("NAME = ").append(this.name).append("\n");
        sb.append(prefix).append("\t").append("CURRENT_VM = ").append(this.currentVMCount).append("\n");
        sb.append(prefix).append("\t").append("LIMIT_VM = ").append(this.limitOfVMs).append("\n");
        sb.append(this.imgManager.getCurrentState(prefix + "\t"));
        sb.append(this.typeManager.getCurrentState(prefix + "\t"));

        // Virtual Instances
        sb.append(prefix).append("\t").append("VIRTUAL_INSTANCES = [").append("\n");
        for (CloudMethodWorker vm : this.hostedWorkers) {
            CloudMethodResourceDescription cmrd = vm.getDescription();
            sb.append(cmrd.getCurrentState(prefix + "\t")).append("\n");
        }
        sb.append(prefix).append("\t").append("]").append("\n");

        sb.append(prefix).append("]").append("\n");

        return sb.toString();
    }

}
