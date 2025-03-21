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
package es.bsc.compss.util;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.connectors.ConnectorException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The CloudManager class is an utility to manage all the cloud interactions and hide the details of each provider.
 */
public class CloudManager {

    private static final String CONNECTORS_REL_PATH =
        File.separator + "Runtime" + File.separator + "connectors" + File.separator;

    private static final String WARN_RESOURCES_PREFIX = "WARN_MSG = [";
    private static final String WARN_NO_COMPSS_HOME = "WARN: COMPSS_HOME not defined, no default connectors loaded";
    private static final String WARN_NO_COMPSS_HOME_RESOURCES =
        WARN_RESOURCES_PREFIX + "COMPSS_HOME NOT DEFINED, NO DEFAULT CONNECTORS LOADED]";
    private static final String WARN_NO_CONNECTORS_FOLDER =
        "WARN: Connectors folder not defined," + " no default connectors loaded";
    private static final String WARN_NO_CONNECTORS_FOLDER_RESOURCES =
        WARN_RESOURCES_PREFIX + "CONNECTORS FOLDER NOT DEFINED, NO DEFAULT CONNECTORS LOADED]";

    private static final Logger RUNTIME_LOGGER = LogManager.getLogger(Loggers.CM_COMP);
    private static final Logger RESOURCES_LOGGER = LogManager.getLogger(Loggers.RESOURCES);

    static {
        RUNTIME_LOGGER.debug("Loading runtime connectors to classpath...");

        String compssHome = System.getenv(COMPSsConstants.COMPSS_HOME);
        if (compssHome == null || compssHome.isEmpty()) {
            RESOURCES_LOGGER.warn(WARN_NO_COMPSS_HOME_RESOURCES);
            RUNTIME_LOGGER.warn(WARN_NO_COMPSS_HOME);
        } else {
            String connPath = compssHome + CONNECTORS_REL_PATH;
            Classpath.loadJarsInPath(connPath, RUNTIME_LOGGER);
        }
    }

    /**
     * Relation between a Cloud provider name and its representation.
     */
    private final Map<String, CloudProvider> providers;

    private boolean useCloud;
    private int initialVMs = 0;
    private int minVMs = 0;
    private int maxVMs = Integer.MAX_VALUE;


    /**
     * Initializes the internal data structures.
     */
    public CloudManager() {
        RUNTIME_LOGGER.info("Initializing Cloud Manager");
        this.useCloud = false;
        this.providers = new HashMap<>();
    }

    /**
     * Returns the number of minimum VMs.
     * 
     * @return The number of minimum VMs.
     */
    public int getMinVMs() {
        return this.minVMs;
    }

    /**
     * Returns the number of maximum VMs.
     * 
     * @return The number of maximum VMs.
     */
    public int getMaxVMs() {
        if (this.maxVMs > this.minVMs) {
            return this.maxVMs;
        } else {
            return this.minVMs;
        }
    }

    /**
     * Returns the number of initial VMs.
     * 
     * @return The number of initial VMs.
     */
    public int getInitialVMs() {
        int initialVMs = this.initialVMs;
        if (initialVMs > this.maxVMs) {
            initialVMs = this.maxVMs;
        }
        if (initialVMs < this.minVMs) {
            initialVMs = this.minVMs;
        }
        return initialVMs;
    }

    /**
     * Sets a new number of minimum VMs.
     * 
     * @param minVMs New number of minimum VMs.
     */
    public void setMinVMs(Integer minVMs) {
        if (minVMs != null) {
            if (minVMs > 0) {
                this.minVMs = minVMs;
                if (minVMs > this.maxVMs) {
                    ErrorManager.warn("Cloud: MaxVMs (" + this.maxVMs + ") is lower than MinVMs (" + this.minVMs
                        + "). The current MaxVMs value (" + this.maxVMs + ") is ignored until MinVMs (" + this.minVMs
                        + ") is lower than it");
                }
            } else {
                this.minVMs = 0;
            }
        }
    }

    /**
     * Sets a new number of maximum VMs.
     * 
     * @param maxVMs New number of maximum VMs.
     */
    public void setMaxVMs(Integer maxVMs) {
        if (maxVMs != null) {
            if (maxVMs > 0) {
                this.maxVMs = maxVMs;
            } else {
                this.maxVMs = 0;
            }
            if (this.minVMs > maxVMs) {
                ErrorManager.warn("Cloud: MaxVMs (" + this.maxVMs + ") is lower than MinVMs (" + this.minVMs
                    + "). The current MaxVMs value (" + this.maxVMs + ") is ignored until MinVMs (" + this.minVMs
                    + ") is higher than it");
            }
        }
    }

    /**
     * Sets a new number of initial VMs.
     * 
     * @param initialVMs New number of initial VMs.
     */
    public void setInitialVMs(Integer initialVMs) {
        if (initialVMs != null) {
            if (initialVMs > 0) {
                this.initialVMs = initialVMs;
            } else {
                this.initialVMs = 0;
            }
        }
    }

    /**
     * Checks whether the Cloud is used to dynamically adapt the resource pool or not.
     *
     * @return {@literal true} if the cloud is used, {@literal false} otherwise.
     */
    public boolean isUseCloud() {
        return this.useCloud;
    }

    /**
     * Adds a new Provider to the management.
     *
     * @param providerName Provider name.
     * @param limitOfVMs Provider limit of VMs.
     * @param runtimeConnectorClass Runtime abstract connector class.
     * @param connectorJarPath Path to the connector JAR.
     * @param connectorMainClass Connector main class.
     * @param connectorProperties Connector specific properties.
     * @return New cloud provider instance.
     * @throws ConnectorException When initializing the CloudProvider.
     */
    public CloudProvider registerCloudProvider(String providerName, Integer limitOfVMs, String runtimeConnectorClass,
        String connectorJarPath, String connectorMainClass, Map<String, String> connectorProperties)
        throws ConnectorException {

        CloudProvider cp = new CloudProvider(providerName, limitOfVMs, runtimeConnectorClass, connectorJarPath,
            connectorMainClass, connectorProperties);
        this.useCloud = true;
        this.providers.put(cp.getName(), cp);
        return cp;
    }

    /**
     * Returns the list of registered providers.
     * 
     * @return A list of registered providers.
     */
    public Collection<CloudProvider> getProviders() {
        return this.providers.values();
    }

    /**
     * Returns the cloud provider object with the given name.
     * 
     * @param name CloudProvider name.
     * @return CloudProvider.
     */
    public CloudProvider getProvider(String name) {
        if (this.providers.containsKey(name)) {
            return this.providers.get(name);
        }
        return null;
    }

    /**
     * Adds the new coreElements.
     * 
     * @param newCores List of new coreElements.
     */
    public void newCoreElementsDetected(List<Integer> newCores) {
        for (CloudProvider cp : this.providers.values()) {
            cp.newCoreElementsDetected(newCores);
        }
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * RESOURCE REQUESTS MANAGEMENT
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    /**
     * Queries the creation requests pending to be served.
     *
     * @return All the pending creation requests.
     */
    public List<ResourceCreationRequest> getPendingRequests() {
        List<ResourceCreationRequest> pendingRequests = new LinkedList<>();
        for (CloudProvider cp : this.providers.values()) {
            pendingRequests.addAll(cp.getPendingRequests());
        }
        return pendingRequests;
    }

    /**
     * Queries the amount of tasks that will be able to run simultaneously once all the VMs have been created.
     *
     * @return Returns all the pending creation requests.
     */
    public int[] getPendingCoreCounts() {
        int coreCount = CoreManager.getCoreCount();
        int[] pendingCoreCounts = new int[coreCount];
        for (CloudProvider cp : this.providers.values()) {
            int[] providerCounts = cp.getPendingCoreCounts();
            for (int coreId = 0; coreId < providerCounts.length; coreId++) {
                pendingCoreCounts[coreId] += providerCounts[coreId];
            }
        }
        return pendingCoreCounts;
    }

    /**
     * CloudManager terminates all the resources obtained from any provider.
     *
     * @throws ConnectorException When any CloudProvider cannot be terminated.
     */
    public void terminateALL() throws ConnectorException {
        RUNTIME_LOGGER.debug("[Cloud Manager] Terminate ALL resources");
        if (this.providers != null) {
            for (Entry<String, CloudProvider> vm : this.providers.entrySet()) {
                CloudProvider cp = vm.getValue();
                cp.terminateAll();
            }
        }
    }

    /**
     * Computes the cost per hour of the whole cloud resource pool.
     *
     * @return The cost per hour of the whole pool.
     */
    public float currentCostPerHour() {
        float total = 0;
        for (CloudProvider cp : this.providers.values()) {
            total += cp.getCurrentCostPerHour();
        }
        return total;
    }

    /**
     * The CloudManager notifies to all the connectors the end of generation of new tasks.
     */
    public void stopReached() {
        for (CloudProvider cp : this.providers.values()) {
            cp.stopReached();
        }
    }

    /**
     * The CloudManager computes the accumulated cost of the execution.
     *
     * @return Cost of the whole execution.
     */
    public float getTotalCost() {
        float total = 0;
        for (CloudProvider cp : this.providers.values()) {
            total += cp.getTotalCost();
        }
        return total;
    }

    /**
     * Returns how long will take a resource to be ready since the CloudManager asks for it.
     *
     * @return Time required for a resource to be ready.
     * @throws Exception When cannot get the creation time for some providers.
     */
    public long getNextCreationTime() throws Exception {
        long total = 0;
        for (CloudProvider cp : this.providers.values()) {
            total = Math.max(total, cp.getNextCreationTime());
        }
        return total;
    }

    /**
     * Returns the minimum time slot of all the registered providers.
     * 
     * @return The minimum time slot of all the registered providers.
     * @throws Exception When time slot cannot be retrieved from cloud provider.
     */
    public long getTimeSlot() throws Exception {
        long total = Long.MAX_VALUE;
        for (CloudProvider cp : this.providers.values()) {
            total = Math.min(total, cp.getTimeSlot());
        }
        return total;
    }

    /**
     * Gets the currently running machines on the cloud.
     *
     * @return Amount of machines on the Cloud.
     */
    public int getCurrentVMCount() {
        int total = 0;
        for (CloudProvider cp : this.providers.values()) {
            total += cp.getCurrentVMCount();
        }
        return total;
    }

    /**
     * Dumps the current state information.
     * 
     * @param prefix Prefix.
     * @return String containing the dump of the current state.
     */
    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        // Current state
        sb.append(prefix).append("CLOUD = [").append("\n");
        sb.append(prefix).append("\t").append("CURRENT_STATE = [").append("\n");
        for (CloudProvider cp : this.providers.values()) {
            sb.append(cp.getCurrentState(prefix + "\t" + "\t"));
        }
        sb.append(prefix).append("\t").append("]").append("\n");

        // Pending requests
        sb.append(prefix).append("\t").append("PENDING_REQUESTS = [").append("\n");
        for (CloudProvider cp : this.providers.values()) {
            for (ResourceCreationRequest rcr : cp.getPendingRequests()) {
                Map<CloudInstanceTypeDescription, int[]> composition = rcr.getRequested().getTypeComposition();
                // REQUEST ARE COMPOSED OF A SINGLE INSTANCE TYPE
                for (CloudInstanceTypeDescription citd : composition.keySet()) {
                    sb.append(prefix).append("\t").append("\t").append("REQUEST = ").append(citd.getName())
                        .append("\n");
                }
            }
        }
        sb.append(prefix).append("\t").append("]").append("\n");
        sb.append(prefix).append("]");

        return sb.toString();
    }
}
