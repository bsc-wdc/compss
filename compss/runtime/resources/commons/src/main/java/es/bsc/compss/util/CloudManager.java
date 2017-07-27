package es.bsc.compss.util;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.connectors.ConnectorException;
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

    private static final String CONNECTORS_REL_PATH = File.separator + "Runtime" + File.separator + "connectors" + File.separator;

    private static final String WARN_NO_COMPSS_HOME = "WARN: COMPSS_HOME not defined, no default connectors loaded";
    private static final String WARN_NO_COMPSS_HOME_RESOURCES = "WARN_MSG = [COMPSS_HOME NOT DEFINED, NO DEFAULT CONNECTORS LOADED]";
    private static final String WARN_NO_CONNECTORS_FOLDER = "WARN: Connectors folder not defined, no default connectors loaded";
    private static final String WARN_NO_CONNECTORS_FOLDER_RESOURCES = "WARN_MSG = [CONNECTORS FOLDER NOT DEFINED, NO DEFAULT CONNECTORS LOADED]";

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
            try {
                Classpath.loadPath(connPath, RUNTIME_LOGGER);
            } catch (FileNotFoundException fnfe) {
                ErrorManager.warn("Connector jar " + connPath + " not found.");
                RESOURCES_LOGGER.warn(WARN_NO_CONNECTORS_FOLDER_RESOURCES);
                RUNTIME_LOGGER.warn(WARN_NO_CONNECTORS_FOLDER);
            }
        }
    }

    /**
     * Relation between a Cloud provider name and its representation
     */
    private final Map<String, CloudProvider> providers;

    private boolean useCloud;
    private int initialVMs = 0;
    private int minVMs = 0;
    private int maxVMs = Integer.MAX_VALUE;


    /**
     * Initializes the internal data structures
     *
     */
    public CloudManager() {
        RUNTIME_LOGGER.info("Initializing Cloud Manager");
        useCloud = false;
        providers = new HashMap<>();
    }

    public int getMinVMs() {
        return minVMs;
    }

    public int getMaxVMs() {
        if (this.maxVMs > this.minVMs) {
            return maxVMs;
        } else {
            return this.minVMs;
        }
    }

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

    public void setMinVMs(Integer minVMs) {
        if (minVMs != null) {
            if (minVMs > 0) {
                this.minVMs = minVMs;
                if (minVMs > maxVMs) {
                    ErrorManager
                            .warn("Cloud: MaxVMs (" + maxVMs + ") is lower than MinVMs (" + this.minVMs + "). The current MaxVMs value ("
                                    + maxVMs + ") is ignored until MinVMs (" + this.minVMs + ") is lower than it");
                }
            } else {
                this.minVMs = 0;
            }
        }
    }

    public void setMaxVMs(Integer maxVMs) {
        if (maxVMs != null) {
            if (maxVMs > 0) {
                this.maxVMs = maxVMs;
            } else {
                this.maxVMs = 0;
            }
            if (minVMs > maxVMs) {
                ErrorManager
                        .warn("Cloud: MaxVMs (" + this.maxVMs + ") is lower than MinVMs (" + this.minVMs + "). The current MaxVMs value ("
                                + this.maxVMs + ") is ignored until MinVMs (" + this.minVMs + ") is higher than it");
            }
        }
    }

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
     * Check if Cloud is used to dynamically adapt the resource pool
     *
     * @return true if it is used
     */
    public boolean isUseCloud() {
        return useCloud;
    }

    /**
     * Adds a new Provider to the management
     *
     * @param providerName
     * @param limitOfVMs
     * @param runtimeConnectorClass
     * @param connectorJarPath
     * @param connectorMainClass
     * @param connectorProperties
     *
     * @return
     * @throws es.bsc.compss.connectors.ConnectorException
     */
    public CloudProvider registerCloudProvider(String providerName, Integer limitOfVMs, String runtimeConnectorClass,
            String connectorJarPath, String connectorMainClass, Map<String, String> connectorProperties) throws ConnectorException {

        CloudProvider cp = new CloudProvider(providerName, limitOfVMs, runtimeConnectorClass, connectorJarPath, connectorMainClass,
                connectorProperties);
        useCloud = true;
        providers.put(cp.getName(), cp);
        return cp;
    }

    public Collection<CloudProvider> getProviders() {
        return providers.values();
    }

    public CloudProvider getProvider(String name) {
        if (providers.containsKey(name)) {
            return providers.get(name);
        }
        return null;
    }

    public void newCoreElementsDetected(List<Integer> newCores) {
        for (CloudProvider cp : providers.values()) {
            cp.newCoreElementsDetected(newCores);
        }
    }

    /**
     * *********************************************************************************************************
     * *********************************************************************************************************
     * RESOURCE REQUESTS MANAGEMENT
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    /**
     * Queries the creation requests pending to be served
     *
     * @return Returns all the pending creation requests
     */
    public List<ResourceCreationRequest> getPendingRequests() {
        List<ResourceCreationRequest> pendingRequests = new LinkedList<>();
        for (CloudProvider cp : providers.values()) {
            pendingRequests.addAll(cp.getPendingRequests());
        }
        return pendingRequests;
    }

    /**
     * Queries the amount of tasks that will be able to run simulataneously once all the VMs have been created
     *
     * @return Returns all the pending creation requests
     */
    public int[] getPendingCoreCounts() {
        int coreCount = CoreManager.getCoreCount();
        int[] pendingCoreCounts = new int[coreCount];
        for (CloudProvider cp : providers.values()) {
            int[] providerCounts = cp.getPendingCoreCounts();
            for (int coreId = 0; coreId < providerCounts.length; coreId++) {
                pendingCoreCounts[coreId] += providerCounts[coreId];
            }
        }
        return pendingCoreCounts;
    }

    /**
     * CloudManager terminates all the resources obtained from any provider
     *
     * @throws ConnectorException
     */
    public void terminateALL() throws ConnectorException {
        RUNTIME_LOGGER.debug("[Cloud Manager] Terminate ALL resources");
        if (providers != null) {
            for (Entry<String, CloudProvider> vm : providers.entrySet()) {
                CloudProvider cp = vm.getValue();
                cp.terminateAll();
            }
        }
    }

    /**
     * Computes the cost per hour of the whole cloud resource pool
     *
     * @return the cost per hour of the whole pool
     */
    public float currentCostPerHour() {
        float total = 0;
        for (CloudProvider cp : providers.values()) {
            total += cp.getCurrentCostPerHour();
        }
        return total;
    }

    /**
     * The CloudManager notifies to all the connectors the end of generation of new tasks
     */
    public void stopReached() {
        for (CloudProvider cp : providers.values()) {
            cp.stopReached();
        }
    }

    /**
     * The CloudManager computes the accumulated cost of the execution
     *
     * @return cost of the whole execution
     */
    public float getTotalCost() {
        float total = 0;
        for (CloudProvider cp : providers.values()) {
            total += cp.getTotalCost();
        }
        return total;
    }

    /**
     * Returns how long will take a resource to be ready since the CloudManager asks for it.
     *
     * @return time required for a resource to be ready
     * @throws Exception
     *             can not get the creation time for some providers.
     */
    public long getNextCreationTime() throws Exception {
        long total = 0;
        for (CloudProvider cp : providers.values()) {
            total = Math.max(total, cp.getNextCreationTime());
        }
        return total;
    }

    public long getTimeSlot() throws Exception {
        long total = Long.MAX_VALUE;
        for (CloudProvider cp : providers.values()) {
            total = Math.min(total, cp.getTimeSlot());
        }
        return total;
    }

    /**
     * Gets the currently running machines on the cloud
     *
     * @return amount of machines on the Cloud
     */
    public int getCurrentVMCount() {
        int total = 0;
        for (CloudProvider cp : providers.values()) {
            total += cp.getCurrentVMCount();
        }
        return total;
    }

    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        // Current state
        sb.append(prefix).append("CLOUD = [").append("\n");
        sb.append(prefix).append("\t").append("CURRENT_STATE = [").append("\n");
        for (CloudProvider cp : providers.values()) {
            sb.append(cp.getCurrentState(prefix + "\t" + "\t"));
        }
        sb.append(prefix).append("\t").append("]").append("\n");

        // Pending requests
        sb.append(prefix).append("\t").append("PENDING_REQUESTS = [").append("\n");
        for (CloudProvider cp : providers.values()) {
            for (ResourceCreationRequest rcr : cp.getPendingRequests()) {
                Map<CloudInstanceTypeDescription, int[]> composition = rcr.getRequested().getTypeComposition();
                // REQUEST ARE COMPOSED OF A SINGLE INSTANCE TYPE
                for (CloudInstanceTypeDescription citd : composition.keySet()) {
                    sb.append(prefix).append("\t").append("\t").append("REQUEST = ").append(citd.getName()).append("\n");
                }
            }
        }
        sb.append(prefix).append("\t").append("]").append("\n");
        sb.append(prefix).append("]");

        return sb.toString();
    }
}
