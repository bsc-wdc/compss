package integratedtoolkit.util;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.CloudProvider;
import integratedtoolkit.ITConstants;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.types.CloudImageDescription;
import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.MethodResourceDescription;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The CloudManager class is an utility to manage all the cloud interactions and hide the details of each provider.
 */
public class CloudManager {

    private static final String CONNECTORS_REL_PATH = File.separator + "Runtime" + File.separator + "connectors" + File.separator;

    private static final String WARN_NO_IT_HOME = "WARN: IT_HOME not defined, no default connectors loaded";
    private static final String WARN_NO_IT_HOME_RESOURCES = "WARN_MSG = [IT_HOME NOT DEFINED, NO DEFAULT CONNECTORS LOADED]";
    private static final String WARN_NO_CONNECTORS_FOLDER = "WARN: Connectors folder not defined, no default connectors loaded";
    private static final String WARN_NO_CONNECTORS_FOLDER_RESOURCES = "WARN_MSG = [CONNECTORS FOLDER NOT DEFINED, NO DEFAULT CONNECTORS LOADED]";
    private static final String WARN_NO_RESOURCE_MATCHES = "WARN: No resource matches the constraints";
    private static final String WARN_CANNOT_TURN_ON = "WARN: Connector cannot turn on resource";
    private static final String WARN_EXCEPTION_TURN_ON = "WARN: Connector exception on turn on resource";

    private static boolean useCloud;
    private static int initialVMs = 0;
    private static int minVMs = 0;
    private static int maxVMs = -1;

    /**
     * Relation between a Cloud provider name and its representation
     */
    private static HashMap<String, CloudProvider> providers;
    /**
     * Relation between a resource name and the representation of the Cloud provider that support it
     */
    private static HashMap<String, CloudProvider> VM2Provider;

    private static final LinkedList<ResourceCreationRequest> pendingRequests = new LinkedList<>();
    private static int[] pendingCoreCount = new int[CoreManager.getCoreCount()];

    private static final Logger runtimeLogger = LogManager.getLogger(Loggers.CM_COMP);
    private static final Logger resourcesLogger = LogManager.getLogger(Loggers.RESOURCES);


    /**
     * Initializes the internal data structures
     *
     */
    public static void initialize() {
        runtimeLogger.info("Initializing Cloud Manager");
        useCloud = false;
        providers = new HashMap<>();
        VM2Provider = new HashMap<>();

        loadRuntimeConnectorJars();
    }

    /**
     * Configures the runtime to use the Cloud to adapt the resource pool
     *
     * @param useCloud
     *            true if enabled
     */
    public static void setUseCloud(boolean useCloud) {
        CloudManager.useCloud = useCloud;
    }

    public static int getInitialVMs() {
        return initialVMs;
    }

    public static void setInitialVMs(int initialVMs) {
        if (initialVMs > 0) {
            CloudManager.initialVMs = initialVMs;
        }
    }

    public static int getMinVMs() {
        return minVMs;
    }

    public static void setMinVMs(int minVMs) {
        if (minVMs > 0) {
            CloudManager.minVMs = minVMs;
        }
    }

    public static int getMaxVMs() {
        return maxVMs;
    }

    public static void setMaxVMs(int maxVMs) {
        CloudManager.maxVMs = maxVMs;
    }

    /**
     * Check if Cloud is used to dynamically adapt the resource pool
     *
     * @return true if it is used
     */
    public static boolean isUseCloud() {
        return useCloud;
    }

    /**
     * Adds a new Provider to the management
     *
     * @param name
     *            Identifier of that cloud provider
     * @param connectorPath
     *            Package and class name of the connector required to interact with the provider
     * @param limitOfVMs
     *            Max amount of VMs that can be running at the same time for that Cloud provider
     * @param connectorProperties
     *            Properties to configure the connector
     * @throws Exception
     *             Loading the connector by reflection
     */
    public static void newCloudProvider(String providerName, Integer limitOfVMs, String connectorJarPath, String connectorMainClass,
            HashMap<String, String> connectorProperties) throws ConnectorException {

        CloudProvider cp = new CloudProvider(providerName, limitOfVMs, connectorJarPath, connectorMainClass, connectorProperties);
        providers.put(providerName, cp);
    }

    /**
     * Adds an image description to a Cloud Provider
     *
     * @param providerName
     *            Identifier of the Cloud provider
     * @param cid
     *            Description of the features offered by that image
     * @throws Exception
     *             the cloud provider does not exist
     */
    public static void addImageToProvider(String providerName, CloudImageDescription cid) throws Exception {

        CloudProvider cp = providers.get(providerName);
        if (cp == null) {
            throw new Exception("Inexistent Cloud Provider " + providerName);
        }
        cp.addCloudImage(cid);
    }

    /**
     * Adds an instance type description to a Cloud Provider
     *
     * @param providerName
     *            Identifier of the Cloud provider
     * @param rd
     *            Description of the features offered by that instance type
     * @throws Exception
     *             the cloud provider does not exist
     */
    public static void addInstanceTypeToProvider(String providerName, CloudMethodResourceDescription rd) throws Exception {
        CloudProvider cp = providers.get(providerName);
        if (cp == null) {
            throw new Exception("Inexistent Cloud Provider " + providerName);
        }
        cp.addInstanceType(rd);
    }

    public static void newCoreElementsDetected(LinkedList<Integer> newCores) {
        pendingCoreCount = new int[CoreManager.getCoreCount()];
        for (ResourceCreationRequest rcr : pendingRequests) {
            int[][] reqCounts = rcr.requestedSimultaneousTaskCount();
            for (int coreId = 0; coreId < reqCounts.length; coreId++) {
                int coreSlots = 0;
                for (int implId = 0; implId < reqCounts[coreId].length; implId++) {
                    coreSlots = Math.max(coreSlots, reqCounts[coreId][implId]);
                }
                pendingCoreCount[coreId] += coreSlots;
            }
        }
        for (CloudProvider cp : providers.values()) {
            cp.newCoreElementsDetected(newCores);
        }
    }

    /**
     * *************************************************************
     * ************************************************************* 
     * ************** RESOURCE REQUESTS MANAGEMENT ***************** 
     * *************************************************************
     * *************************************************************
     */
    /**
     * Queries the creation requests pending to be served
     *
     * @return Returns all the pending creation requests
     */
    public static LinkedList<ResourceCreationRequest> getPendingRequests() {
        return pendingRequests;
    }

    /**
     * Queries the amount of tasks that will be able to run simulataneously once all the VMs have been created
     *
     * @return Returns all the pending creation requests
     */
    public static int[] getPendingCoreCounts() {
        return pendingCoreCount;
    }

    /**
     * Asks for the described resources to a Cloud provider. The CloudManager checks the best resource that each
     * provider can offer. Then it picks one of them and it constructs a resourceRequest describing the resource and
     * which cores can be executed on it. This ResourceRequest will be used to ask for that resource creation to the
     * Cloud Provider and returned if the application is accepted.
     *
     * @param requirements
     *            description of the resource expected to receive
     * @param contained
     *            {@literal true} if we want the request to ask for a resource contained in the description; else, the
     *            result contains the passed in description.
     * @return Description of the ResourceRequest sent to the CloudProvider. {@literal Null} if any of the Cloud
     *         Providers can offer a resource like the requested one.
     */
    public static ResourceCreationRequest askForResources(MethodResourceDescription requirements, boolean contained) {
        return askForResources(1, requirements, contained);
    }

    /**
     * The CloudManager ask for resources that can execute certain amount of cores at the same time. It checks the best
     * resource that each provider can offer to execute that amount of cores and picks one of them. It constructs a
     * resourceRequest describing the resource and which cores can be executed on it. This ResourceRequest will be used
     * to ask for that resource creation to the Cloud Provider and returned if the application is accepted.
     *
     * @param amount
     *            amount of slots
     * @param requirements
     *            features of the resource
     * @param contained
     *            {@literal true} if we want the request to ask for a resource contained in the description; else, the
     *            result contains the passed in description.
     * @return
     */
    public static ResourceCreationRequest askForResources(Integer amount, MethodResourceDescription requirements, boolean contained) {
        // Search best resource
        CloudProvider bestProvider = null;
        CloudMethodResourceDescription bestConstraints = null;
        Float bestValue = Float.MAX_VALUE;
        for (CloudProvider cp : providers.values()) {
            CloudMethodResourceDescription rc = cp.getBestIncrease(amount, requirements, contained);
            if (rc != null && rc.getValue() < bestValue) {
                bestProvider = cp;
                bestConstraints = rc;
                bestValue = rc.getValue();
            }
        }
        if (bestConstraints == null) {
            runtimeLogger.warn(WARN_NO_RESOURCE_MATCHES);
            return null;
        }

        // Code only executed if a resource fits the constraints
        int coreCount = CoreManager.getCoreCount();
        int[][] simultaneousCounts = bestProvider.getSimultaneousImpls(bestConstraints.getType());
        if (simultaneousCounts == null) {
            simultaneousCounts = new int[coreCount][];
            for (int coreId = 0; coreId < coreCount; coreId++) {
                Implementation<?>[] impls = CoreManager.getCoreImplementations(coreId);
                simultaneousCounts[coreId] = new int[impls.length];
                for (int implId = 0; implId < impls.length; ++implId) {
                    if (impls[implId].getTaskType() == TaskType.METHOD) {
                        MethodResourceDescription description = (MethodResourceDescription) impls[implId].getRequirements();
                        if (description != null) {
                            Integer into = bestConstraints.canHostSimultaneously(description);
                            simultaneousCounts[coreId][implId] = into;
                        }
                    }
                }
            }
        }

        runtimeLogger.debug("Asking for resource creation");
        ResourceCreationRequest rcr = new ResourceCreationRequest(bestConstraints, simultaneousCounts, bestProvider.getName());

        try {
            if (bestProvider.turnON(rcr)) {
                pendingRequests.add(rcr);
                int[][] reqCounts = rcr.requestedSimultaneousTaskCount();
                for (int coreId = 0; coreId < reqCounts.length; coreId++) {
                    int coreSlots = 0;
                    for (int implId = 0; implId < reqCounts[coreId].length; implId++) {
                        coreSlots = Math.max(coreSlots, reqCounts[coreId][implId]);
                    }
                    pendingCoreCount[coreId] += coreSlots;
                }
                return rcr;
            } else {
                runtimeLogger.warn(WARN_CANNOT_TURN_ON);
                return null;
            }
        } catch (Exception e) {
            runtimeLogger.warn(WARN_EXCEPTION_TURN_ON, e);
            return null;
        }
    }

    public static void confirmedRequest(ResourceCreationRequest rcr, CloudMethodWorker r) {
        pendingRequests.remove(rcr);
        int[][] reqCounts = rcr.requestedSimultaneousTaskCount();
        for (int coreId = 0; coreId < reqCounts.length; coreId++) {
            int coreSlots = 0;
            for (int implId = 0; implId < reqCounts[coreId].length; implId++) {
                coreSlots = Math.max(coreSlots, reqCounts[coreId][implId]);
            }
            pendingCoreCount[coreId] -= coreSlots;
        }
        String provider = rcr.getProvider();
        CloudProvider cp = providers.get(provider);
        String vmName = r.getName();
        VM2Provider.put(vmName, cp);
        cp.createdVM(vmName, (CloudMethodResourceDescription) r.getDescription());
    }

    public static void refusedRequest(ResourceCreationRequest rcr) {
        pendingRequests.remove(rcr);
        int[][] reqCounts = rcr.requestedSimultaneousTaskCount();
        for (int coreId = 0; coreId < reqCounts.length; coreId++) {
            int coreSlots = 0;
            for (int implId = 0; implId < reqCounts[coreId].length; implId++) {
                coreSlots = Math.max(coreSlots, reqCounts[coreId][implId]);
            }
            pendingCoreCount[coreId] -= coreSlots;
        }
        CloudProvider cp = providers.get(rcr.getProvider());
        cp.refusedWorker(rcr.getRequested());
    }

    /**
     * Given a set of resources, it checks every possible modification of the resource and returns the one that better
     * fits with the destruction recommendations.
     *
     * The decision-making algorithm tries to minimize the number of affected CE that weren't recommended to be
     * modified, minimize the number of slots that weren't requested to be destroyed and maximize the number of slots
     * that can be removed and they were requested for.
     *
     * @param resourceSet
     *            set of resources
     * @param destroyRecommendations
     *            number of slots to be removed for each CE
     * @return an object array defining the best solution. 0-> (Resource) selected Resource. 1-> (int[]) record of the
     *         #CE with removed slots and that they shouldn't be modified, #slots that will be destroyed and they
     *         weren't recommended, #slots that will be removed and they were asked to be. 2->(int[]) #slots to be
     *         removed by each CE. 3->(ResourceDescription) description of the resource to be destroyed.
     *
     *
     */
    public static Object[] getBestDestruction(Collection<CloudMethodWorker> resourceSet, float[] destroyRecommendations) {
        CloudProvider cp;
        float[] bestRecord = new float[3];
        bestRecord[0] = Float.MAX_VALUE;
        bestRecord[1] = Float.MAX_VALUE;
        bestRecord[2] = Float.MIN_VALUE;
        Resource bestResource = null;
        CloudProvider bestCP = null;
        String bestType = null;
        CloudMethodResourceDescription bestRD = null;

        for (CloudMethodWorker res : resourceSet) {
            cp = VM2Provider.get(res.getName());
            if (cp == null) { // it's not a cloud machine
                continue;
            }
            HashMap<String, Object[]> typeToPoints = cp.getPossibleReductions(res, destroyRecommendations);

            for (Entry<String, Object[]> destruction : typeToPoints.entrySet()) {
                String typeName = destruction.getKey();
                Object[] description = destruction.getValue();
                float[] values = (float[]) description[0];
                CloudMethodResourceDescription rd = (CloudMethodResourceDescription) description[1];
                if (bestRecord[0] == values[0]) {
                    if (bestRecord[1] == values[1]) {
                        if (bestRecord[2] < values[2]) {
                            bestRecord = values;
                            bestResource = res;
                            bestType = typeName;
                            bestCP = cp;
                            bestRD = rd;
                        }
                    } else {
                        if (bestRecord[1] > values[1]) {
                            bestRecord = values;
                            bestResource = res;
                            bestType = typeName;
                            bestCP = cp;
                            bestRD = rd;
                        }
                    }
                } else {
                    if (bestRecord[0] > values[0]) {
                        bestRecord = values;
                        bestResource = res;
                        bestType = typeName;
                        bestCP = cp;
                        bestRD = rd;
                    }
                }
            }
        }
        if (bestResource != null) {
            Object[] ret = new Object[4];
            ret[0] = bestResource;
            ret[1] = bestRecord;
            ret[2] = bestCP.getSimultaneousImpls(bestType);
            ret[3] = bestRD;
            return ret;
        } else {
            return null;
        }
    }

    public static void destroyResources(CloudMethodWorker res, CloudMethodResourceDescription reduction) {
        runtimeLogger.debug("Destroying resources for reduction");
        CloudProvider cp = VM2Provider.get(res.getName());
        cp.turnOff(res, reduction);
    }

    /**
     * CloudManager terminates all the resources obtained from any provider
     *
     * @throws ConnectorException
     */
    public static void terminateALL() throws ConnectorException {
        runtimeLogger.debug("Terminate ALL resources");
        if (providers != null) {
            for (Entry<String, CloudProvider> vm : providers.entrySet()) {
                CloudProvider cp = vm.getValue();
                cp.terminateAll();
            }
            VM2Provider.clear();
        }
    }

    /**
     * Computes the cost per hour of the whole cloud resource pool
     *
     * @return the cost per hour of the whole pool
     */
    public static float currentCostPerHour() {
        float total = 0;
        for (CloudProvider cp : providers.values()) {
            total += cp.getCurrentCostPerHour();
        }
        return total;
    }

    /**
     * The CloudManager notifies to all the connectors the end of generation of new tasks
     */
    public static void stopReached() {
        for (CloudProvider cp : providers.values()) {
            cp.stopReached();
        }
    }

    /**
     * The CloudManager computes the accumulated cost of the execution
     *
     * @return cost of the whole execution
     */
    public static float getTotalCost() {
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
    public static long getNextCreationTime() throws Exception {
        long total = 0;
        for (CloudProvider cp : providers.values()) {
            total = Math.max(total, cp.getNextCreationTime());
        }
        return total;
    }

    public static long getTimeSlot() throws Exception {
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
    public static int getCurrentVMCount() {
        int total = 0;
        for (CloudProvider cp : providers.values()) {
            total += cp.getCurrentVMCount();
        }
        return total;
    }

    public static String getCurrentState(String prefix) {
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
        for (ResourceCreationRequest rcr : pendingRequests) {
            sb.append(prefix).append("\t").append("\t").append("REQUEST = ").append(rcr.getRequested().getType()).append("\n");
        }
        sb.append(prefix).append("\t").append("]").append("\n");
        sb.append(prefix).append("]");

        return sb.toString();
    }

    public static CloudProvider getProvider(String name) {
        if (providers.containsKey(name)) {
            return providers.get(name);
        }
        return null;
    }

    private static void loadRuntimeConnectorJars() {
        runtimeLogger.debug("Loading runtime connectors to classpath...");

        String itHome = System.getenv(ITConstants.IT_HOME);
        if (itHome == null || itHome.isEmpty()) {
            resourcesLogger.warn(WARN_NO_IT_HOME_RESOURCES);
            runtimeLogger.warn(WARN_NO_IT_HOME);
            return;
        }

        String connPath = itHome + CONNECTORS_REL_PATH;
        try {
            Classpath.loadPath(connPath, runtimeLogger);
        } catch (FileNotFoundException fnfe) {
            ErrorManager.warn("Connector jar " + connPath + " not found.");
            resourcesLogger.warn(WARN_NO_CONNECTORS_FOLDER_RESOURCES);
            runtimeLogger.warn(WARN_NO_CONNECTORS_FOLDER);
        }
    }

    /*
     * private static class Ender extends Thread {
     * 
     * public void run() { for (CloudProvider cp : providers.values()) { logger.debug("Terminating all at ender");
     * cp.terminateAll(); } } }
     */
}
