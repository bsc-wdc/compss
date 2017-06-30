package integratedtoolkit.util;

import integratedtoolkit.ITConstants;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.ResourceUser;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.exceptions.InitNodeException;
import integratedtoolkit.exceptions.NoResourceAvailableException;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.CloudProvider;
import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.project.exceptions.ProjectFileValidationException;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.Resource.Type;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.MethodWorker;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.types.resources.exceptions.ResourcesFileValidationException;
import integratedtoolkit.types.resources.updates.PendingReduction;
import integratedtoolkit.types.resources.updates.PerformedIncrease;
import integratedtoolkit.types.resources.updates.ResourceUpdate;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The ResourceManager class is an utility to manage all the resources available
 * for the cores execution. It keeps information about the features of each
 * resource and is used as an endpoint to discover which resources can run a
 * core in a certain moment, the total and the available number of slots.
 *
 */
public class ResourceManager {

    // File Locations
    private static final String RESOURCES_XML = System.getProperty(ITConstants.IT_RES_FILE);
    private static final String RESOURCES_XSD = System.getProperty(ITConstants.IT_RES_SCHEMA);
    private static final String PROJECT_XML = System.getProperty(ITConstants.IT_PROJ_FILE);
    private static final String PROJECT_XSD = System.getProperty(ITConstants.IT_PROJ_SCHEMA);

    // Error messages
    private static final String ERROR_RESOURCES_XML = "ERROR: Cannot parse resources.xml file";
    private static final String ERROR_PROJECT_XML = "ERROR: Cannot parse project.xml file";
    private static final String ERROR_NO_RES = "ERROR: No computational resource available (ComputeNode, service or CloudProvider)";
    protected static final String ERROR_UNKNOWN_HOST = "ERROR: Cannot determine the IP address of the local host";
    private static final String DEL_VM_ERR = "ERROR: Canot delete VMs";

    // Information about resources
    private static CloudManager cloudManager;
    private static WorkerPool pool;
    private static int[] poolCoreMaxConcurrentTasks;
    private static ResourceUser resourceUser;

    // Loggers
    private static final Logger resourcesLogger = LogManager.getLogger(Loggers.RESOURCES);
    private static final Logger runtimeLogger = LogManager.getLogger(Loggers.RM_COMP);


    /*
     * ******************************************************************** INITIALIZER METHOD
     ********************************************************************/
    /**
     * Constructs a new ResourceManager using the Resources xml file content.
     * First of all, an empty resource pool is created and the Cloud Manager is
     * initialized without any providers. Secondly the resource file is
     * validated and parsed and the toplevel xml nodes are processed in
     * different ways depending on its type: - Resource: a new Physical resource
     * is added to the resource pool with the same id as its Name attribute and
     * as many slots as indicated in the project file. If it has 0 slots or it
     * is not on the project xml, the resource is not included.
     *
     * - Service: a new Physical resource is added to the resource pool with the
     * same id as its wsdl attribute and as many slots as indicated in the
     * project file. If it has 0 slots or it is not on the project xml, the
     * resource is not included.
     *
     * - Cloud Provider: if there is any CloudProvider in the project file with
     * the same name, a new Cloud Provider is added to the cloudManager with its
     * name attribute value as identifier. The cloudManager is configured as
     * described between the project xml and the resources file. From the
     * resource file it gets the properties which describe how to connect with
     * it: the connector path, the endpoint, ... Other properties required to
     * manage the resources are specified on the project file: i.e. the maximum
     * amount of resource deployed on that provider. Some configurations depend
     * on both files. One of them is the list of usable images. The images
     * offered by the cloud provider are on a list on the resources file, where
     * there are specified the name and the software description of that image.
     * On the project file there is a description of how the resources created
     * with that image must be used: username, working directory,... Only the
     * images that have been described in both files are added to the
     * cloudManager
     *
     * @param resUser object to notify resource changes
     *
     */
    public static void load(ResourceUser resUser) {
        // Store the ResourceUser
        resourceUser = resUser;

        // Initialize Worker pool
        pool = new WorkerPool();
        poolCoreMaxConcurrentTasks = new int[CoreManager.getCoreCount()];

        // Initialize the Cloud structures (even if we won't need them)
        cloudManager = new CloudManager();

        // Load Resources and Project file and cross validate them
        try {
            ResourceLoader.load(RESOURCES_XML, RESOURCES_XSD, PROJECT_XML, PROJECT_XSD);
        } catch (ResourcesFileValidationException e) {
            ErrorManager.fatal(ERROR_RESOURCES_XML, e);
        } catch (ProjectFileValidationException e) {
            ErrorManager.fatal(ERROR_PROJECT_XML, e);
        } catch (NoResourceAvailableException e) {
            ErrorManager.fatal(ERROR_NO_RES, e);
        }

    }

    /**
     * Reinitializes the ResourceManager
     *
     * @param resUser obect to notify resource changes
     */
    public static void clear(ResourceUser resUser) {
        resourceUser = resUser;
        pool = new WorkerPool();
        poolCoreMaxConcurrentTasks = new int[CoreManager.getCoreCount()];
        cloudManager = new CloudManager();
    }

    /**
     * Reconfigures the master node adding its shared disks
     *
     * @param sharedDisks Shared Disk descriptions (diskName->mountpoint)
     */
    public static void updateMasterConfiguration(HashMap<String, String> sharedDisks) {
        Comm.getAppHost().updateSharedDisk(sharedDisks);
        try {
            Comm.getAppHost().start();
        } catch (InitNodeException e) {
            ErrorManager.error("Error updating master configuration", e);

        }
    }

    /**
     * Stops all the nodes within the pool
     *
     */
    public static void stopNodes() {
        // Log resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [Stopping all workers]");
        runtimeLogger.info("Stopping all workers");

        // Stop all Cloud VM
        if (cloudManager.isUseCloud()) {
            // Transfer files
            resourcesLogger.debug("DEBUG_MSG = [Terminating cloud instances...]");
            try {
                cloudManager.terminateALL();
                resourcesLogger.info("TOTAL_EXEC_COST = " + cloudManager.getTotalCost());
            } catch (Exception e) {
                resourcesLogger.error(ITConstants.TS + ": " + DEL_VM_ERR, e);
            }
            resourcesLogger.info("INFO_MSG = [Cloud instances terminated]");
        }

        // Stop static workers - Order its destruction from runtime and transfer files
        // Physical worker (COMM) is erased now - because of cloud
        if (pool != null && !pool.getStaticResources().isEmpty()) {
            resourcesLogger.debug("DEBUG_MSG = [Resource Manager retrieving data from workers...]");
            for (Worker<? extends WorkerResourceDescription> r : pool.getStaticResources()) {
                r.retrieveData(false);
            }
            Semaphore sem = new Semaphore(0);
            ShutdownListener sl = new ShutdownListener(sem);
            resourcesLogger.debug("DEBUG_MSG = [Resource Manager stopping workers...]");
            for (Worker<? extends WorkerResourceDescription> r : pool.getStaticResources()) {
                r.stop(sl);
            }

            resourcesLogger.debug("DEBUG_MSG = [Waiting for workers to shutdown...]");
            sl.enable();

            try {
                sem.acquire();
            } catch (Exception e) {
                resourcesLogger.error("ERROR_MSG= [ERROR: Exception raised on worker shutdown]");
            }
            resourcesLogger.info("INFO_MSG = [Workers stopped]");
        }
    }

    /*
     ********************************************************************
     ********************************************************************
     *************************  POOL METHODS ****************************
     ********************************************************************
     ********************************************************************
     */
    /**
     * Returns a worker instance with the given name @name
     *
     * @param name
     * @return
     */
    public static Worker<? extends WorkerResourceDescription> getWorker(String name) {
        return pool.getResource(name);
    }

    /**
     * Return a list of all the resources
     *
     * @return list of all the resources
     */
    public static LinkedList<Worker<? extends WorkerResourceDescription>> getAllWorkers() {
        return pool.findAllResources();
    }

    /**
     * Returns the number of available workers
     *
     * @return the number of available workers
     */
    public static int getTotalNumberOfWorkers() {
        return pool.findAllResources().size();
    }

    /**
     * Initializes a new Method Worker
     *
     * @param name
     * @param rd
     * @param sharedDisks
     * @param mc
     *
     * @return the created method Worker
     */
    private static MethodWorker newMethodWorker(String name, MethodResourceDescription rd, HashMap<String, String> sharedDisks, MethodConfiguration mc) {
        // Compute task count
        int taskCount;
        int limitOfTasks = mc.getLimitOfTasks();
        int computingUnits = rd.getTotalCPUComputingUnits();

        if (limitOfTasks < 0 && computingUnits < 0) {
            taskCount = 0;
        } else {
            taskCount = Math.max(limitOfTasks, computingUnits);
        }

        mc.setLimitOfTasks(taskCount);

        limitOfTasks = mc.getLimitOfGPUTasks();
        computingUnits = rd.getTotalGPUComputingUnits();

        if (limitOfTasks < 0 && computingUnits < 0) {
            taskCount = 0;
        } else {
            taskCount = Math.max(limitOfTasks, computingUnits);
        }
        mc.setLimitOfGPUTasks(taskCount);

        limitOfTasks = mc.getLimitOfFPGATasks();
        computingUnits = rd.getTotalFPGAComputingUnits();

        if (limitOfTasks < 0 && computingUnits < 0) {
            taskCount = 0;
        } else {
            taskCount = Math.max(limitOfTasks, computingUnits);
        }
        mc.setLimitOfFPGATasks(taskCount);

        limitOfTasks = mc.getLimitOfOTHERSTasks();
        computingUnits = rd.getTotalOTHERComputingUnits();

        if (limitOfTasks < 0 && computingUnits < 0) {
            taskCount = 0;
        } else {
            taskCount = Math.max(limitOfTasks, computingUnits);
        }
        mc.setLimitOfOTHERSTasks(taskCount);

        MethodWorker newResource = new MethodWorker(name, rd, mc, sharedDisks);
        addStaticResource(newResource);
        return newResource;
    }

    /**
     *
     * @param <T>
     * @param worker
     */
    public static <T extends WorkerResourceDescription> void addStaticResource(Worker<T> worker) {
        synchronized (pool) {
            worker.updatedFeatures();
            pool.addStaticResource(worker);
            pool.defineCriticalSet();

            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; ++coreId) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
        }
        // Log new resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [New resource available in the pool. Name = " + worker.getName() + "]");
        runtimeLogger.info("New " + ((worker.getType() == Type.SERVICE) ? "service" : "computeNode") + " available in the pool. Name = "
                + worker.getName());
    }

    public static void removeWorker(Worker<? extends WorkerResourceDescription> r) {
        pool.delete(r);
        int[] maxTaskCount = r.getSimultaneousTasks();
        for (int coreId = 0; coreId < maxTaskCount.length; ++coreId) {
            poolCoreMaxConcurrentTasks[coreId] -= maxTaskCount[coreId];
        }
    }

    /**
     * Updates the coreElement information
     *
     * @param updatedCores
     */
    public static void coreElementUpdates(List<Integer> updatedCores) {
        synchronized (pool) {
            pool.coreElementUpdates(updatedCores);
            cloudManager.newCoreElementsDetected(updatedCores);
        }
    }

    /*
     ********************************************************************
     ********************************************************************
     ************************** CLOUD METHODS ***************************
     ********************************************************************
     ********************************************************************
     */
    /**
     * Sets the boundaries on the cloud elasticity
     *
     * @param minVMs lower number of VMs allowed
     * @param initialVMs initial number of VMs
     * @param maxVMs higher number of VMs allowed
     */
    public static void setCloudVMsBoundaries(Integer minVMs, Integer initialVMs, Integer maxVMs) {
        cloudManager.setInitialVMs(initialVMs);
        cloudManager.setMinVMs(minVMs);
        cloudManager.setMaxVMs(maxVMs);
    }

    /**
     * Adds a new Provider to the Cloud section management (and enables the
     * cloud usage)
     *
     * @param providerName
     * @param limitOfVMs
     * @param runtimeConnectorClass
     * @param connectorJarPath
     * @param connectorMainClass
     * @param connectorProperties
     * @return
     * @throws integratedtoolkit.connectors.ConnectorException
     */
    public static CloudProvider registerCloudProvider(String providerName, Integer limitOfVMs, String runtimeConnectorClass, String connectorJarPath, String connectorMainClass,
            HashMap<String, String> connectorProperties) throws ConnectorException {
        return cloudManager.registerCloudProvider(providerName, limitOfVMs, runtimeConnectorClass, connectorJarPath, connectorMainClass, connectorProperties);
    }

    /**
     * Adds a cloud worker
     *
     * @param origin
     * @param worker
     * @param granted
     */
    public static void addCloudWorker(ResourceCreationRequest origin, CloudMethodWorker worker, CloudMethodResourceDescription granted) {
        synchronized (pool) {
            CloudProvider cloudProvider = origin.getProvider();
            cloudProvider.confirmedCreation(origin, worker, granted);
            worker.updatedFeatures();
            pool.addDynamicResource(worker);
            pool.defineCriticalSet();

            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
        }
        ResourceUpdate<MethodResourceDescription> ru = new PerformedIncrease<>(worker.getDescription());
        resourceUser.updatedResource(worker, ru);

        // Log new resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [New resource available in the pool. Name = " + worker.getName() + "]");
        runtimeLogger.info("New resource available in the pool. Name = " + worker.getName());
    }

    /**
     * Increases the capabilities of a given cloud worker
     *
     * @param origin
     * @param worker
     * @param extension
     */
    public static void increasedCloudWorker(ResourceCreationRequest origin, CloudMethodWorker worker, CloudMethodResourceDescription extension) {

        synchronized (pool) {
            CloudProvider cloudProvider = origin.getProvider();
            cloudProvider.confirmedCreation(origin, worker, extension);
            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] -= maxTaskCount[coreId];
            }
            worker.increaseFeatures(extension);

            maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
            pool.defineCriticalSet();
        }
        ResourceUpdate<MethodResourceDescription> ru = new PerformedIncrease<>(extension);
        resourceUser.updatedResource(worker, ru);

        // Log modified resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [Resource modified. Name = " + worker.getName() + "]");
        runtimeLogger.info("Resource modified. Name = " + worker.getName());
    }

    /**
     * Decreases the capabilities of a given cloud worker
     *
     * @param worker
     * @param reduction
     */
    public static void reduceCloudWorker(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        ResourceUpdate<MethodResourceDescription> modification = new PendingReduction<>(reduction);
        resourceUser.updatedResource(worker, modification);
    }

    /**
     * Decreases the capabilities of a given cloud worker
     *
     * @param <R>
     * @param worker
     * @param modification
     */
    @SuppressWarnings("unchecked")
    public static <R extends WorkerResourceDescription> void reduceResource(CloudMethodWorker worker, PendingReduction<R> modification) {
        synchronized (pool) {
            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] -= maxTaskCount[coreId];
            }
            worker.applyReduction((PendingReduction<MethodResourceDescription>) modification);
            maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
            pool.defineCriticalSet();
        }

        // Log new resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [Resource removed from the pool. Name = " + worker.getName() + "]");
        runtimeLogger.info("Resource removed from the pool. Name = " + worker.getName());
    }

    public static void terminateResource(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        if (worker.getDescription().getTypeComposition().isEmpty()) {
            pool.delete(worker);
        }
        CloudProvider cp = worker.getProvider();
        cp.requestResourceReduction(worker, reduction);
    }

    /**
     * Returns whether the cloud is enabled or not
     *
     * @return
     */
    public static boolean useCloud() {
        return cloudManager.isUseCloud();
    }

    /**
     * Returns the mean creation time
     *
     * @return
     * @throws Exception
     */
    public static Long getCreationTime() throws Exception {
        try {
            return cloudManager.getNextCreationTime();
        } catch (ConnectorException e) {
            throw new Exception(e);
        }
    }

    /**
     * Computes the cost per hour of the whole cloud resource pool
     *
     * @return the cost per hour of the whole pool
     */
    public static float getCurrentCostPerHour() {
        return cloudManager.currentCostPerHour();
    }

    /**
     * The cloudManager computes the accumulated cost of the execution
     *
     * @return cost of the whole execution
     */
    public static float getTotalCost() {
        return cloudManager.getTotalCost();
    }

    /*
     * **********************************************************************************************************
     * GETTERS
     ***********************************************************************************************************/
    public static int getMaxCloudVMs() {
        return cloudManager.getMaxVMs();
    }

    public static int getInitialCloudVMs() {
        return cloudManager.getInitialVMs();
    }

    public static int getMinCloudVMs() {
        return cloudManager.getMinVMs();
    }

    public static int getCurrentVMCount() {
        return cloudManager.getCurrentVMCount();
    }

    public static long getNextCreationTime() throws Exception {
        return cloudManager.getNextCreationTime();
    }

    /**
     * Returns the total slots per per core
     *
     * @return
     */
    public static int[] getTotalSlots() {
        int[] counts = new int[CoreManager.getCoreCount()];
        int[] cloudCount = cloudManager.getPendingCoreCounts();
        synchronized (pool) {

            for (int i = 0; i < counts.length; i++) {
                counts[i] = poolCoreMaxConcurrentTasks[i] + cloudCount[i];
            }
        }
        return counts;
    }

    /**
     * Returns the available slots per core
     *
     * @return
     */
    public static int[] getAvailableSlots() {
        return poolCoreMaxConcurrentTasks;
    }

    /**
     * Returns the static resources available at the pool
     *
     * @return
     */
    public static Collection<Worker<? extends WorkerResourceDescription>> getStaticResources() {
        synchronized (pool) {
            return pool.getStaticResources();
        }
    }

    /**
     * Returns the dynamic resources available at the pool
     *
     * @return
     */
    public static LinkedList<CloudMethodWorker> getDynamicResources() {
        synchronized (pool) {
            return pool.getDynamicResources();
        }
    }

    /**
     * Returns the dynamic resources available at the pool that are in the
     * critical set
     *
     * @return
     */
    public static Collection<CloudMethodWorker> getCriticalDynamicResources() {
        synchronized (pool) {
            return pool.getCriticalResources();
        }
    }

    /**
     * Returns the dynamic resources available at the pool that are NOT in the
     * critical set
     *
     * @return
     */
    public static Collection<CloudMethodWorker> getNonCriticalDynamicResources() {
        synchronized (pool) {
            return pool.getNonCriticalResources();
        }
    }

    /**
     * Returns the dynamic resource with name = @name
     *
     * @param name
     * @return
     */
    public static CloudMethodWorker getDynamicResource(String name) {
        synchronized (pool) {
            return pool.getDynamicResource(name);
        }
    }

    public static Collection<CloudProvider> getAvailableCloudProviders() {
        return cloudManager.getProviders();
    }

    public static CloudProvider getCloudProvider(String name) {
        return cloudManager.getProvider(name);
    }

    public static LinkedList<ResourceCreationRequest> getPendingCreationRequests() {
        return cloudManager.getPendingRequests();
    }

    /*
     * ************************************************************************************************************
     * LOGGER METHODS
     **********************************************************************************************************
     */
    /**
     * Prints out the information about the pending requests
     *
     * @param prefix
     * @return
     */
    public static String getPendingRequestsMonitorData(String prefix) {
        StringBuilder sb = new StringBuilder();
        LinkedList<ResourceCreationRequest> rcr = cloudManager.getPendingRequests();
        for (ResourceCreationRequest r : rcr) {
            // TODO: Add more information (i.e. information per processor, memory type, etc.)
            sb.append(prefix).append("<Resource id=\"requested new VM\">").append("\n");
            sb.append(prefix).append("\t").append("<CPUComputingUnits>").append(0).append("</CPUComputingUnits>").append("\n");
            sb.append(prefix).append("\t").append("<GPUComputingUnits>").append(0).append("</GPUComputingUnits>").append("\n");
            sb.append(prefix).append("\t").append("<FPGAComputingUnits>").append(0).append("</FPGAComputingUnits>").append("\n");
            sb.append(prefix).append("\t").append("<OTHERComputingUnits>").append(0).append("</OTHERComputingUnits>").append("\n");
            sb.append(prefix).append("\t").append("<Memory>").append(0).append("</Memory>").append("\n");
            sb.append(prefix).append("\t").append("<Disk>").append(0).append("</Disk>").append("\n");
            sb.append(prefix).append("\t").append("<Provider>").append(r.getProvider()).append("</Provider>").append("\n");
            sb.append(prefix).append("\t").append("<Image>").append(r.getRequested().getImage().getImageName()).append("</Image>").append("\n");
            sb.append(prefix).append("\t").append("<Status>").append("Creating").append("</Status>").append("\n");
            sb.append(prefix).append("\t").append("<Tasks>").append("</Tasks>").append("\n");
            sb.append(prefix).append("</Resource>").append("\n");
        }
        return sb.toString();
    }

    /**
     * Prints out the resources state
     *
     */
    public static void printResourcesState() {
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        StringBuilder resourceState = new StringBuilder();

        resourceState.append("RESOURCES_INFO = [").append("\n");
        synchronized (pool) {
            for (Worker<? extends WorkerResourceDescription> resource : pool.findAllResources()) {
                resourceState.append("\t").append("RESOURCE = [").append("\n");
                resourceState.append("\t\t").append("NAME = ").append(resource.getName()).append("\n");
                resourceState.append("\t\t").append("TYPE = ").append(resource.getType().toString()).append("\n");
                if (resource.getType() == Type.SERVICE) {
                    resourceState.append("\t\t").append("CPUS = 0\n");
                    resourceState.append("\t\t").append("MEMORY = 0\n");
                } else {
                    MethodResourceDescription mrd = (MethodResourceDescription) resource.getDescription();
                    resourceState.append("\t\t").append("CPUS = ").append(mrd.getTotalCPUComputingUnits()).append("\n");
                    resourceState.append("\t\t").append("MEMORY = ").append(mrd.getMemorySize()).append("\n");
                }
                int[] coreSlots = resource.getSimultaneousTasks();
                resourceState.append("\t\t").append("CAN_RUN = [").append("\n");
                for (int i = 0; i < coreSlots.length; i++) {
                    resourceState.append("\t\t\t").append("CORE = [").append("\n");
                    resourceState.append("\t\t\t").append("\t").append("COREID = ").append(i).append("\n");
                    resourceState.append("\t\t\t").append("\t").append("NUM_SLOTS = ").append(coreSlots[i]).append("\n");
                    resourceState.append("\t\t\t").append("]").append("\n");
                }
                resourceState.append("\t\t").append("]").append("\n"); // End CAN_RUN
                resourceState.append("\t").append("]\n"); // End RESOURCE
            }
        }
        resourceState.append("]").append("\n"); // END RESOURCES_INFO

        resourceState.append("CLOUD_INFO = [").append("\n");

        if (cloudManager.isUseCloud()) {
            resourceState.append("\t").append("CURRENT_CLOUD_VM_COUNT = ").append(cloudManager.getCurrentVMCount()).append("\n");
            try {
                resourceState.append("\t").append("CREATION_TIME = ").append(Long.toString(cloudManager.getNextCreationTime())).append("\n");
            } catch (Exception ex) {
                resourceState.append("\t").append("CREATION_TIME = ").append(120000l).append("\n");
            }
            resourceState.append("\t").append("PENDING_RESOURCES = [").append("\n");
            for (ResourceCreationRequest rcr : cloudManager.getPendingRequests()) {
                resourceState.append("\t\t").append("RESOURCE = [").append("\n");
                CloudMethodResourceDescription cmrd = rcr.getRequested();
                resourceState.append("\t\t\t").append("NAME = ").append(cmrd.getName()).append("\n");
                resourceState.append("\t\t\t").append("TYPE = ").append(Type.WORKER.toString()).append("\n");
                resourceState.append("\t\t\t").append("CPUS = ").append(cmrd.getTotalCPUComputingUnits()).append("\n");
                resourceState.append("\t\t\t").append("MEMORY = ").append(cmrd.getMemorySize()).append("\n");
                resourceState.append("\t\t\t").append("CAN_RUN = [").append("\n");
                int[][] simTasks = rcr.requestedSimultaneousTaskCount();
                for (int coreId = 0; coreId < simTasks.length; coreId++) {
                    int coreSlots = 0;
                    for (int implId = 0; implId < simTasks[coreId].length; implId++) {
                        coreSlots = Math.max(coreSlots, simTasks[coreId][implId]);
                    }
                    resourceState.append("\t\t\t\t").append("CORE = [").append("\n");
                    resourceState.append("\t\t\t\t\t").append("COREID = ").append(coreId).append("\n");
                    resourceState.append("\t\t\t\t\t").append("NUM_SLOTS = ").append(coreSlots).append("\n");
                    resourceState.append("\t\t\t\t").append("]").append("\n");
                }
                resourceState.append("\t\t\t").append("]").append("\n"); // End CAN_RUN
                resourceState.append("\t\t").append("]").append("\n"); // End RESOURCE
            }
            resourceState.append("\t").append("]").append("\n"); // End PENDING_RESOURCES
        }

        resourceState.append("]"); // END CLOUD_INFO
        resourcesLogger.info(resourceState.toString());
    }

    /**
     * Returns the current state of the resources pool
     *
     * @param prefix
     * @return
     */
    public static String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("TIMESTAMP = ").append(String.valueOf(System.currentTimeMillis())).append("\n");
        sb.append(pool.getCurrentState(prefix)).append("\n");
        sb.append(cloudManager.getCurrentState(prefix));
        return sb.toString();
    }

}
