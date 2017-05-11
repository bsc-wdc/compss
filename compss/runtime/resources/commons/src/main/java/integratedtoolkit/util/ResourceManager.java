package integratedtoolkit.util;

import java.util.LinkedList;
import java.util.List;

import integratedtoolkit.ITConstants;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.types.project.exceptions.ProjectFileValidationException;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;
import integratedtoolkit.types.resources.configuration.ServiceConfiguration;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.types.resources.exceptions.ResourcesFileValidationException;
import integratedtoolkit.components.ResourceUser;
import integratedtoolkit.components.ResourceUser.WorkloadStatus;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.exceptions.InitNodeException;
import integratedtoolkit.exceptions.NoResourceAvailableException;
import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.ResourcesState;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.Resource.Type;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.MethodWorker;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ServiceResourceDescription;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.resources.ServiceWorker;

import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The ResourceManager class is an utility to manage all the resources available for the cores execution. It keeps
 * information about the features of each resource and is used as an endpoint to discover which resources can run a core
 * in a certain moment, the total and the available number of slots.
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
    private static WorkerPool pool;
    private static int[] poolCoreMaxConcurrentTasks;
    private static ResourceUser resourceUser;
    private static ResourceOptimizer ro;
    static int maxTasks = 0;

    // Loggers
    private static final Logger resourcesLogger = LogManager.getLogger(Loggers.RESOURCES);
    private static final Logger runtimeLogger = LogManager.getLogger(Loggers.RM_COMP);


    /*
     * ******************************************************************** INITIALIZER METHOD
     ********************************************************************/
    /**
     * Constructs a new ResourceManager using the Resources xml file content. First of all, an empty resource pool is
     * created and the Cloud Manager is initialized without any providers. Secondly the resource file is validated and
     * parsed and the toplevel xml nodes are processed in different ways depending on its type: - Resource: a new
     * Physical resource is added to the resource pool with the same id as its Name attribute and as many slots as
     * indicated in the project file. If it has 0 slots or it is not on the project xml, the resource is not included.
     *
     * - Service: a new Physical resource is added to the resource pool with the same id as its wsdl attribute and as
     * many slots as indicated in the project file. If it has 0 slots or it is not on the project xml, the resource is
     * not included.
     *
     * - Cloud Provider: if there is any CloudProvider in the project file with the same name, a new Cloud Provider is
     * added to the CloudManager with its name attribute value as identifier. The CloudManager is configured as
     * described between the project xml and the resources file. From the resource file it gets the properties which
     * describe how to connect with it: the connector path, the endpoint, ... Other properties required to manage the
     * resources are specified on the project file: i.e. the maximum amount of resource deployed on that provider. Some
     * configurations depend on both files. One of them is the list of usable images. The images offered by the cloud
     * provider are on a list on the resources file, where there are specified the name and the software description of
     * that image. On the project file there is a description of how the resources created with that image must be used:
     * username, working directory,... Only the images that have been described in both files are added to the
     * CloudManager
     *
     * @param resUser
     *            class to notify resource changes
     *
     */
    public static void load(ResourceUser resUser) {
        // Store the ResourceUser
        resourceUser = resUser;

        // Initialize Worker pool
        pool = new WorkerPool();
        poolCoreMaxConcurrentTasks = new int[CoreManager.getCoreCount()];

        // Initialize the Cloud structures (even if we won't need them)
        CloudManager.initialize();

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

        // Start ResourceOptimizer
        ro = new ResourceOptimizer(resourceUser);
        ro.setName("Resource Optimizer");
        ro.start();
    }

    /*
     * ******************************************************************** SHUTDOWN METHOD
     ********************************************************************/
    /**
     * Stops all the nodes within the pool
     *
     * @param status
     */
    public static void stopNodes(WorkloadStatus status) {
        // Log resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [Stopping all workers]");
        runtimeLogger.info("Stopping all workers");

        // Stop Resource Optimizer
        if (ro != null) {
            ro.shutdown(status);
        } else {
            runtimeLogger.info("Resource Optimizer was not initialized");
        }

        // Stop all Cloud VM
        if (CloudManager.isUseCloud()) {
            // Transfer files
            /*
             * Semaphore sem = new Semaphore(0); ShutdownListener sl = new ShutdownListener(sem);
             * resourcesLogger.debug("DEBUG_MSG = [Resource Manager stopping cloud workers...]"); for (Worker<?> r :
             * pool.getDynamicResources()) { // TODO: The worker is not really needed to be stopped because VM is going
             * to be erased. // However, the app-files and the tracing files MUST be transferred r.stop(false, sl); }
             * resourcesLogger.debug("DEBUG_MSG = [Waiting for cloud workers to shutdown...]"); sl.enable(); try {
             * sem.acquire(); } catch (Exception e) {
             * resourcesLogger.error("ERROR_MSG= [ERROR: Exception raised on cloud worker shutdown]"); }
             * resourcesLogger.info("INFO_MSG = [Cloud Workers stopped]");
             */
            resourcesLogger.debug("DEBUG_MSG = [Terminating cloud instances...]");
            try {
                CloudManager.terminateALL();
                resourcesLogger.info("TOTAL_EXEC_COST = " + CloudManager.getTotalCost());
            } catch (Exception e) {
                resourcesLogger.error(ITConstants.TS + ": " + DEL_VM_ERR, e);
            }
            resourcesLogger.info("INFO_MSG = [Cloud instances terminated]");
        }

        // Stop static workers - Order its destruction from runtime and transfer files
        // Physical worker (COMM) is erased now - because of cloud
        if (pool != null && !pool.getStaticResources().isEmpty()) {

            resourcesLogger.debug("DEBUG_MSG = [Resource Manager retrieving data from workers...]");
            for (Worker<?, ?> r : pool.getStaticResources()) {
                r.retrieveData(false);
            }
            Semaphore sem = new Semaphore(0);
            ShutdownListener sl = new ShutdownListener(sem);
            resourcesLogger.debug("DEBUG_MSG = [Resource Manager stopping workers...]");
            for (Worker<?, ?> r : pool.getStaticResources()) {
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
     * ******************************************************************** STATIC POOL METHODS
     ********************************************************************/
    /**
     * Returns a worker instance with the given name @name
     *
     * @param name
     * @return
     */
    public static Worker<?, ?> getWorker(String name) {
        return pool.getResource(name);
    }

    /**
     * Return a list of all the resources
     *
     * @return list of all the resources
     */
    public static LinkedList<Worker<?, ?>> getAllWorkers() {
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
     * Reconfigures the master node adding its shared disks
     *
     * @param sharedDisks
     *            Shared Disk descriptions (diskName->mountpoint)
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
     * Initializes a new Method Worker
     *
     * @param name
     * @param rd
     * @param sharedDisks
     * @param mc
     */
    public static void newMethodWorker(String name, MethodResourceDescription rd, HashMap<String, String> sharedDisks,
            MethodConfiguration mc) {
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
        maxTasks = maxTasks + newResource.getMaxTaskCount();
        addStaticResource(newResource);
    }

    /**
     * Initializes a new Service Worker. Returns true if the worker has been created, false otherwise
     *
     * @param wsdl
     * @param sd
     * @param sc
     */
    public static void newServiceWorker(String wsdl, ServiceResourceDescription sd, ServiceConfiguration sc) {
        ServiceWorker newResource = new ServiceWorker(wsdl, sd, sc);
        addStaticResource(newResource);
    }

    private static <T extends WorkerResourceDescription, I extends Implementation<T>> void addStaticResource(Worker<T, I> worker) {
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

    public static void removeWorker(Worker<?, ?> r) {
        pool.delete(r);
        maxTasks = maxTasks - r.getMaxTaskCount();
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
            CloudManager.newCoreElementsDetected(updatedCores);
        }
    }

    /*
     * ************************************************************************************************************
     * CLOUD METHODS
     **************************************************************************************************************/
    /**
     * Adds a cloud worker
     *
     * @param origin
     * @param worker
     */
    public static void addCloudWorker(ResourceCreationRequest origin, CloudMethodWorker worker) {
        synchronized (pool) {
            CloudManager.confirmedRequest(origin, worker);
            worker.updatedFeatures();
            maxTasks = maxTasks + worker.getMaxTaskCount();
            pool.addDynamicResource(worker);
            pool.defineCriticalSet();

            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
        }

        resourceUser.updatedResource((Resource) worker);

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
    public static void increasedCloudWorker(ResourceCreationRequest origin, CloudMethodWorker worker,
            CloudMethodResourceDescription extension) {

        synchronized (pool) {
            CloudManager.confirmedRequest(origin, worker);
            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] -= maxTaskCount[coreId];
            }
            maxTasks = maxTasks - worker.getMaxTaskCount();
            worker.increaseFeatures(extension);
            maxTasks = maxTasks + worker.getMaxTaskCount();

            maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
            pool.defineCriticalSet();
        }
        resourceUser.updatedResource(worker);

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
     * @return
     */
    public static Semaphore reduceCloudWorker(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        Semaphore sem;
        synchronized (pool) {
            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] -= maxTaskCount[coreId];
            }
            maxTasks = maxTasks - worker.getMaxTaskCount();
            sem = worker.reduceFeatures(reduction);
            maxTasks = maxTasks + worker.getMaxTaskCount();
            maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
            pool.defineCriticalSet();

            //
            resourceUser.updatedResource(worker);
        }
        // Log new resource
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info("INFO_MSG = [Resource removed from the pool. Name = " + worker.getName() + "]");
        runtimeLogger.info("Resource removed from the pool. Name = " + worker.getName());

        return sem;
    }

    /**
     * Returns whether the cloud is enabled or not
     *
     * @return
     */
    public static boolean useCloud() {
        return CloudManager.isUseCloud();
    }

    /**
     * Returns the mean creation time
     *
     * @return
     * @throws Exception
     */
    public static Long getCreationTime() throws Exception {
        try {
            return CloudManager.getNextCreationTime();
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
        return CloudManager.currentCostPerHour();
    }

    /**
     * The CloudManager computes the accumulated cost of the execution
     *
     * @return cost of the whole execution
     */
    public static float getTotalCost() {
        return CloudManager.getTotalCost();
    }

    /*
     * ******************************************************************** GETTERS
     ********************************************************************/
    /**
     * Returns the total slots per per core
     *
     * @return
     */
    public static int[] getTotalSlots() {
        int[] counts = new int[CoreManager.getCoreCount()];
        int[] cloudCount = CloudManager.getPendingCoreCounts();
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
    public static Collection<Worker<?, ?>> getStaticResources() {
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
     * Returns the dynamic resources available at the pool that are in the critical set
     *
     * @return
     */
    public static Collection<CloudMethodWorker> getCriticalDynamicResources() {
        synchronized (pool) {
            return pool.getCriticalResources();
        }
    }

    /**
     * Returns the dynamic resources available at the pool that are NOT in the critical set
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

    /**
     * Refuses a cloud creation request
     *
     * @param rcr
     */
    public static void refuseCloudRequest(ResourceCreationRequest rcr) {
        CloudManager.refusedRequest(rcr);
    }

    /*
     * ******************************************************************** LOGGER METHODS
     ********************************************************************/
    /**
     * Returns the resources state
     *
     * @return
     */
    public static ResourcesState getResourcesState() {
        ResourcesState state = new ResourcesState();
        synchronized (pool) {
            // Set resources information
            for (Worker<?, ?> resource : pool.findAllResources()) {
                if (resource.getType().equals(Type.WORKER)) {
                    int cores = ((MethodResourceDescription) resource.getDescription()).getTotalCPUComputingUnits();
                    float memory = ((MethodResourceDescription) resource.getDescription()).getMemorySize();
                    // Last boolean equals true because this resource is active
                    state.addHost(resource.getName(), resource.getType().toString(), cores, memory, resource.getSimultaneousTasks(), true);
                } else {
                    // Services doesn't have cores/memory
                    // Last boolean equals true because this resource is active
                    state.addHost(resource.getName(), resource.getType().toString(), 0, (float) 0.0, resource.getSimultaneousTasks(), true);
                }
            }
        }
        // Set cloud information
        state.setUseCloud(CloudManager.isUseCloud());
        if (state.getUseCloud()) {
            try {
                state.setCreationTime(CloudManager.getNextCreationTime());
            } catch (Exception ex) {
                state.setCreationTime(120000l);
            }
            state.setCurrentCloudVMCount(CloudManager.getCurrentVMCount());

            for (ResourceCreationRequest rcr : CloudManager.getPendingRequests()) {
                int[][] simTasks = rcr.requestedSimultaneousTaskCount();
                for (int coreId = 0; coreId < simTasks.length; coreId++) {
                    int coreSlots = 0;
                    for (int implId = 0; implId < simTasks[coreId].length; ++implId) {
                        coreSlots += Math.max(coreSlots, simTasks[coreId][implId]);
                    }
                    // Last boolean equals false because this resource is pending
                    state.updateHostInfo(rcr.getRequested().getName(), rcr.getRequested().getType(),
                            rcr.getRequested().getTotalCPUComputingUnits(), rcr.getRequested().getMemorySize(), coreId, coreSlots, false);
                }
            }
        }

        return state;
    }

    /**
     * Prints out the information about the pending requests
     *
     * @param prefix
     * @return
     */
    public static String getPendingRequestsMonitorData(String prefix) {
        StringBuilder sb = new StringBuilder();
        LinkedList<ResourceCreationRequest> rcr = CloudManager.getPendingRequests();
        for (ResourceCreationRequest r : rcr) {
            // TODO: Add more information (i.e. information per processor, memory type, etc.)
            sb.append(prefix).append("<Resource id=\"requested new VM\">").append("\n");
            sb.append(prefix + "\t").append("<CPUComputingUnits>").append(0).append("</CPUComputingUnits>").append("\n");
            sb.append(prefix + "\t").append("<GPUComputingUnits>").append(0).append("</GPUComputingUnits>").append("\n");
            sb.append(prefix + "\t").append("<FPGAComputingUnits>").append(0).append("</FPGAComputingUnits>").append("\n");
            sb.append(prefix + "\t").append("<OTHERComputingUnits>").append(0).append("</OTHERComputingUnits>").append("\n");
            sb.append(prefix + "\t").append("<Memory>").append(0).append("</Memory>").append("\n");
            sb.append(prefix + "\t").append("<Disk>").append(0).append("</Disk>").append("\n");
            sb.append(prefix + "\t").append("<Provider>").append(r.getProvider()).append("</Provider>").append("\n");
            sb.append(prefix + "\t").append("<Image>").append(r.getRequested().getImage().getImageName()).append("</Image>").append("\n");
            sb.append(prefix + "\t").append("<Status>").append("Creating").append("</Status>").append("\n");
            sb.append(prefix + "\t").append("<Tasks>").append("</Tasks>").append("\n");
            sb.append(prefix).append("</Resource>").append("\n");
        }
        return sb.toString();
    }

    /**
     * Prints out the load information
     *
     */
    public static void printLoadInfo() {
        resourcesLogger.info(resourceUser.getWorkload().toString());
    }

    /**
     * Prints out the resources state
     *
     */
    public static void printResourcesState() {
        resourcesLogger.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        resourcesLogger.info(getResourcesState().toString());
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
        sb.append(CloudManager.getCurrentState(prefix));
        return sb.toString();
    }

    public static long getMaxTasks() {
        return maxTasks;
    }

}
