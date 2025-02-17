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
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.ResourceUser;
import es.bsc.compss.connectors.ConnectorException;
import es.bsc.compss.exceptions.NoResourceAvailableException;
import es.bsc.compss.log.LoggerManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.listeners.ResourceCreationListener;
import es.bsc.compss.types.project.exceptions.ProjectFileValidationException;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.DynamicMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.MethodWorker;
import es.bsc.compss.types.resources.ResourceType;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.types.resources.exceptions.ResourcesFileValidationException;
import es.bsc.compss.types.resources.updates.BusyResources;
import es.bsc.compss.types.resources.updates.IdleResources;
import es.bsc.compss.types.resources.updates.PendingReduction;
import es.bsc.compss.types.resources.updates.PerformedIncrease;
import es.bsc.compss.types.resources.updates.PerformedReduction;
import es.bsc.compss.types.resources.updates.ResourceUpdate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


/**
 * The ResourceManager class is an utility to manage all the resources available for the cores execution. It keeps
 * information about the features of each resource and is used as an endpoint to discover which resources can run a core
 * in a certain moment, the total and the available number of slots.
 */
public class ResourceManager {

    // File Locations
    private static final String RESOURCES_XML = System.getProperty(COMPSsConstants.RES_FILE);
    private static final String RESOURCES_XSD = System.getProperty(COMPSsConstants.RES_SCHEMA);
    private static final String PROJECT_XML = System.getProperty(COMPSsConstants.PROJ_FILE);
    private static final String PROJECT_XSD = System.getProperty(COMPSsConstants.PROJ_SCHEMA);

    // Error messages
    private static final String ERROR_RESOURCES_XML = "ERROR: Cannot parse resources.xml file";
    private static final String ERROR_PROJECT_XML = "ERROR: Cannot parse project.xml file";
    private static final String ERROR_NO_RES =
        "ERROR: No computational resource available" + " (ComputeNode, service or CloudProvider)";
    protected static final String ERROR_UNKNOWN_HOST = "ERROR: Cannot determine the IP address of the local host";
    private static final String DEL_VM_ERR = "ERROR: Canot delete VMs";

    // Information about resources
    private static CloudManager cloudManager;
    private static WorkerPool pool;
    private static int[] poolCoreMaxConcurrentTasks;
    private static ResourceUser resourceUser;

    // Loggers
    private static final Logger RESOURCES_LOGGER = LogManager.getLogger(Loggers.RESOURCES);
    private static final Logger RUNTIME_LOGGER = LogManager.getLogger(Loggers.RM_COMP);


    /*
     ********************************************************************
     ********************************************************************
     ********************** INITIALIZER METHODS *************************
     ********************************************************************
     ********************************************************************
     */
    /**
     * Constructs a new ResourceManager using the Resources xml file content. First of all, an empty resource pool is
     * created and the Cloud Manager is initialized without any providers. Secondly the resource file is validated and
     * parsed and the toplevel xml nodes are processed in different ways depending on its type: - Resource: a new
     * Physical resource is added to the resource pool with the same id as its Name attribute and as many slots as
     * indicated in the project file. If it has 0 slots or it is not on the project xml, the resource is not included. -
     * Service: a new Physical resource is added to the resource pool with the same id as its wsdl attribute and as many
     * slots as indicated in the project file. If it has 0 slots or it is not on the project xml, the resource is not
     * included. - Cloud Provider: if there is any CloudProvider in the project file with the same name, a new Cloud
     * Provider is added to the cloudManager with its name attribute value as identifier. The cloudManager is configured
     * as described between the project xml and the resources file. From the resource file it gets the properties which
     * describe how to connect with it: the connector path, the endpoint, ... Other properties required to manage the
     * resources are specified on the project file: i.e. the maximum amount of resource deployed on that provider. Some
     * configurations depend on both files. One of them is the list of usable images. The images offered by the cloud
     * provider are on a list on the resources file, where there are specified the name and the software description of
     * that image. On the project file there is a description of how the resources created with that image must be used:
     * username, working directory,... Only the images that have been described in both files are added to the
     * cloudManager.
     *
     * @param resUser object to notify resource changes
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
     * Reinitializes the ResourceManager.
     *
     * @param resUser object to notify resource changes.
     */
    public static void clear(ResourceUser resUser) {
        resourceUser = resUser;
        pool = new WorkerPool();
        poolCoreMaxConcurrentTasks = new int[CoreManager.getCoreCount()];
        cloudManager = new CloudManager();
    }

    /**
     * Stops all the nodes within the pool.
     */
    public static void stopNodes() {
        // Log resource
        RESOURCES_LOGGER.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        RESOURCES_LOGGER.info("INFO_MSG = [Stopping all workers]");
        RUNTIME_LOGGER.info("Stopping all workers");

        // Stop all Cloud VM
        if (cloudManager.isUseCloud()) {
            // Transfer files
            RESOURCES_LOGGER.debug("DEBUG_MSG = [Terminating cloud instances...]");
            try {
                cloudManager.terminateALL();
                RESOURCES_LOGGER.info("TOTAL_EXEC_COST = " + cloudManager.getTotalCost());
            } catch (Exception e) {
                RESOURCES_LOGGER.error(COMPSsConstants.TS + ": " + DEL_VM_ERR, e);
            }
            RESOURCES_LOGGER.info("INFO_MSG = [Cloud instances terminated]");
        }

        // Stop static workers - Order its destruction from runtime and transfer files
        // Physical worker (COMM) is erased now - because of cloud
        if (pool != null && !pool.getStaticResources().isEmpty()) {
            RESOURCES_LOGGER.debug("DEBUG_MSG = [Resource Manager retrieving data from workers...]");
            for (Worker<? extends WorkerResourceDescription> r : pool.getStaticResources()) {
                r.disableExecution();
                r.retrieveTracingAndDebugData();
            }
            Semaphore sem = new Semaphore(0);
            ShutdownListener sl = new ShutdownListener(sem);
            RESOURCES_LOGGER.debug("DEBUG_MSG = [Resource Manager stopping workers...]");
            for (Worker<? extends WorkerResourceDescription> r : pool.getStaticResources()) {
                r.stop(sl);
            }
            RUNTIME_LOGGER.debug("Waiting for workers to shutdown...");
            RESOURCES_LOGGER.debug("DEBUG_MSG = [Waiting for workers to shutdown...]");
            sl.enable();

            try {
                sem.acquire();
            } catch (Exception e) {
                RUNTIME_LOGGER.error("ERROR: Exception raised on worker shutdown", e);
                RESOURCES_LOGGER.error("ERROR_MSG= [ERROR: Exception raised on worker shutdown]");
            }
            RESOURCES_LOGGER.info("INFO_MSG = [Workers stopped]");
        }

        // Stopping worker at master process
        RESOURCES_LOGGER.debug("DEBUG_MSG = [Resource Manager stopping worker in master process...]");
        Comm.getAppHost().disableExecution();
        Comm.getAppHost().retrieveTracingAndDebugData();

        mergeProfilerFiles();
        Semaphore sem = new Semaphore(0);
        ShutdownListener sl = new ShutdownListener(sem);
        RESOURCES_LOGGER.debug("DEBUG_MSG = [Resource Manager stopping worker in master process...]");
        Comm.getAppHost().stop(sl);
        sl.enable();
        RUNTIME_LOGGER.debug("Waiting for local worker to shutdown...");
        try {
            sem.acquire();
        } catch (Exception e) {
            RUNTIME_LOGGER.info("ERROR: Exception raised on worker shutdown", e);
            RESOURCES_LOGGER.error("ERROR_MSG= [ERROR: Exception raised on worker shutdown]");
        }
        RESOURCES_LOGGER.info("INFO_MSG = [Worker in master stopped]");
    }

    /*
     ********************************************************************
     ********************************************************************
     ************************* POOL METHODS ****************************
     ********************************************************************
     ********************************************************************
     */
    /**
     * Returns a worker instance with the given name {@code name}.
     *
     * @param name Worker name.
     * @return The worker instance with the given name.
     */
    public static Worker<? extends WorkerResourceDescription> getWorker(String name) {
        return pool.getResource(name);
    }

    /**
     * Return a list of all the resources.
     *
     * @return List of all the resources.
     */
    public static List<Worker<? extends WorkerResourceDescription>> getAllWorkers() {
        return pool.findAllResources();
    }

    /**
     * Returns the number of available workers.
     *
     * @return The number of available workers.
     */
    public static int getTotalNumberOfWorkers() {
        return pool.findAllResources().size();
    }

    /**
     * Adds a new static resource.
     *
     * @param <T> WorkerResourceDescription extension.
     * @param worker Worker to add.
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
        RESOURCES_LOGGER.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        RESOURCES_LOGGER.info("INFO_MSG = [New resource available in the pool. Name = " + worker.getName() + "]");
        RUNTIME_LOGGER.info("New " + ((worker.getType() == ResourceType.HTTP) ? "http" : "computeNode")
            + " available in the pool. Name = " + worker.getName());
    }

    /**
     * Removes a given worker.
     *
     * @param r Worker description.
     */
    public static void removeWorker(Worker<? extends WorkerResourceDescription> r) {
        pool.delete(r);
        int[] maxTaskCount = r.getSimultaneousTasks();
        for (int coreId = 0; coreId < maxTaskCount.length; ++coreId) {
            poolCoreMaxConcurrentTasks[coreId] -= maxTaskCount[coreId];
        }
    }

    /**
     * Updates the coreElement information.
     *
     * @param updatedCores New coreElement information.
     */
    public static void coreElementUpdates(List<Integer> updatedCores) {
        synchronized (pool) {

            pool.coreElementUpdates(updatedCores);
            cloudManager.newCoreElementsDetected(updatedCores);
            updateMaxConcurrentTasks(updatedCores);

        }
    }

    private static void updateMaxConcurrentTasks(List<Integer> updatedCores) {
        poolCoreMaxConcurrentTasks = Arrays.copyOf(poolCoreMaxConcurrentTasks, CoreManager.getCoreCount());
        List<Worker<? extends WorkerResourceDescription>> workers = pool.findAllResources();
        for (Integer coreId : updatedCores) {
            int total = 0;
            for (Worker<? extends WorkerResourceDescription> w : workers) {
                int[] maxTaskCount = w.getSimultaneousTasks();
                total += maxTaskCount[coreId];
            }
            poolCoreMaxConcurrentTasks[coreId] = total;

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
     * Sets the boundaries on the cloud elasticity.
     *
     * @param minVMs Lower number of VMs allowed.
     * @param initialVMs Initial number of VMs.
     * @param maxVMs Higher number of VMs allowed.
     */
    public static void setCloudVMsBoundaries(Integer minVMs, Integer initialVMs, Integer maxVMs) {
        cloudManager.setInitialVMs(initialVMs);
        cloudManager.setMinVMs(minVMs);
        cloudManager.setMaxVMs(maxVMs);
    }

    /**
     * Adds a new Provider to the Cloud section management (and enables the cloud usage).
     *
     * @param providerName Cloud provider name.
     * @param limitOfVMs Provider limit of VMs.
     * @param runtimeConnectorClass Runtime connector abstract class.
     * @param connectorJarPath Path to the connector JAR.
     * @param connectorMainClass Connector main class.
     * @param connectorProperties Connector specific properties.
     * @return A cloud provider instance.
     * @throws ConnectorException When connector cannot be instantiated.
     */
    public static CloudProvider registerCloudProvider(String providerName, Integer limitOfVMs,
        String runtimeConnectorClass, String connectorJarPath, String connectorMainClass,
        Map<String, String> connectorProperties) throws ConnectorException {

        return cloudManager.registerCloudProvider(providerName, limitOfVMs, runtimeConnectorClass, connectorJarPath,
            connectorMainClass, connectorProperties);
    }

    /**
     * Adds a dynamic worker.
     *
     * @param worker Worker to add.
     * @param granted Worker resource description granted by the connector.
     */
    public static void addDynamicWorker(DynamicMethodWorker worker, MethodResourceDescription granted) {
        synchronized (pool) {
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
        RESOURCES_LOGGER.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        RESOURCES_LOGGER.info("INFO_MSG = [New resource available in the pool. Name = " + worker.getName() + "]");
        RUNTIME_LOGGER.info("New resource available in the pool. Name = " + worker.getName());
    }

    /**
     * Adds a cloud worker.
     *
     * @param origin Creation request.
     * @param worker Worker to add.
     * @param granted Worker resource description granted by the connector.
     */
    public static void addCloudWorker(ResourceCreationRequest origin, CloudMethodWorker worker,
        CloudMethodResourceDescription granted) {
        CloudProvider cloudProvider = origin.getProvider();
        cloudProvider.confirmedCreation(origin, worker, granted);
        addDynamicWorker(worker, granted);

        // Notify if listener is set
        ResourceCreationListener listener = origin.getListener();
        if (listener != null) {
            listener.notifyResourceCreation(granted);
        }
    }

    /**
     * Increases the capabilities of a given dynamic worker.
     *
     * @param worker Worker to update.
     * @param extension Description of the increase.
     */
    public static void increasedDynamicWorker(DynamicMethodWorker worker, MethodResourceDescription extension) {

        synchronized (pool) {
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
        RESOURCES_LOGGER.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        RESOURCES_LOGGER.info("INFO_MSG = [Resource modified. Name = " + worker.getName() + "]");
        RUNTIME_LOGGER.info("Resource modified. Name = " + worker.getName());
    }

    /**
     * Increases the capabilities of a given cloud worker.
     *
     * @param origin Creation request.
     * @param worker Worker to increase.
     * @param extension Description of the increase.
     */
    public static void increasedCloudWorker(ResourceCreationRequest origin, CloudMethodWorker worker,
        CloudMethodResourceDescription extension) {
        CloudProvider cloudProvider = origin.getProvider();
        cloudProvider.confirmedCreation(origin, worker, extension);
        increasedDynamicWorker(worker, extension);
    }

    /**
     * Decreases the capabilities of a given cloud worker.
     *
     * @param worker Worker to decrease.
     * @param reduction Description of the decrease.
     */
    public static void requestWorkerReduction(DynamicMethodWorker worker, MethodResourceDescription reduction) {
        ResourceUpdate<MethodResourceDescription> modification = new PendingReduction<>(reduction);
        resourceUser.updatedResource(worker, modification);
    }

    /**
     * Decrease all the capabilities of a given worker.
     *
     * @param name Worker name.
     */
    public static void requestWholeWorkerReduction(String name) {
        MethodWorker worker = (MethodWorker) pool.getResource(name);
        ResourceUpdate<MethodResourceDescription> modification = new PendingReduction<>(worker.getDescription().copy());
        resourceUser.updatedResource(worker, modification);
    }

    /**
     * Decrease all the capabilities of a given worker.
     *
     * @param worker Worker.
     */
    public static void requestWholeWorkerReduction(MethodWorker worker) {
        ResourceUpdate<MethodResourceDescription> modification = new PendingReduction<>(worker.getDescription().copy());
        resourceUser.updatedResource(worker, modification);
    }

    /**
     * Confirms the reduction of a given worker.
     *
     * @param worker Decreased worker.
     * @param reduction Decreased reduction.
     */
    public static <T extends WorkerResourceDescription> void confirmWorkerReduction(Worker<T> worker,
        PendingReduction<T> reduction) {

        ResourceUpdate<T> ru = new PerformedReduction<>(reduction.getModification());
        resourceUser.updatedResource(worker, ru);
    }

    /**
     * Notifies when the worker has been reduced.
     *
     * @param worker Worker to reduce.
     * @param reduction Reduction performed.
     */
    public static void notifyWorkerReduction(DynamicMethodWorker worker, MethodResourceDescription reduction) {
        worker.applyReduction(new PendingReduction<>(reduction));
        MethodResourceDescription modification = reduction;
        ResourceUpdate<MethodResourceDescription> ru = new PerformedReduction<>(modification);
        resourceUser.updatedResource(worker, ru);
    }

    /**
     * Notifies the reduction of the whole worker.
     *
     * @param name Worker name.
     */
    public static void notifyWholeWorkerReduction(String name) {
        DynamicMethodWorker worker = (DynamicMethodWorker) pool.getResource(name);
        MethodResourceDescription reduction = worker.getDescription();
        worker.applyReduction(new PendingReduction<>(reduction));
        MethodResourceDescription modification = reduction;
        ResourceUpdate<MethodResourceDescription> ru = new PerformedReduction<>(modification);
        resourceUser.updatedResource(worker, ru);
    }

    /**
     * Notifies the reduction of the whole worker.
     *
     * @param worker Worker.
     */
    public static void notifyWholeWorkerReduction(DynamicMethodWorker worker) {
        MethodResourceDescription reduction = worker.getDescription();
        worker.applyReduction(new PendingReduction<>(reduction));
        MethodResourceDescription modification = reduction;
        ResourceUpdate<MethodResourceDescription> ru = new PerformedReduction<>(modification);
        resourceUser.updatedResource(worker, ru);
    }

    /**
     * Notifies the reduction of the whole worker.
     *
     * @param name Worker name.
     */
    @SuppressWarnings("unchecked")
    public static void notifyRestart(String name) {
        DynamicMethodWorker worker = (DynamicMethodWorker) pool.getResource(name);
        resourceUser.restartedResource(worker, null);
    }

    /**
     * Decreases the capabilities of a given cloud worker.
     *
     * @param <R> WorkerResourceDescription extension.
     * @param worker Worker to reduce.
     * @param reduction Reduction to perform.
     */
    @SuppressWarnings("unchecked")
    public static <R extends WorkerResourceDescription> void reduceDynamicWorker(DynamicMethodWorker worker,
        PendingReduction<R> reduction) {
        synchronized (pool) {
            int[] maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] -= maxTaskCount[coreId];
            }
            worker.applyReduction((PendingReduction<MethodResourceDescription>) reduction);
            maxTaskCount = worker.getSimultaneousTasks();
            for (int coreId = 0; coreId < maxTaskCount.length; coreId++) {
                poolCoreMaxConcurrentTasks[coreId] += maxTaskCount[coreId];
            }
            pool.defineCriticalSet();
            // Log modified resource
            RESOURCES_LOGGER.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
            RESOURCES_LOGGER.info("INFO_MSG = [Resource modified. Name = " + worker.getName() + "]");
            RUNTIME_LOGGER.info("Resource modified. Name = " + worker.getName());
        }

    }

    /**
     * Notifies that a worker has some idle reserved resources.
     *
     * @param worker Worker to whom the resources belong
     * @param resources amount of resources that are idle
     */
    public static void notifyIdleResources(MethodWorker worker, MethodResourceDescription resources) {

        RUNTIME_LOGGER.info("Node with idle resources. Name = " + worker.getName());
        resourceUser.updatedResource(worker, new IdleResources<>(resources));
    }

    /**
     * Notifies that some worker's resources are no longer idle.
     *
     * @param worker Worker to whom the resources belong
     * @param resources amount of resources that are no longer idle
     */
    public static void notifyResourcesReacquisition(MethodWorker worker, MethodResourceDescription resources) {
        RUNTIME_LOGGER.info("Node reacquires resources. Name = " + worker.getName());
        BusyResources<MethodResourceDescription> ru = new BusyResources<>(resources);
        resourceUser.updatedResource(worker, ru);
        try {
            ru.waitForCompletion();
        } catch (InterruptedException e) {
            // shouldn't be possible
        }
    }

    /**
     * Terminates the given dynamic resource.
     *
     * @param worker Worker to stop.
     * @param reduction Reduction to perform.
     */
    public static void terminateDynamicResource(DynamicMethodWorker worker, MethodResourceDescription reduction) {
        if (worker.shouldBeStopped()) {
            pool.delete(worker);
            RESOURCES_LOGGER.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
            RESOURCES_LOGGER.info("INFO_MSG = [Resource removed from the pool. Name = " + worker.getName() + "]");
            RUNTIME_LOGGER.info("Resource removed from the pool. Name = " + worker.getName());
        }
    }

    /**
     * Terminates the given cloud resource.
     *
     * @param worker Worker to stop.
     * @param reduction Reduction to perform.
     */
    public static void terminateCloudResource(CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        terminateDynamicResource(worker, reduction);
        CloudProvider cp = worker.getProvider();
        cp.requestResourceReduction(worker, reduction);
    }

    /**
     * Requests the creation of {@code numResources} new machines.
     * 
     * @param numResources Number of resources to create.
     * @param listener listener to be notified progress on the resource creation
     */
    public static void requestResources(int numResources, ResourceCreationListener listener) {
        CloudProvider cp = getAvailableCloudProviders().iterator().next(); // first
        if (cp != null) {
            CloudImageDescription imageDescription = cp.getAllImages().iterator().next(); // first
            CloudInstanceTypeDescription typeDescription = cp.getAllTypes().iterator().next(); // first
            CloudMethodResourceDescription cmrd = new CloudMethodResourceDescription(typeDescription, imageDescription);
            for (int i = 1; i <= numResources; ++i) {
                ResourceCreationRequest rcr = cp.requestResourceCreation(cmrd, listener);
                if (rcr != null) {
                    RUNTIME_LOGGER.info("Submited request for new resources (" + i + "/" + numResources + ")");
                }
            }
        }
    }

    /**
     * Requests the destruction of {@code numResources} machines.
     * 
     * @param numResources Number of resources to destroy.
     */
    public static void freeResources(int numResources) {
        // Iterator for Cloud providers
        Collection<CloudProvider> availableCloudProviders = getAvailableCloudProviders();
        int n = 1;
        for (CloudProvider cp : availableCloudProviders) {
            for (CloudMethodWorker cmw : cp.getHostedWorkers()) {
                if (cmw != null) {
                    n = n + 1;
                    requestWorkerReduction(cmw, cmw.getDescription());
                    RUNTIME_LOGGER.info(
                        "Submited request to destroy resources " + cmw.getName() + " (" + n + "/" + numResources + ")");
                }

                // Stop if we have freed enough resources
                if (n > numResources) {
                    return;
                }
            }
        }

        // Check if we have not freed enough resources
        if (n <= numResources) {
            RUNTIME_LOGGER.info("No remaining workers to destroy. Skipping the rest of requests");
        }
    }

    /**
     * Returns whether the cloud is enabled or not.
     *
     * @return {@literal true} if the cloud is enabled, {@literal false} otherwise.
     */
    public static boolean useCloud() {
        return cloudManager.isUseCloud();
    }

    /**
     * Returns the mean creation time.
     *
     * @return The mean creation time.
     * @throws Exception When the connector raises an internal exception.
     */
    public static Long getCreationTime() throws Exception {
        try {
            return cloudManager.getNextCreationTime();
        } catch (ConnectorException e) {
            throw new Exception(e);
        }
    }

    /**
     * Computes the cost per hour of the whole cloud resource pool.
     *
     * @return The cost per hour of the whole pool.
     */
    public static float getCurrentCostPerHour() {
        return cloudManager.currentCostPerHour();
    }

    /**
     * The cloudManager computes the accumulated cost of the execution.
     *
     * @return Cost of the whole execution.
     */
    public static float getTotalCost() {
        return cloudManager.getTotalCost();
    }

    /*
     * **********************************************************************************************************
     * GETTERS
     ***********************************************************************************************************/
    /**
     * Returns the number of maximum cloud VMs.
     *
     * @return The number of maximum cloud VMs.
     */
    public static int getMaxCloudVMs() {
        return cloudManager.getMaxVMs();
    }

    /**
     * Return the number of initial cloud VMs.
     *
     * @return The number of initial cloud VMs.
     */
    public static int getInitialCloudVMs() {
        return cloudManager.getInitialVMs();
    }

    /**
     * The number of minimum cloud VMs.
     *
     * @return The number of minimum cloud VMs.
     */
    public static int getMinCloudVMs() {
        return cloudManager.getMinVMs();
    }

    /**
     * The number of current VMs.
     *
     * @return Number of current VMs.
     */
    public static int getCurrentVMCount() {
        return cloudManager.getCurrentVMCount();
    }

    /**
     * Returns the time until the next VM creation.
     *
     * @return The time until the next VM creation.
     * @throws Exception When the connector raises an internal error.
     */
    public static long getNextCreationTime() throws Exception {
        return cloudManager.getNextCreationTime();
    }

    /**
     * Returns the total slots per core.
     *
     * @return The total slots per core.
     */
    public static int[] getTotalSlots() {
        int[] counts = new int[CoreManager.getCoreCount()];
        if (CoreManager.getCoreCount() > 0) {
            int[] cloudCount = cloudManager.getPendingCoreCounts();
            synchronized (pool) {
                for (int i = 0; i < counts.length; i++) {
                    if (i < cloudCount.length) {
                        counts[i] = poolCoreMaxConcurrentTasks[i] + cloudCount[i];
                    } else {
                        counts[i] = poolCoreMaxConcurrentTasks[i];
                    }
                }
            }
        }
        return counts;
    }

    /**
     * Returns the available slots per core.
     *
     * @return The available slots per core.
     */
    public static int[] getAvailableSlots() {
        return poolCoreMaxConcurrentTasks;
    }

    /**
     * Returns the static resources available at the pool.
     *
     * @return The static resource available at the pool.
     */
    public static Collection<Worker<? extends WorkerResourceDescription>> getStaticResources() {
        synchronized (pool) {
            return pool.getStaticResources();
        }
    }

    /**
     * Returns the dynamic resources available at the pool.
     *
     * @return The dynamic resources available at the pool.
     */
    public static List<DynamicMethodWorker> getDynamicResources() {
        synchronized (pool) {
            return pool.getDynamicResources();
        }
    }

    /**
     * Returns the dynamic resources available at the pool that are in the critical set.
     *
     * @return The dynamic resources available at the pool that are in the critical set.
     */
    public static Collection<DynamicMethodWorker> getCriticalDynamicResources() {
        synchronized (pool) {
            return pool.getCriticalResources();
        }
    }

    /**
     * Returns the dynamic resources available at the pool that are NOT in the critical set.
     *
     * @return The dynamic resources available at the pool that are NOT in the critical set.
     */
    public static Collection<DynamicMethodWorker> getNonCriticalDynamicResources() {
        synchronized (pool) {
            return pool.getNonCriticalResources();
        }
    }

    /**
     * Returns the dynamic resource with the given name {@code name}.
     *
     * @param name Resource name.
     * @return The dynamic resource with the given name {@code name}.
     */
    public static DynamicMethodWorker getDynamicResource(String name) {
        synchronized (pool) {
            return pool.getDynamicResource(name);
        }
    }

    /**
     * Returns the available cloud providers.
     *
     * @return A list of the available cloud providers.
     */
    public static Collection<CloudProvider> getAvailableCloudProviders() {
        return cloudManager.getProviders();
    }

    /**
     * Returns the cloud provider with the given name {@code name}.
     *
     * @param name Cloud provider name.
     * @return The cloud provider with the given name {@code name}.
     */
    public static CloudProvider getCloudProvider(String name) {
        return cloudManager.getProvider(name);
    }

    /**
     * Returns the pending creation requests.
     *
     * @return The pending creation requests.
     */
    public static List<ResourceCreationRequest> getPendingCreationRequests() {
        return cloudManager.getPendingRequests();
    }

    /*
     * ************************************************************************************************************
     * LOGGER METHODS
     **********************************************************************************************************
     */
    /**
     * Dumps the information about the pending requests to a string respecting the given prefix.
     *
     * @param prefix Prefix indentation.
     * @return A string containing the information about the pending requests.
     */
    public static String getPendingRequestsMonitorData(String prefix) {
        StringBuilder sb = new StringBuilder();
        for (ResourceCreationRequest r : cloudManager.getPendingRequests()) {
            // TODO: Add more information (i.e. information per processor, memory type, etc.)
            sb.append(prefix).append("<Resource id=\"requested new VM\">").append("\n");
            sb.append(prefix).append("\t").append("<CPUComputingUnits>").append(0).append("</CPUComputingUnits>")
                .append("\n");
            sb.append(prefix).append("\t").append("<GPUComputingUnits>").append(0).append("</GPUComputingUnits>")
                .append("\n");
            sb.append(prefix).append("\t").append("<FPGAComputingUnits>").append(0).append("</FPGAComputingUnits>")
                .append("\n");
            sb.append(prefix).append("\t").append("<OTHERComputingUnits>").append(0).append("</OTHERComputingUnits>")
                .append("\n");
            sb.append(prefix).append("\t").append("<Memory>").append(0).append("</Memory>").append("\n");
            sb.append(prefix).append("\t").append("<Disk>").append(0).append("</Disk>").append("\n");
            sb.append(prefix).append("\t").append("<Provider>").append(r.getProvider()).append("</Provider>")
                .append("\n");
            sb.append(prefix).append("\t").append("<Image>").append(r.getRequested().getImage().getImageName())
                .append("</Image>").append("\n");
            sb.append(prefix).append("\t").append("<Status>").append("Creating").append("</Status>").append("\n");
            sb.append(prefix).append("\t").append("<Tasks>").append("</Tasks>").append("\n");
            sb.append(prefix).append("</Resource>").append("\n");
        }
        return sb.toString();
    }

    /**
     * Prints out the resources state.
     */
    public static void printResourcesState() {
        RESOURCES_LOGGER.info("TIMESTAMP = " + String.valueOf(System.currentTimeMillis()));
        StringBuilder resourceState = new StringBuilder();

        resourceState.append("RESOURCES_INFO = [").append("\n");
        synchronized (pool) {
            for (Worker<? extends WorkerResourceDescription> resource : pool.findAllResources()) {
                resourceState.append("\t").append("RESOURCE = [").append("\n");
                resourceState.append("\t\t").append("NAME = ").append(resource.getName()).append("\n");
                resourceState.append("\t\t").append("TYPE = ").append(resource.getType().toString()).append("\n");
                if (resource.getType() == ResourceType.HTTP) {
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
                    resourceState.append("\t\t\t").append("\t").append("NUM_SLOTS = ").append(coreSlots[i])
                        .append("\n");
                    resourceState.append("\t\t\t").append("]").append("\n");
                }
                resourceState.append("\t\t").append("]").append("\n"); // End CAN_RUN
                resourceState.append("\t").append("]\n"); // End RESOURCE
            }
        }
        resourceState.append("]").append("\n"); // END RESOURCES_INFO

        resourceState.append("CLOUD_INFO = [").append("\n");

        if (cloudManager.isUseCloud()) {
            resourceState.append("\t").append("CURRENT_CLOUD_VM_COUNT = ").append(cloudManager.getCurrentVMCount())
                .append("\n");
            try {
                resourceState.append("\t").append("CREATION_TIME = ")
                    .append(Long.toString(cloudManager.getNextCreationTime())).append("\n");
            } catch (Exception ex) {
                resourceState.append("\t").append("CREATION_TIME = ").append(120_000L).append("\n");
            }
            resourceState.append("\t").append("PENDING_RESOURCES = [").append("\n");
            for (ResourceCreationRequest rcr : cloudManager.getPendingRequests()) {
                resourceState.append("\t\t").append("RESOURCE = [").append("\n");
                CloudMethodResourceDescription cmrd = rcr.getRequested();
                resourceState.append("\t\t\t").append("NAME = ").append(cmrd.getName()).append("\n");
                resourceState.append("\t\t\t").append("TYPE = ").append(ResourceType.WORKER.toString()).append("\n");
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
        RESOURCES_LOGGER.info(resourceState.toString());
    }

    /**
     * Returns a string containing a dump of the current state of the resources pool.
     *
     * @param prefix Indentation prefix.
     * @return A string containing a dump of the current state of the resources pool.
     */
    public static String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("TIMESTAMP = ").append(String.valueOf(System.currentTimeMillis())).append("\n");
        sb.append(pool.getCurrentState(prefix)).append("\n");
        sb.append(cloudManager.getCurrentState(prefix));
        return sb.toString();
    }

    private static void mergeProfilerFiles() {
        String logPath = LoggerManager.getWorkersLogDir();
        File folder = new File(logPath);
        String[] paths = folder.list();
        HashMap<String, HashMap<String, HashMap<String, Object>>> cacheProfiler = new HashMap<>();
        boolean filesExist = false;
        for (String f : paths) {
            if (f.startsWith("cache_profiler")) {
                filesExist = true;
                try {
                    JSONTokener tokener = new JSONTokener(new FileReader(logPath + f));
                    JSONObject object = new JSONObject(tokener);
                    for (String function : object.keySet()) {
                        if (!cacheProfiler.containsKey(function)) {
                            cacheProfiler.put(function, new HashMap<>());
                        }
                        for (String parameter : object.getJSONObject(function).keySet()) {
                            if (!cacheProfiler.get(function).containsKey(parameter)) {
                                cacheProfiler.get(function).put(parameter, new HashMap<>());
                            }
                            for (String key : object.getJSONObject(function).getJSONObject(parameter).keySet()) {
                                if (!cacheProfiler.get(function).get(parameter).containsKey(key)) {
                                    if (key.equals("USED")) {
                                        cacheProfiler.get(function).get(parameter).put(key, new ArrayList<>());
                                        for (Object s : (JSONArray) object.getJSONObject(function)
                                            .getJSONObject(parameter).get(key)) {
                                            if (!((ArrayList) cacheProfiler.get(function).get(parameter).get(key))
                                                .contains(s.toString())) {
                                                ((ArrayList) cacheProfiler.get(function).get(parameter).get(key))
                                                    .add(s.toString());
                                            }
                                        }
                                    } else {
                                        cacheProfiler.get(function).get(parameter).put(key, Integer.valueOf(object
                                            .getJSONObject(function).getJSONObject(parameter).get(key).toString()));
                                    }
                                } else {
                                    if (key.equals("USED")) {
                                        for (Object s : (JSONArray) object.getJSONObject(function)
                                            .getJSONObject(parameter).get(key)) {
                                            if (!((ArrayList) cacheProfiler.get(function).get(parameter).get(key))
                                                .contains(s.toString())) {
                                                ((ArrayList) cacheProfiler.get(function).get(parameter).get(key))
                                                    .add(s.toString());
                                            }
                                        }
                                    } else {
                                        cacheProfiler.get(function).get(parameter).put(key,
                                            Integer
                                                .valueOf(object.getJSONObject(function).getJSONObject(parameter)
                                                    .get(key).toString())
                                                + Integer.valueOf(
                                                    cacheProfiler.get(function).get(parameter).get(key).toString()));
                                    }
                                }
                            }
                        }
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
                File file = new File(logPath + f);
                file.delete();
            }

        }
        if (filesExist) {
            try {
                int totalGets = 0;
                int totalPuts = 0;
                String filename = "profiler_cache_summary.out";
                FileWriter writer = new FileWriter(logPath + filename);
                writer.write("PROFILER SUMMARY" + "\n");
                for (String function : cacheProfiler.keySet()) {
                    writer.write('\t' + "FUNCTION: " + function + "\n");
                    for (String parameter : cacheProfiler.get(function).keySet()) {
                        writer.write('\t' + " " + '\t' + " " + '\t' + "PARAMETER: " + parameter + "\n");
                        int puts = (int) cacheProfiler.get(function).get(parameter).get("PUT");
                        int gets = (int) cacheProfiler.get(function).get(parameter).get("GET");
                        totalGets += gets;
                        totalPuts += puts;
                        ArrayList<String> used =
                            (ArrayList<String>) cacheProfiler.get(function).get(parameter).get("USED");
                        if (used.size() > 0 || gets > 0) {
                            writer.write('\t' + " " + '\t' + " " + '\t' + " " + '\t' + "PUTS: " + puts + " GETS: "
                                + gets + ". USED IN: " + used + "\n");
                        } else {
                            writer.write('\t' + " " + '\t' + " " + '\t' + " " + '\t' + "[NOT USED]  PUTS: " + puts
                                + " GETS: " + gets + "\n");
                        }
                    }
                }
                writer.write("TOTAL GETS: " + totalGets + "\n");
                writer.write("TOTAL PUTS: " + totalPuts + "\n");
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
