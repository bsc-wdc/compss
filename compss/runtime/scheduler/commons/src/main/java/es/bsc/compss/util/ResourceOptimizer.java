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

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.scheduler.types.WorkloadState;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.ResourceCreationRequest;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.DynamicMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.types.resources.description.CloudImageDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.types.uri.MultiURI;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;


public class ResourceOptimizer extends Thread {

    // Loggers
    protected static final Logger RESOURCES_LOGGER = LogManager.getLogger(Loggers.RESOURCES);
    protected static final Logger RUNTIME_LOGGER = LogManager.getLogger(Loggers.RM_COMP);
    protected static final boolean DEBUG = RUNTIME_LOGGER.isDebugEnabled();

    // WARNING MESSAGES
    private static final String WARN_NO_RESOURCE_MATCHES = "WARN: No resource matches the constraints";
    private static final String WARN_NO_COMPATIBLE_TYPE = "WARN: Cannot find any compatible instanceType";
    private static final String WARN_NO_COMPATIBLE_IMAGE = "WARN: Cannot find any compatible Image";
    private static final String WARN_NO_VALID_INSTANCE = "WARN: Cannot find a containing/contained instanceType";
    private static final String WARN_NO_MORE_INSTANCES = "WARN: Cloud Provider cannot host more instances";
    private static final String WARN_NO_POSIBLE_INCREASE = "WARN: Cloud Provider cannot increase resources";
    private static final String WARN_EXCEPTION_TURN_ON = "WARN: Connector exception on turn on resource";

    // Sleep times
    private static final int INITIAL_SLEEP = 1_000; // ms
    private static final int SLEEP_TIME = 2_000;
    private static final int EVERYTHING_BLOCKED_INTERVAL_TIME = 20_000;
    private static final int EVERYTHING_BLOCKED_MAX_RETRIES = 3;

    // Error messages
    private static final String ERROR_OPT_RES = "Error optimizing resources.";
    private static final String PERSISTENT_BLOCK_ERR = "Unschedulable tasks detected.\n"
        + "COMPSs has found tasks with constraints that cannot be fulfilled.\n" + "Shutting down COMPSs now...";

    private static boolean cleanUp;
    private static boolean redo;

    protected final TaskScheduler ts;
    private boolean running;
    // This counts the number of retries when detecting a situation where all the tasks are blocked, and no resources
    // are being created/can be created. (if you don't handle this situation, the runtime gets blocked)
    // To don't consider the first execution we initialize the value to -1
    private int everythingBlockedRetryCount = -1;
    private long lastPotentialBlockedCheck = System.currentTimeMillis();
    private Map<CloudInstanceTypeDescription, CloudTypeProfile> defaultProfiles;


    /**
     * Creates a new ResourceOptimizer instance.
     * 
     * @param ts Associated Task Scheduler.
     */
    public ResourceOptimizer(TaskScheduler ts) {
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Initializing Resource Optimizer");
        }
        this.setName("ResourceOptimizer");
        this.ts = ts;
        redo = false;
        this.defaultProfiles = new HashMap<>();
        for (CloudProvider cp : ResourceManager.getAvailableCloudProviders()) {
            for (CloudInstanceTypeDescription citd : cp.getAllTypes()) {
                JSONObject citdJSON = ts.getJSONForCloudInstanceTypeDescription(cp, citd);
                JSONObject implsJSON = ts.getJSONForImplementations();
                CloudTypeProfile prof = generateCloudTypeProfile(citdJSON, implsJSON);
                this.defaultProfiles.put(citd, prof);
                RUNTIME_LOGGER.debug("[ResourceOptimizer] JSONProfile for " + citd.getName() + " --> " + citdJSON);
            }
        }
        RUNTIME_LOGGER.info("[Resource Optimizer] Initialization finished");
    }

    /**
     * Generates a new CloudType Profile.
     * 
     * @param citdJSON JSON Object representation of a Cloud instance type.
     * @param implsJSON Implementation JSON Object representation.
     * @return CloudTypeProfile.
     */
    protected CloudTypeProfile generateCloudTypeProfile(JSONObject citdJSON, JSONObject implsJSON) {
        return new CloudTypeProfile(citdJSON, implsJSON);
    }

    /**
     * Creates a new CloudType Profile from the given cloud instance type description.
     * 
     * @param citd Cloud instance type description.
     * @return CloudTypeProfile.
     */
    protected CloudTypeProfile getCloudTypeProfile(CloudInstanceTypeDescription citd) {
        return this.defaultProfiles.get(citd);
    }

    /**
     * Actions to perform after the core elements are updated.
     */
    public void coreElementsUpdated() {
        // Nothing to do
    }

    @Override
    public final void run() {
        // Mark the thread as running
        this.running = true;

        // Initial sleep
        if (ResourceManager.useCloud()) {
            try {
                Thread.sleep(INITIAL_SLEEP);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // Initial creations
        if (ResourceManager.useCloud()) {
            RUNTIME_LOGGER.info("[Resource Optimizer] Checking initial creations");
            initialCreations();
        }

        // General running
        while (this.running) {
            try {
                // Do operations
                doOperations();

                // Remove obsoletes
                periodicRemoveObsoletes();
                periodicCheckWorkers();
                // Wait until applying next optimization
                try {
                    synchronized (this) {
                        if (this.running) {
                            this.wait(SLEEP_TIME);
                        }
                    }
                } catch (InterruptedException ie) {
                    // Do nothing. It was interrupted to trigger another optimization
                }
            } catch (Exception e) {
                RUNTIME_LOGGER.error(ERROR_OPT_RES, e);
            }
        }
    }

    private void doOperations() {
        do {
            redo = false;

            // Apply optimizations
            if (ResourceManager.useCloud()) {
                // Only apply them when connector supports it
                // Only apply them when there is no External Adaptation
                if (isAutomaticScalingEnabled() && !this.ts.isExternalAdaptationEnabled()) {
                    if (DEBUG) {
                        RUNTIME_LOGGER.debug("[Resource Optimizer] Applying automatic scaling operations");
                    }
                    WorkloadState workload = this.ts.getWorkload();
                    applyPolicies(workload);
                }
            }

            // Handle potential blocks
            if (DEBUG) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] Handling potential blocks");
            }
            int blockedTasks = this.ts.getNumberOfBlockedActions();
            boolean potentialBlock = blockedTasks > 0;
            if (ResourceManager.useCloud()) {
                // If cloud is enabled, also check the creating VMs
                int vmsBeingCreated = ResourceManager.getPendingCreationRequests().size();
                potentialBlock = potentialBlock && (vmsBeingCreated == 0);
            }
            handlePotentialBlock(potentialBlock);
        } while (redo);
    }

    private boolean isAutomaticScalingEnabled() {
        // Check if any of the registered Cloud Providers have Automatic Scaling enabled
        for (CloudProvider cp : ResourceManager.getAvailableCloudProviders()) {
            if (cp.isAutomaticScalingEnabled()) {
                return true;
            }
        }

        // Otherwise set it to false
        return false;
    }

    private void periodicRemoveObsoletes() {
        List<Worker<? extends WorkerResourceDescription>> workers = ResourceManager.getAllWorkers();
        for (Worker<? extends WorkerResourceDescription> w : workers) {
            List<MultiURI> obsoletes = w.pollObsoletes();
            if (obsoletes.size() > 0) {
                w.getNode().removeObsoletes(obsoletes);
            }
        }

    }

    private void periodicCheckWorkers() {
        List<Worker<? extends WorkerResourceDescription>> workers = ResourceManager.getAllWorkers();
        for (Worker<? extends WorkerResourceDescription> w : workers) {
            if (!w.isLost()) {
                w.getNode().verifyNodeIsRunning();
            } else {
                RUNTIME_LOGGER.debug(" Node is lost, skipping ping: " + w.getName());
            }
        }
    }

    /**
     * Perform an optimization.
     */
    public final void optimizeNow() {
        synchronized (this) {
            this.notify();
            redo = true;
        }
    }

    /**
     * Marks the cleanup.
     */
    public void cleanUp() {
        cleanUp = true;
    }

    /**
     * Sets the component to shut down.
     */
    public final void shutdown() {
        synchronized (this) {
            // Stop
            this.running = false;
            this.notify();
            cleanUp = true;
        }

        // Print status
        RESOURCES_LOGGER.info(this.ts.getWorkload());
    }

    /**
     * Handles a potential block.
     * 
     * @param potentialBlock Boolean indicating whether there is a potential block or not.
     */
    public final void handlePotentialBlock(boolean potentialBlock) {
        if (potentialBlock) {
            // All tasks are blocked and there are no resources available...
            if ((System.currentTimeMillis() - this.lastPotentialBlockedCheck) > EVERYTHING_BLOCKED_INTERVAL_TIME) {
                this.lastPotentialBlockedCheck = System.currentTimeMillis();
                ++this.everythingBlockedRetryCount;
                if (this.everythingBlockedRetryCount > 0) {
                    // First time not taken into account
                    if (this.everythingBlockedRetryCount < EVERYTHING_BLOCKED_MAX_RETRIES) {
                        // Retries limit not reached. Warn the user...
                        int retriesLeft = EVERYTHING_BLOCKED_MAX_RETRIES - this.everythingBlockedRetryCount;
                        ErrorManager.warn("No task could be scheduled to any of the available resources.\n"
                            + "This could end up blocking COMPSs. Will check it again in "
                            + (EVERYTHING_BLOCKED_INTERVAL_TIME / 1_000) + " seconds.\n" + "Possible causes: \n"
                            + "    -Network problems: non-reachable nodes, sshd service not started, etc.\n"
                            + "    -There isn't any computing resource that fits the defined tasks constraints.\n"
                            + "If this happens " + retriesLeft + " more time" + (retriesLeft > 1 ? "s" : "")
                            + ", the runtime will shutdown.");
                    } else {
                        // Retry limit reached. Error and shutdown.
                        ErrorManager.error(PERSISTENT_BLOCK_ERR);
                    }
                }
            }
        } else {
            this.everythingBlockedRetryCount = 0;
            this.lastPotentialBlockedCheck = System.currentTimeMillis();
        }
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************** INITIAL RESOURCES CREATIONS ******************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    /**
     * Triggers the creation of the initial VMs to accomplish the expected minimum VMs and ensure that there are workers
     * to run every type of task.
     */
    protected void initialCreations() {
        // Requests initial VMs considering the registered core elements
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Requesting VMs related to registered coreElements");
        }
        int ceFilledVms = addBasicNodes();
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Requested " + ceFilledVms + " coreElement-related VMs");
        }

        // Requests the rest of VMs to fulfill the initial VMs property
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Requesting VMs to fulfill initialVMs property");
        }
        int extraVms = addExtraNodes(ceFilledVms);
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Requested " + extraVms + " extra VMs");
        }
    }

    /**
     * Asks for the VM needed for the runtime to be able to execute all method cores. First it groups the constraints of
     * all the methods per Architecture and tries to merge included resource descriptions in order to reduce the amount
     * of required VMs. It also tries to join the unassigned architecture methods with the closer constraints of a
     * defined one. After that it distributes the Initial VM Count among the architectures taking into account the
     * number of methods that can be run in each architecture. If the amount of different constraints is higher than the
     * Initial VM count it applies an aggressive merge method to each architecture in order to fulfill the initial
     * Constraint. It creates a single VM for each final method constraint. Although these aggressive merges, the amount
     * of different constraints can be higher than the initial VM Count constraint. In this case, it violates the
     * initial VM constraint and asks for more resources.
     *
     * @return the amount of requested VM
     */
    public static int addBasicNodes() {
        int coreCount = CoreManager.getCoreCount();
        List<ConstraintsCore>[] unfulfilledConstraints = getUnfulfilledConstraints();
        int unfulfilledConstraintsCores = 0;
        for (int coreId = 0; coreId < coreCount; coreId++) {
            if (unfulfilledConstraints[coreId].size() > 0) {
                unfulfilledConstraintsCores += unfulfilledConstraints[coreId].size();
                break;
            }
        }
        if (unfulfilledConstraintsCores == 0) {
            return 0;
        }

        /*
         * constraintsPerArquitecture has loaded all constraint for each task. architectures has a list of all the
         * architecture names.
         * 
         * e.g. architectures constraintsPerArquitecture Intel = |MR1|--|MR2| AMD = |MR3| [unassigned] = |MR4|--|MR5|
         */
        Map<String, List<ConstraintsCore>> arch2Constraints = classifyArchitectures(unfulfilledConstraints);

        /*
         * Tries to reduce the number of machines per architecture by entering constraints in another core's constraints
         */
        reduceArchitecturesConstraints(arch2Constraints);

        /*
         * Checks if there are enough Vm for a Unassigned Arquitecture If not it set each unassigned task into the
         * architecture with the most similar task e.g. constraintsPerArquitecture Intel --> |MR1|--|MR2|--|MR5| AMD -->
         * |MR3|--|MR4|
         */
        reassignUnassignedConstraints(arch2Constraints);

        /*
         * Tries to reduce the number of machines per architecture by entering constraints in another core's constraints
         */
        reduceArchitecturesConstraints(arch2Constraints);

        int createdCount = 0;
        for (int coreId = 0; coreId < coreCount; coreId++) {
            while (!unfulfilledConstraints[coreId].isEmpty()) {
                ConstraintsCore cc = unfulfilledConstraints[coreId].remove(0);
                cc.confirmed();
                ResourceCreationRequest rcr = askForResources(cc.desc, false);
                if (rcr != null) {
                    rcr.print(RESOURCES_LOGGER, DEBUG);
                    createdCount++;
                }
            }
        }

        if (DEBUG) {
            RESOURCES_LOGGER
                .debug("DEBUG_MSG = [\n" + "\tIn order to be able to execute all cores, Resource Manager has asked for "
                    + createdCount + " Cloud resources\n" + "]");
        }
        return createdCount;
    }

    // Removes from the list all the Constraints fulfilled by existing resources
    @SuppressWarnings("unchecked")
    private static List<ConstraintsCore>[] getUnfulfilledConstraints() {
        int coreCount = CoreManager.getCoreCount();
        List<ConstraintsCore>[] unfulfilledConstraints = new LinkedList[coreCount];
        int[] maxSimTasks = ResourceManager.getTotalSlots();
        for (CoreElement ce : CoreManager.getAllCores()) {
            int coreId = ce.getCoreId();
            unfulfilledConstraints[coreId] = new LinkedList<>();
            if (maxSimTasks[coreId] == 0) {
                List<Implementation> impls = ce.getImplementations();
                for (Implementation impl : impls) {
                    if (impl.getTaskType() == TaskType.METHOD) {
                        MethodResourceDescription requirements = (MethodResourceDescription) impl.getRequirements();
                        CloudMethodResourceDescription cd = new CloudMethodResourceDescription(requirements);
                        ConstraintsCore cc = new ConstraintsCore(cd, coreId, unfulfilledConstraints[coreId]);
                        unfulfilledConstraints[coreId].add(cc);
                    }
                }
            }
        }
        return unfulfilledConstraints;
    }

    /**
     * Classifies the constraints depending on their architecture and leaves it on coreResourceList.
     *
     * @param constraints Constraints to classify.
     * @return List with all the architectures' names.
     */
    private static Map<String, List<ConstraintsCore>> classifyArchitectures(List<ConstraintsCore>[] constraints) {
        Map<String, List<ConstraintsCore>> archs = new HashMap<>();

        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            if (constraints[coreId] != null) {
                for (ConstraintsCore cc : constraints[coreId]) {
                    List<String> runnableArchitectures = cc.desc.getArchitectures();
                    for (String arch : runnableArchitectures) {
                        // Insert element into HashMap
                        List<ConstraintsCore> archConstr = archs.get(arch);
                        if (archConstr == null) {
                            archConstr = new LinkedList<>();
                            archs.put(arch, archConstr);
                        }
                        archConstr.add(cc);
                    }
                }
            }
        }
        return archs;
    }

    private static void reduceArchitecturesConstraints(Map<String, List<ConstraintsCore>> arch2Ctrs) {
        for (List<ConstraintsCore> arch : arch2Ctrs.values()) {
            ConstraintsCore[] ctrs = new ConstraintsCore[arch.size()];
            int i = 0;
            for (ConstraintsCore cc : arch) {
                ctrs[i++] = cc;
            }

            Integer[] mergedTo = new Integer[arch.size()];
            for (i = 0; i < ctrs.length; i++) {
                if (mergedTo[i] != null) {
                    continue;
                }
                String os = ctrs[i].desc.getOperatingSystemType();
                for (int j = i + 1; j < ctrs.length; j++) {
                    if (mergedTo[j] != null) {
                        continue;
                    }
                    if (os.compareTo(ctrs[j].desc.getOperatingSystemType()) == 0
                        || os.compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0 || ctrs[j].desc
                            .getOperatingSystemType().compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0) {
                        mergedTo[j] = i;
                        ctrs[i].join(ctrs[j]);
                        arch.remove(ctrs[j]);
                    }
                }
            }
        }

    }

    private static void reassignUnassignedConstraints(Map<String, List<ConstraintsCore>> arch2Ctrs) {
        /*
         * ATTENTION: Since this method is only evaluated from constraints, there is only 1 PROCESSOR and 1 ARCHITECTURE
         */
        List<ConstraintsCore> unassignedList = arch2Ctrs.get(CloudMethodResourceDescription.UNASSIGNED_STR);
        if (unassignedList == null) {
            return;
        }
        if (arch2Ctrs.size() == 1) {
            return;
        }
        if (arch2Ctrs.size() == 2) {
            for (Entry<String, List<ConstraintsCore>> ctrs : arch2Ctrs.entrySet()) {
                if (ctrs.getKey().compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0) {
                    continue;
                } else {
                    ctrs.getValue().addAll(unassignedList);
                    return;
                }
            }
        }

        List<ConstraintsCore> assignedList = new LinkedList<>();
        for (Entry<String, List<ConstraintsCore>> ctrs : arch2Ctrs.entrySet()) {
            if (ctrs.getKey().compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0) {
                continue;
            } else {
                assignedList.addAll(ctrs.getValue());

            }
        }

        while (!unassignedList.isEmpty()) {
            ConstraintsCore unassigned = unassignedList.remove(0);
            CloudMethodResourceDescription candidate = unassigned.desc;
            String bestArch = CloudMethodResourceDescription.UNASSIGNED_STR;
            Float bestDifference = Float.MAX_VALUE;
            for (ConstraintsCore assigned : assignedList) {
                CloudMethodResourceDescription option = assigned.desc;
                float difference = candidate.difference(option);
                if (bestDifference < 0) {
                    if (difference < 0) {
                        if (difference > bestDifference) {
                            List<String> availArchs = option.getArchitectures();
                            if (availArchs != null && !availArchs.isEmpty()) {
                                bestArch = availArchs.get(0);
                            }
                            bestDifference = difference;
                        }
                    }
                } else if (difference < bestDifference) {
                    List<String> availArchs = option.getArchitectures();
                    if (availArchs != null && !availArchs.isEmpty()) {
                        bestArch = availArchs.get(0);
                    }
                    bestDifference = difference;
                }
            }

            // Add
            List<Processor> procs = unassigned.desc.getProcessors();
            if (procs == null) {
                procs = new LinkedList<>();
            }

            if (!procs.isEmpty()) {
                procs.get(0).setArchitecture(bestArch);
            } else {
                Processor p = new Processor();
                p.setArchitecture(bestArch);
                procs.add(p);
            }
            arch2Ctrs.get(bestArch).add(unassigned);
        }
    }

    /**
     * Asks for the rest of VM that user wants to start with. After executing the addBasicNodes, it might happen that
     * the number of initial VMs constrained by the user is still not been fulfilled. The addBasicNodes creates up to as
     * much VMs as different methods. If the initial VM Count is higher than this number of methods then there will be
     * still some VM requests missing. The addExtraNodes creates this difference of VMs. First it tries to merge the
     * method constraints that are included into another methods. And performs a less aggressive and more equal
     * distribution.
     *
     * @param alreadyCreated number of already requested VMs.
     * @return the number of extra VMs created to fulfill the Initial VM Count constraint.
     */
    public static int addExtraNodes(int alreadyCreated) {
        int initialVMsCount = ResourceManager.getInitialCloudVMs();
        int vmCount = initialVMsCount - alreadyCreated;
        if (vmCount <= 0) {
            return 0;
        }

        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Adding " + vmCount + " extra VMs for initialVM property...");
            RESOURCES_LOGGER.debug("DEBUG_MSG = [\n" + "\tALREADY_CREATED_INSTANCES = " + alreadyCreated + "\n"
                + "\tMAXIMUM_NEW_PETITIONS = " + vmCount + "\n" + "]");
        }

        // Since the core elements do not trigger any resource creation, we ask for random VMs
        ResourceManager.requestResources(vmCount, null);
        return vmCount;
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************** DYNAMIC RESOURCES CREATIONS ******************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    protected void applyPolicies(WorkloadState workload) {
        int currentCloudVMCount = ResourceManager.getCurrentVMCount();
        Integer maxNumberOfVMs = ResourceManager.getMaxCloudVMs();
        Integer minNumberOfVMs = ResourceManager.getMinCloudVMs();

        long creationTime;
        try {
            creationTime = ResourceManager.getNextCreationTime();
        } catch (Exception ex) {
            creationTime = 120_000L;
        }

        if (creationTime < 30_000) {
            creationTime = 30_000L;
        }

        int coreCount = workload.getCoreCount();
        int noResourceCount = workload.getNoResourceCount();
        int[] noResourceCounts = workload.getNoResourceCounts();

        long[] minCoreTime = new long[coreCount];
        long[] meanCoreTime = new long[coreCount];
        long[] maxCoreTime = new long[coreCount];
        long[] minRemainingCoreTime = new long[coreCount];
        long[] meanRemainingCoreTime = new long[coreCount];
        long[] maxRemainingCoreTime = new long[coreCount];

        for (int coreId = 0; coreId < coreCount; coreId++) {
            minCoreTime[coreId] = Math.min(workload.getCoreMinTime(coreId), creationTime);
            meanCoreTime[coreId] = Math.min(workload.getCoreMeanTime(coreId), creationTime);
            maxCoreTime[coreId] = Math.min(workload.getCoreMaxTime(coreId), creationTime);
            long meanRunningCoreTime = workload.getRunningCoreMeanTime(coreId);
            if (minCoreTime[coreId] - meanRunningCoreTime < 0) {
                minRemainingCoreTime[coreId] = 0;
            } else {
                minRemainingCoreTime[coreId] = Math.min(minCoreTime[coreId] - meanRunningCoreTime, creationTime);
            }
            if (meanCoreTime[coreId] - meanRunningCoreTime < 0) {
                meanRemainingCoreTime[coreId] = 0;
            } else {
                meanRemainingCoreTime[coreId] = Math.min(meanCoreTime[coreId] - meanRunningCoreTime, creationTime);
            }
            if (maxCoreTime[coreId] - meanRunningCoreTime < 0) {
                maxRemainingCoreTime[coreId] = 0;
            } else {
                maxRemainingCoreTime[coreId] = Math.min(maxCoreTime[coreId] - meanRunningCoreTime, creationTime);
            }

        }
        long[] runningMinCoreTime = new long[coreCount];
        long[] runningMeanCoreTime = new long[coreCount];
        long[] runningMaxCoreTime = new long[coreCount];
        long[] readyMinCoreTime = new long[coreCount];
        long[] readyMeanCoreTime = new long[coreCount];
        long[] readyMaxCoreTime = new long[coreCount];
        long[] pendingMinCoreTime = new long[coreCount];
        long[] pendingMeanCoreTime = new long[coreCount];
        long[] pendingMaxCoreTime = new long[coreCount];

        int[] realSlots = ResourceManager.getAvailableSlots();
        int[] totalSlots = ResourceManager.getTotalSlots();

        int[] runningCounts = workload.getRunningTaskCounts();
        int[] readyCounts = workload.getReadyCounts();
        // int[] pendingCounts = workload.getWaitingTaskCounts();
        int[] pendingCounts = new int[coreCount];

        long totalPendingTasks = 0;
        int maxConcurrentTasks = 0;
        for (int i = 0; i < coreCount; i++) {
            runningMinCoreTime[i] = minRemainingCoreTime[i] * runningCounts[i];
            readyMinCoreTime[i] = runningMinCoreTime[i] + (minCoreTime[i] * readyCounts[i]);
            pendingMinCoreTime[i] = readyMinCoreTime[i] + (minCoreTime[i] * pendingCounts[i]);
            runningMeanCoreTime[i] = meanRemainingCoreTime[i] * runningCounts[i];
            readyMeanCoreTime[i] = runningMeanCoreTime[i] + (meanCoreTime[i] * readyCounts[i]);
            pendingMeanCoreTime[i] = readyMeanCoreTime[i] + (meanCoreTime[i] * pendingCounts[i]);
            runningMaxCoreTime[i] = maxRemainingCoreTime[i] * runningCounts[i];
            readyMaxCoreTime[i] = runningMaxCoreTime[i] + (maxCoreTime[i] * readyCounts[i]);
            pendingMaxCoreTime[i] = readyMaxCoreTime[i] + (maxCoreTime[i] * pendingCounts[i]);
            totalPendingTasks = totalPendingTasks + pendingCounts[i];
            if (runningCounts[i] > 0 || readyCounts[i] > 0) {
                if (realSlots[i] > maxConcurrentTasks) {
                    maxConcurrentTasks = realSlots[i];
                }
            }

        }
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Applying VM optimization policies (currentVMs: "
                + currentCloudVMCount + " maxVMs: " + maxNumberOfVMs + " minVMs: " + minNumberOfVMs + ")");
        }

        // Check if there is some mandatory creation/destruction
        if (!cleanUp) {
            // For CE without resources where to run
            List<Integer> requiredVMs = checkNeededMachines(noResourceCount, noResourceCounts, totalSlots);

            if (!requiredVMs.isEmpty()) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] Required VMs. Mandatory Increase");
                float[] creationRecommendations = recommendCreations(coreCount, creationTime, readyMinCoreTime,
                    readyMeanCoreTime, readyMaxCoreTime, totalSlots, realSlots);
                for (Integer coreId : requiredVMs) {
                    // Ensure that we ask one slot for it
                    creationRecommendations[coreId] = Math.max(creationRecommendations[coreId], 1);
                }
                mandatoryIncrease(creationRecommendations, requiredVMs);
                return;
            }

            // For accomplishing the minimum amount of vms
            if (minNumberOfVMs != null && minNumberOfVMs > currentCloudVMCount) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount
                    + ") count smaller than minimum VMs (" + minNumberOfVMs + "). Mandatory Increase");
                float[] creationRecommendations = orderCreations(coreCount, creationTime, readyMinCoreTime,
                    readyMeanCoreTime, readyMaxCoreTime, totalSlots, realSlots);
                mandatoryIncrease(creationRecommendations, new LinkedList<>());
                return;
            }
            // For not exceeding the VM top limit
            if (maxNumberOfVMs != null && maxNumberOfVMs < currentCloudVMCount) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount
                    + ") count bigger than maximum VMs (" + maxNumberOfVMs + "). Mandatory reduction");
                float[] destroyRecommendations = deleteRecommendations(coreCount, SLEEP_TIME, pendingMinCoreTime,
                    pendingMeanCoreTime, pendingMaxCoreTime, totalSlots, realSlots);
                mandatoryReduction(destroyRecommendations);
                return;
            }
        }

        // Check Recommended creations
        if ((maxNumberOfVMs == null || maxNumberOfVMs > currentCloudVMCount) && workload.getReadyCount() > 1) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount
                + ") count smaller than maximum VMs (" + maxNumberOfVMs + ")");
            float[] creationRecommendations = recommendCreations(coreCount, creationTime, readyMinCoreTime,
                readyMeanCoreTime, readyMaxCoreTime, totalSlots, realSlots);
            if (optionalIncrease(creationRecommendations)) {
                return;
            }
        }
        // Check Recommended destructions
        if ((minNumberOfVMs == null || minNumberOfVMs < currentCloudVMCount)
            && totalPendingTasks <= workload.getReadyCount() + workload.getRunningTaskCount()
            && workload.getReadyCount() + workload.getRunningTaskCount() <= maxConcurrentTasks) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount
                + ") count bigger than minimum VMs (" + minNumberOfVMs + ")");
            float[] destroyRecommendations = deleteRecommendations(coreCount, SLEEP_TIME, pendingMinCoreTime,
                pendingMeanCoreTime, pendingMaxCoreTime, totalSlots, realSlots);
            if (optionalReduction(destroyRecommendations)) {
                return;
            }
        }
    }

    private void mandatoryIncrease(float[] creationRecommendations, List<Integer> requiredVMs) {
        PriorityQueue<ValueResourceDescription> pq = new PriorityQueue<>();

        boolean[] required = new boolean[creationRecommendations.length];
        for (int coreId : requiredVMs) {
            required[coreId] = true;
        }

        for (int coreId = 0; coreId < creationRecommendations.length; coreId++) {
            CoreElement ce = CoreManager.getCore(coreId);
            for (Implementation impl : ce.getImplementations()) {
                if (impl.getTaskType() == TaskType.HTTP) {
                    continue;
                }

                MethodResourceDescription constraints = ((AbstractMethodImplementation) impl).getRequirements();
                ValueResourceDescription v =
                    new ValueResourceDescription(constraints, creationRecommendations[coreId], false);
                pq.add(v);
            }
        }
        requestOneCreation(pq, true);
    }

    private boolean optionalIncrease(float[] creationRecommendations) {
        PriorityQueue<ValueResourceDescription> pq = new PriorityQueue<>();

        for (int coreId = 0; coreId < creationRecommendations.length; coreId++) {
            if (creationRecommendations[coreId] > 1) {
                CoreElement ce = CoreManager.getCore(coreId);
                for (Implementation impl : ce.getImplementations()) {
                    if (impl.getTaskType() == TaskType.HTTP) {
                        continue;
                    }
                    MethodResourceDescription constraints = ((AbstractMethodImplementation) impl).getRequirements();
                    ValueResourceDescription v =
                        new ValueResourceDescription(constraints, creationRecommendations[coreId], false);
                    pq.add(v);
                }
            }
        }

        ResourceCreationRequest rcr = requestOneCreation(pq, false);
        return (rcr != null);
    }

    private boolean optionalReduction(float[] destroyRecommendations) {
        List<DynamicMethodWorker> nonCritical =
            trimReductionOptions(ResourceManager.getNonCriticalDynamicResources(), destroyRecommendations);
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Searching for best destruction");
        }
        ReductionOption nonCriticalSolution = getBestDestruction(nonCritical, destroyRecommendations);
        CloudMethodWorker res;
        if (nonCriticalSolution == null) {
            if (DEBUG) {
                RUNTIME_LOGGER.warn("[Resource Optimizer] No solution found");
            }
            return false;
        }

        res = (CloudMethodWorker) nonCriticalSolution.getResource();

        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Best resource to remove is " + nonCriticalSolution.getName()
                + "and record is [" + nonCriticalSolution.getUndesiredCEsAffected() + ","
                + nonCriticalSolution.getUndesiredSlotsAffected() + "," + nonCriticalSolution.getDesiredSlotsAffected()
                + "]");
        }
        if (nonCriticalSolution.getUndesiredSlotsAffected() > 0 && res.getUsedCPUTaskCount() > 0) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Optional destroy recommendation not applied");
            return false;
        } else {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Optional destroy recommendation applied");
            nonCriticalSolution.apply();
            return true;
        }
    }

    private void mandatoryReduction(float[] destroyRecommendations) {
        List<DynamicMethodWorker> critical =
            trimReductionOptions(ResourceManager.getCriticalDynamicResources(), destroyRecommendations);
        // LinkedList<CloudMethodWorker> critical = checkCriticalSafeness
        // (critical);
        List<DynamicMethodWorker> nonCritical =
            trimReductionOptions(ResourceManager.getNonCriticalDynamicResources(), destroyRecommendations);
        ReductionOption criticalSolution = getBestDestruction(critical, destroyRecommendations);
        ReductionOption nonCriticalSolution = getBestDestruction(nonCritical, destroyRecommendations);

        boolean criticalIsBetter;
        if (criticalSolution == null) {
            if (nonCriticalSolution == null) {
                return;
            } else {
                criticalIsBetter = false;
            }
        } else if (nonCriticalSolution == null) {
            criticalIsBetter = true;
        } else {
            criticalIsBetter = false;

            if (nonCriticalSolution.getUndesiredCEsAffected() == criticalSolution.getUndesiredCEsAffected()) {
                if (nonCriticalSolution.getUndesiredSlotsAffected() == criticalSolution.getUndesiredSlotsAffected()) {
                    if (nonCriticalSolution.getDesiredSlotsAffected() < criticalSolution.getDesiredSlotsAffected()) {
                        criticalIsBetter = true;
                    }
                } else if (nonCriticalSolution.getUndesiredSlotsAffected() > criticalSolution
                    .getUndesiredSlotsAffected()) {
                    criticalIsBetter = true;
                }
            } else if (nonCriticalSolution.getUndesiredCEsAffected() > criticalSolution.getUndesiredCEsAffected()) {
                criticalIsBetter = true;
            }
        }

        if (criticalIsBetter) {
            criticalSolution.apply();
        } else {
            if (nonCriticalSolution == null) {
                return;
            }
            nonCriticalSolution.apply();
        }

    }

    private List<DynamicMethodWorker> trimReductionOptions(Collection<DynamicMethodWorker> options,
        float[] recommendations) {
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] * Trimming reduction options");
        }
        List<DynamicMethodWorker> resources = new LinkedList<>();
        Iterator<DynamicMethodWorker> it = options.iterator();
        while (it.hasNext()) {

            DynamicMethodWorker resource = it.next();
            boolean aggressive = false;// (resource.getUsedTaskCount() == 0);
            boolean add = !aggressive;
            if (DEBUG) {
                RUNTIME_LOGGER.debug("\t Evaluating " + resource.getName() + ". Default reduction is " + add);
            }
            List<Integer> executableCores = resource.getExecutableCores();
            for (int coreId : executableCores) {

                if (!aggressive && recommendations[coreId] < 1) {
                    if (DEBUG) {
                        RUNTIME_LOGGER.debug("\t\tVM not removed because of not agressive and recomendations < 1 ("
                            + recommendations[coreId] + ")");
                    }
                    add = false;
                    break;
                }
                if (aggressive && recommendations[coreId] > 0) {
                    if (DEBUG) {
                        RUNTIME_LOGGER.debug("\t\tVM removed because of agressive and recomendations > 0 ("
                            + recommendations[coreId] + ")");
                    }
                    add = true;
                    break;
                }
            }
            if (add) {
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("\t\tVM added to candidate to remove.");
                }
                resources.add(resource);
            }
        }
        return resources;
    }

    private static ResourceCreationRequest requestOneCreation(PriorityQueue<ValueResourceDescription> pq,
        boolean include) {
        ValueResourceDescription v;
        while ((v = pq.poll()) != null) {
            ResourceCreationRequest rcr = askForResources(v.value < 1 ? 1 : (int) v.value, v.constraints, include);
            if (rcr != null) {
                rcr.print(RESOURCES_LOGGER, DEBUG);
                return rcr;
            }
        }

        return null;
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ******************************* DYNAMIC RESOURCES RECOMMENDATIONS ***************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    private List<Integer> checkNeededMachines(int noResourceCount, int[] noResourceCountPerCore,
        int[] slotCountPerCore) {
        List<Integer> needed = new LinkedList<>();
        if (noResourceCount == 0) {
            return needed;
        }
        for (int i = 0; i < CoreManager.getCoreCount(); i++) {
            if (noResourceCountPerCore[i] > 0 && slotCountPerCore[i] == 0) {
                needed.add(i);
            }
        }
        return needed;
    }

    private float[] recommendCreations(int coreCount, long creationTime, long[] aggregatedMinCoreTime,
        long[] aggregatedMeanCoreTime, long[] aggregatedMaxCoreTime, int[] totalSlots, int[] realSlots) {

        float[] creations = new float[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            // long realTime = realSlots[coreId] * creationTime;
            long totalTime = totalSlots[coreId] * creationTime;
            // OLD version: did a mean of total (include pending creations) and
            // real
            // long embraceableLoad = (realTime + totalTime) / 2;
            long embraceableLoad = totalTime;
            long remainingLoad = aggregatedMeanCoreTime[coreId] - embraceableLoad;
            if (DEBUG) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] * Calculating increase recomendations");
                RUNTIME_LOGGER.debug("\tRemaining load = " + aggregatedMeanCoreTime[coreId] + "-( " + totalSlots[coreId]
                    + " * " + creationTime + " ) = " + remainingLoad);
            }
            if (remainingLoad > 0) {
                creations[coreId] = (int) (remainingLoad / creationTime);
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("\tRecomended slots = " + creations[coreId] + " ( " + remainingLoad + " / "
                        + creationTime + " )");
                }
            } else {
                creations[coreId] = 0;
            }
        }
        return creations;
    }

    private float[] orderCreations(int coreCount, long creationTime, long[] aggregatedMinCoreTime,
        long[] aggregatedMeanCoreTime, long[] aggregatedMaxCoreTime, int[] totalSlots, int[] realSlots) {

        float[] creations = new float[coreCount];
        int maxI = 0;
        float maxRatio = 0;
        for (int i = 0; i < CoreManager.getCoreCount(); i++) {
            if (aggregatedMeanCoreTime[i] > 0 && totalSlots[i] > 0) {
                float ratio = aggregatedMeanCoreTime[i] / totalSlots[i];
                if (ratio > maxRatio) {
                    maxI = i;
                    maxRatio = ratio;
                }
            }
        }
        creations[maxI] = 1;
        return creations;
    }

    private float[] deleteRecommendations(int coreCount, long limitTime, long[] aggregatedMinCoreTime,
        long[] aggregatedMeanCoreTime, long[] aggregatedMaxCoreTime, int[] totalSlots, int[] realSlots) {

        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] * Delete Recomendations calculations:\n\tcoreCount: " + coreCount
                + " limitTime: " + limitTime);
        }
        float[] destructions = new float[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {

            long embraceableLoad = limitTime * (realSlots[coreId]);

            if (embraceableLoad == 0L) {
                destructions[coreId] = 0;
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("\tembraceableLoad [ " + coreId + "] = " + limitTime + " * "
                        + realSlots[coreId] + " = " + embraceableLoad);
                    RUNTIME_LOGGER.debug("\tdestructions [ " + coreId + "] = 0");
                }
            } else if (aggregatedMinCoreTime[coreId] > 0) {
                double unusedTime = (((double) embraceableLoad) / (double) 2) - (double) aggregatedMeanCoreTime[coreId];
                destructions[coreId] = (float) (unusedTime / (double) limitTime);
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("\tembraceableLoad [ " + coreId + "] = " + limitTime + " * "
                        + realSlots[coreId] + " = " + embraceableLoad);
                    RUNTIME_LOGGER.debug("\tunused [ " + coreId + "] =  ( " + embraceableLoad + " / 2) - "
                        + aggregatedMeanCoreTime[coreId] + " = " + unusedTime);
                    RUNTIME_LOGGER.debug("\tdestructions [ " + coreId + "] = " + destructions[coreId]);
                }
            } else {
                destructions[coreId] = embraceableLoad / limitTime;
            }

        }
        return destructions;
    }

    /**
     * Asks for resources.
     * 
     * @param cp Cloud provider.
     * @param instanceName Instance name to ask.
     * @param imageName Image name to ask.
     * @return ResourceCreationRequest to ask for more resources.
     */
    public ResourceCreationRequest askForResources(CloudProvider cp, String instanceName, String imageName) {
        CloudMethodResourceDescription constraints = cp.getResourceDescription(instanceName, imageName);
        if (constraints == null) {
            RUNTIME_LOGGER.warn(WARN_EXCEPTION_TURN_ON);
            return null;
        }

        return cp.requestResourceCreation(constraints, null);
    }

    /**
     * Asks for the described resources to a Cloud provider. The CloudManager checks the best resource that each
     * provider can offer. Then it picks one of them and it constructs a resourceRequest describing the resource and
     * which cores can be executed on it. This ResourceRequest will be used to ask for that resource creation to the
     * Cloud Provider and returned if the application is accepted.
     *
     * @param requirements Description of the resource expected to receive.
     * @param contained {@literal true} if we want the request to ask for a resource contained in the description; else,
     *            the result contains the passed in description.
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
     * @param amount Amount of slots.
     * @param requirements Features of the resource.
     * @param contained {@literal true} if we want the request to ask for a resource contained in the description; else,
     *            the result contains the passed in description.
     * @return A ResourceCreationRequest.
     */
    public static ResourceCreationRequest askForResources(Integer amount, MethodResourceDescription requirements,
        boolean contained) {
        // Search best resource
        CloudProvider bestProvider = null;
        CloudMethodResourceDescription bestConstraints = null;
        Float bestValue = Float.MAX_VALUE;
        for (CloudProvider cp : ResourceManager.getAvailableCloudProviders()) {
            CloudMethodResourceDescription rc = getBestIncreaseOnProvider(cp, amount, requirements, contained);
            if (rc != null && rc.getValue() < bestValue) {
                bestProvider = cp;
                bestConstraints = rc;
                bestValue = rc.getValue();
            } else if (rc != null && bestConstraints == null) {
                bestProvider = cp;
                bestConstraints = rc;
                bestValue = rc.getValue();
            } else {
                RUNTIME_LOGGER.warn(WARN_NO_POSIBLE_INCREASE + " (" + cp.getName() + ")");
            }
        }
        if (bestConstraints == null) {
            RUNTIME_LOGGER.warn(WARN_NO_RESOURCE_MATCHES);
            return null;
        }

        return bestProvider.requestResourceCreation(bestConstraints, null);
    }

    /**
     * Return the best increase on the CloudProvider to fit the requirements.
     * 
     * @param cp Cloud Provider.
     * @param amount Increase amount.
     * @param requirements Requirements to fit.
     * @param contained Whether it is contained or not.
     * @return A MethodResourceDescription describing the best increase to fit the requirements.
     */
    public static CloudMethodResourceDescription getBestIncreaseOnProvider(CloudProvider cp, Integer amount,
        MethodResourceDescription requirements, boolean contained) {

        RUNTIME_LOGGER.debug("[Resource Optimizer] Getting best increase in provider " + cp.getName());
        // Check Cloud capabilities
        if (!cp.canHostMoreInstances()) {
            RUNTIME_LOGGER.warn(WARN_NO_MORE_INSTANCES);
            return null;
        }

        // Select all the compatible types
        List<CloudInstanceTypeDescription> instances = cp.getCompatibleTypes(requirements);
        RUNTIME_LOGGER.debug("[Resource Optimizer] There are " + instances.size() + " instances compatible.");
        if (instances.isEmpty()) {
            RUNTIME_LOGGER.warn(WARN_NO_COMPATIBLE_TYPE);
            return null;
        }

        CloudMethodResourceDescription result = null;
        CloudInstanceTypeDescription type = null;
        if (contained) {
            type = selectContainedInstance(instances, requirements, amount);
        } else {
            type = selectContainingInstance(instances, requirements, amount);
        }

        // Pick an image to be loaded in the Type (or return null)
        if (type != null) {
            // Select all the compatible images
            List<CloudImageDescription> images = cp.getCompatibleImages(requirements);
            RUNTIME_LOGGER.debug("[Resource Optimizer] There are " + images.size() + " images compatible.");
            if (!images.isEmpty()) {
                CloudImageDescription image = images.get(0);
                result = new CloudMethodResourceDescription(type, image);
                result.setValue(cp.getInstanceCostPerHour(result));
            } else {
                RUNTIME_LOGGER.warn(WARN_NO_COMPATIBLE_IMAGE);
            }
        } else {
            RUNTIME_LOGGER.warn(WARN_NO_VALID_INSTANCE);
        }

        return result;
    }

    private static CloudInstanceTypeDescription selectContainingInstance(List<CloudInstanceTypeDescription> instances,
        MethodResourceDescription constraints, int amount) {

        CloudInstanceTypeDescription result = null;
        MethodResourceDescription bestDescription = null;
        float bestDistance = Integer.MIN_VALUE;

        for (CloudInstanceTypeDescription type : instances) {
            MethodResourceDescription rd = type.getResourceDescription();
            int slots = rd.canHostSimultaneously(constraints);
            float distance = slots - amount;
            RUNTIME_LOGGER.debug("[Resource Optimizer] Can host: slots = " + slots + " amount = " + amount
                + " distance = " + distance + " bestDistance = " + bestDistance);
            if (distance > 0.0) {
                continue;
            }

            if (distance > bestDistance) {
                result = type;
                bestDescription = type.getResourceDescription();
                bestDistance = distance;
            } else if (distance == bestDistance && bestDescription != null) {
                if (bestDescription.getValue() != null && rd.getValue() != null
                    && bestDescription.getValue() > rd.getValue()) {
                    // Evaluate optimal candidate
                    result = type;
                    bestDescription = type.getResourceDescription();
                    bestDistance = distance;
                }
            }
        }

        if (result == null) {
            return null;
        }
        return result;
    }

    private static CloudInstanceTypeDescription selectContainedInstance(List<CloudInstanceTypeDescription> instances,
        MethodResourceDescription constraints, int amount) {

        CloudInstanceTypeDescription bestType = null;
        MethodResourceDescription bestDescription = null;
        float bestDistance = Integer.MAX_VALUE;

        for (CloudInstanceTypeDescription type : instances) {
            MethodResourceDescription rd = type.getResourceDescription();
            int slots = rd.canHostSimultaneously(constraints);
            float distance = slots - amount;
            RUNTIME_LOGGER.debug("[Resource Optimizer] Can host: slots = " + slots + " amount = " + amount
                + " distance = " + distance + " bestDistance = " + bestDistance);
            if (distance < 0.0) {
                continue;
            }

            if (distance < bestDistance) {
                bestType = type;
                bestDescription = type.getResourceDescription();
                bestDistance = distance;
            } else if (distance == bestDistance && bestDescription != null) {
                if (bestDescription.getValue() != null && rd.getValue() != null
                    && bestDescription.getValue() > rd.getValue()) {
                    // Evaluate optimal candidate
                    bestType = type;
                    bestDescription = type.getResourceDescription();
                    bestDistance = distance;
                }
            }

        }

        if (bestType == null) {
            return null;
        }
        return bestType;
    }

    /**
     * Given a set of resources, it checks every possible modification of the resource and returns the one that better
     * fits with the destruction recommendations. The decision-making algorithm tries to minimize the number of affected
     * CE that weren't recommended to be modified, minimize the number of slots that weren't requested to be destroyed
     * and maximize the number of slots that can be removed and they were requested for.
     *
     * @param resourceSet set of resources
     * @param destroyRecommendations number of slots to be removed for each CE
     * @return an object array defining the best solution. 0-> (Resource) selected Resource.
     *         1->(CloudTypeInstanceDescription) Type to be destroyed to be destroyed. 2-> (int[]) record of the #CE
     *         with removed slots and that they shouldn't be modified, #slots that will be destroyed and they weren't
     *         recommended, #slots that will be removed and they were asked to be.
     */
    private ReductionOption getBestDestruction(Collection<DynamicMethodWorker> resourceSet,
        float[] destroyRecommendations) {

        float bestUndesiredCEs = Float.MAX_VALUE;
        float bestUndesiredSlots = Float.MAX_VALUE;
        float bestDesiredSlots = Float.MAX_VALUE;

        ReductionOption bestOption = null;

        for (DynamicMethodWorker res : resourceSet) {

            List<ReductionOption> reductions = getPossibleReductions(res, destroyRecommendations);

            for (ReductionOption option : reductions) {
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("Type: " + option.getName() + " value 0: " + option.getUndesiredCEsAffected()
                        + " (" + bestUndesiredCEs + ") " + " value 1: " + option.getUndesiredSlotsAffected() + " ("
                        + bestUndesiredSlots + ") " + " value 2: " + option.getDesiredSlotsAffected() + " ("
                        + bestDesiredSlots + ")");
                }
                if (bestUndesiredCEs == option.getUndesiredCEsAffected()) {
                    if (bestUndesiredSlots == option.getUndesiredSlotsAffected()) {
                        if (bestDesiredSlots < option.getDesiredSlotsAffected()) {
                            bestOption = option;
                        }
                    } else if (bestUndesiredSlots > option.getUndesiredSlotsAffected()) {
                        bestOption = option;
                    }
                } else if (bestUndesiredCEs > option.getUndesiredCEsAffected()) {
                    bestOption = option;
                }
            }
        }

        if (bestOption != null) {
            return bestOption;
        } else {
            RUNTIME_LOGGER.warn("Best resource to remove not found");
            return null;
        }
    }

    // Type -> [# modified CE that weren't requested,
    // #slots removed that weren't requested,
    // #slots removed that were requested]
    private List<ReductionOption> getPossibleReductions(DynamicMethodWorker res, float[] recommendedSlots) {
        List<ReductionOption> reductions = new LinkedList<>();

        MethodResourceDescription description = res.getDescription();
        List<CloudInstanceTypeDescription> types;
        if (res instanceof CloudMethodWorker) {
            types = ((CloudMethodResourceDescription) description).getPossibleReductions();
            for (CloudInstanceTypeDescription type : types) {
                ReductionOption option = new CloudReductionOption(type, res);
                int[] reducedSlots = type.getSlotsCore();
                for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                    if (res.canRun(coreId)) {
                        if (recommendedSlots[coreId] < 1 && reducedSlots[coreId] > 0) {
                            option.undesiredCEAffected(); // Adding a desired CE whose slots will be destroyed
                            option.undesiredSlotsAffected(reducedSlots[coreId]);// all reduced slots weren't requested
                        } else {
                            float dif = (float) reducedSlots[coreId] - recommendedSlots[coreId];
                            if (dif < 0) {
                                option.desiredSlotsAffected(reducedSlots[coreId]);
                            } else {
                                option.desiredSlotsAffected(recommendedSlots[coreId]);
                                option.undesiredSlotsAffected(dif);
                            }
                        }
                    }
                }
                reductions.add(option);
            }
        } else {
            ReductionOption option = new ReductionOption(res);
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                int[] reducedSlots = res.getSimultaneousTasks();
                if (reducedSlots[coreId] > 0) {
                    if (recommendedSlots[coreId] < 1 && reducedSlots[coreId] > 0) {
                        option.undesiredCEAffected(); // Adding a desired CE whose slots will be destroyed
                        option.undesiredSlotsAffected(reducedSlots[coreId]);// all reduced slots weren't requested
                    } else {
                        float dif = (float) reducedSlots[coreId] - recommendedSlots[coreId];
                        if (dif < 0) {
                            option.desiredSlotsAffected(reducedSlots[coreId]);
                        } else {
                            option.desiredSlotsAffected(recommendedSlots[coreId]);
                            option.undesiredSlotsAffected(dif);
                        }
                    }
                }
            }
            reductions.add(option);
        }

        return reductions;
    }


    private class ReductionOption {

        private final Resource res;

        private float undesiredCEsAffected = 0;// values[0]
        private float undesiredSlotsAffected = 0; // values[1]
        private float desiredSlotsAffected = 0; // values[2]


        public ReductionOption(Resource res) {
            this.res = res;
        }

        public void undesiredCEAffected() {
            undesiredCEsAffected++;
        }

        public void undesiredSlotsAffected(float slots) {
            undesiredSlotsAffected += slots;
        }

        public void desiredSlotsAffected(float slots) {
            desiredSlotsAffected += slots;
        }

        public float getUndesiredCEsAffected() {
            return undesiredCEsAffected;
        }

        public float getUndesiredSlotsAffected() {
            return undesiredSlotsAffected;
        }

        public float getDesiredSlotsAffected() {
            return desiredSlotsAffected;
        }

        public String getName() {
            return res.getName();
        }

        public void apply() {
            ResourceManager.requestWholeWorkerReduction((DynamicMethodWorker) res);
        }

        public Resource getResource() {
            return res;
        }

    }

    private class CloudReductionOption extends ReductionOption {

        private final CloudInstanceTypeDescription type;


        public CloudReductionOption(CloudInstanceTypeDescription type, Resource res) {
            super(res);
            this.type = type;
        }

        @Override
        public String getName() {
            return type.getName() + "@" + super.getName();
        }

        @Override
        public void apply() {
            CloudMethodWorker res = ((CloudMethodWorker) getResource());
            CloudMethodResourceDescription cmrd = (CloudMethodResourceDescription) res.getDescription();
            CloudImageDescription cid = cmrd.getImage();

            CloudMethodResourceDescription finalDescription = new CloudMethodResourceDescription(type, cid);
            finalDescription.setName(res.getName());
            ResourceManager.requestWorkerReduction(res, finalDescription);
        }
    }

    // Private Classes
    protected class CloudTypeProfile {

        private Profile[][] implProfiles;


        public CloudTypeProfile(JSONObject typeJSON, JSONObject implsJSON) {
            this.implProfiles = loadProfiles(typeJSON, implsJSON);
        }

        public Profile getImplProfiles(int coreId, int implId) {
            return this.implProfiles[coreId][implId];
        }

        /**
         * Prepares the default profiles for each implementation cores.
         *
         * @param resMap default profile values for the resource.
         * @param implMap default profile values for the implementation.
         * @return default profile structure.
         */
        private final Profile[][] loadProfiles(JSONObject resMap, JSONObject implMap) {
            Profile[][] profiles;
            int coreCount = CoreManager.getCoreCount();
            profiles = new Profile[coreCount][];
            for (CoreElement ce : CoreManager.getAllCores()) {
                int coreId = ce.getCoreId();
                List<Implementation> impls = ce.getImplementations();
                int implCount = impls.size();
                profiles[coreId] = new Profile[implCount];
                for (Implementation impl : impls) {
                    String signature = impl.getSignature();
                    JSONObject jsonImpl = null;
                    if (resMap != null) {
                        try {
                            jsonImpl = resMap.getJSONObject(signature);
                            profiles[coreId][impl.getImplementationId()] =
                                generateProfileForImplementation(impl, jsonImpl);
                        } catch (JSONException je) {
                            // Do nothing
                        }
                    }
                    if (profiles[coreId][impl.getImplementationId()] == null) {
                        if (implMap != null) {
                            try {
                                jsonImpl = implMap.getJSONObject(signature);
                            } catch (JSONException je) {
                                // Do nothing
                            }
                        }
                        profiles[coreId][impl.getImplementationId()] = generateProfileForImplementation(impl, jsonImpl);
                        profiles[coreId][impl.getImplementationId()].clearExecutionCount();
                    }
                }
            }
            return profiles;
        }

        /**
         * Generates a Profile for an action.
         *
         * @param impl Action implementation.
         * @param jsonImpl JSON Implementation definition.
         * @return A profile object for an action.
         */
        protected Profile generateProfileForImplementation(Implementation impl, JSONObject jsonImpl) {
            return new Profile(jsonImpl);
        }

    }

    private static class ConstraintsCore {

        private CloudMethodResourceDescription desc;
        private List<ConstraintsCore>[] cores;


        @SuppressWarnings("unchecked")
        public ConstraintsCore(CloudMethodResourceDescription desc, int core, List<ConstraintsCore> coreList) {
            this.desc = desc;
            this.cores = new LinkedList[CoreManager.getCoreCount()];
            this.cores[core] = coreList;
        }

        public void join(ConstraintsCore c2) {
            desc.increase(c2.desc);
            c2.desc = desc;
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                if (cores[coreId] != null) {
                    if (c2.cores[coreId] != null) {
                        // Remove one instance of the list to avoid replication
                        cores[coreId].remove(c2);
                    } else {
                        c2.cores[coreId] = cores[coreId];
                    }
                } else {
                    cores[coreId] = c2.cores[coreId];
                }
            }
        }

        public void confirmed() {
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                if (cores[coreId] != null) {
                    cores[coreId].clear();
                }
            }
        }

        @Override
        public String toString() {
            List<Integer> cores = new LinkedList<>();
            for (int i = 0; i < CoreManager.getCoreCount(); i++) {
                if (this.cores[i] != null) {
                    cores.add(i);
                }
            }
            return desc.toString() + " meets constraints for cores " + cores;
        }
    }

    private static class ValueResourceDescription implements Comparable<ValueResourceDescription> {

        private final MethodResourceDescription constraints;
        private final float value;
        private final boolean prioritary;


        public ValueResourceDescription(MethodResourceDescription constraints, float value, boolean prioritary) {
            this.constraints = constraints;
            this.value = value;
            this.prioritary = prioritary;
        }

        @Override
        public int compareTo(ValueResourceDescription o) {
            if (this.prioritary && !o.prioritary) {
                return 1;
            }

            if (!this.prioritary && o.prioritary) {
                return -1;
            }

            float dif = value - o.value;
            if (dif > 0) {
                return 1;
            } else if (dif < 0) {
                return -1;
            } else {
                return 0;
            }
        }

        @Override
        public String toString() {
            return value + (prioritary ? "!" : "") + constraints;
        }
    }

}
