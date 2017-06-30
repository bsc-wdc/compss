package integratedtoolkit.util;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.types.WorkloadState;
import integratedtoolkit.types.CloudProvider;
import integratedtoolkit.types.resources.description.CloudInstanceTypeDescription;
import integratedtoolkit.types.resources.description.CloudImageDescription;
import integratedtoolkit.types.ResourceCreationRequest;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.components.Processor;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourceOptimizer extends Thread {

    //WARNING MESSAGES
    private static final String WARN_NO_RESOURCE_MATCHES = "WARN: No resource matches the constraints";
    private static final String WARN_NO_COMPATIBLE_TYPE = "WARN: Cannot find any compatible instanceType";
    private static final String WARN_NO_COMPATIBLE_IMAGE = "WARN: Cannot find any compatible Image";
    private static final String WARN_NO_VALID_INSTANCE = "WARN: Cannot find a containing/contained instanceType";
    private static final String WARN_EXCEPTION_TURN_ON = "WARN: Connector exception on turn on resource";

    // Loggers
    protected static final Logger RESOURCES_LOGGER = LogManager.getLogger(Loggers.RESOURCES);
    protected static final Logger RUNTIME_LOGGER = LogManager.getLogger(Loggers.RM_COMP);
    protected static final boolean DEBUG = RUNTIME_LOGGER.isDebugEnabled();

    // Sleep times
    private static final int SLEEP_TIME = 10_000;
    private static final int EVERYTHING_BLOCKED_MAX_RETRIES = 3;

    // Error messages
    private static final String ERROR_OPT_RES = "Error optimizing resources.";
    private static final String PERSISTENT_BLOCK_ERR = "Unschedulable tasks detected.\n"
            + "COMPSs has found tasks with constraints that cannot be fulfilled.\n"
            + "Shutting down COMPSs now...";

    protected final TaskScheduler ts;
    private boolean running;

    private static boolean cleanUp;
    private static boolean redo;

    // This counts the number of retries when detecting a situation
    // where all the tasks are blocked, and no resources are being created/can be created.
    // (if you don't handle this situation, the runtime gets blocked)
    // The first run execution won't take into account this check.
    // That's why it's initialized to -1, to know when it's the first run.
    private int everythingBlockedRetryCount = -1;

    public ResourceOptimizer(TaskScheduler ts) {
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Initializing Resource Optimizer");
        }
        this.setName("ResourceOptimizer");
        this.ts = ts;
        redo = false;
        RUNTIME_LOGGER.info("[Resource Optimizer] Initialization finished");
    }

    @Override
    public final void run() {
        running = true;
        if (ResourceManager.useCloud()) {
            initialCreations();
        }

        WorkloadState workload;
        while (running) {
            try {
                do {
                    redo = false;
                    workload = ts.getWorkload();

                    int blockedTasks = workload.getNoResourceCount();
                    boolean potentialBlock = (blockedTasks > 0);

                    if (ResourceManager.useCloud()) {
                        applyPolicies(workload);

                        // There is a potentialBlock in cloud only if all the possible VMs have been created
                        int VMsBeingCreated = ResourceManager.getPendingCreationRequests().size();
                        potentialBlock = potentialBlock && (VMsBeingCreated == 0);
                    }

                    handlePotentialBlock(potentialBlock);

                } while (redo);

                try {
                    synchronized (this) {
                        if (running) {
                            this.wait(SLEEP_TIME);
                        }
                    }
                } catch (InterruptedException ex) {
                    //Do nothing. It was interrupted to trigger another optimization
                }

            } catch (Exception e) {
                RUNTIME_LOGGER.error(ERROR_OPT_RES, e);
            }
        }
    }

    public final void optimizeNow() {
        synchronized (this) {
            this.notify();
            redo = true;
        }
    }

    public void cleanUp() {
        cleanUp = true;
    }

    public final void shutdown() {
        synchronized (this) {
            // Stop
            running = false;
            this.notify();
            cleanUp = true;
        }

        // Print status
        RESOURCES_LOGGER.info(ts.getWorkload());
    }

    public final void handlePotentialBlock(boolean potentialBlock) {
        if (potentialBlock) { // All tasks are blocked, and there are no resources available...
            ++everythingBlockedRetryCount;
            if (everythingBlockedRetryCount > 0) { // First time not taken into account
                if (everythingBlockedRetryCount < EVERYTHING_BLOCKED_MAX_RETRIES) {
                    // Retries limit not reached. Warn the user...
                    int retriesLeft = EVERYTHING_BLOCKED_MAX_RETRIES - everythingBlockedRetryCount;
                    ErrorManager.warn(
                            "No task could be scheduled to any of the available resources.\n"
                            + "This could end up blocking COMPSs. Will check it again in " + (SLEEP_TIME / 1_000) + " seconds.\n"
                            + "Possible causes: \n"
                            + "    -Network problems: non-reachable nodes, sshd service not started, etc.\n"
                            + "    -There isn't any computing resource that fits the defined tasks constraints.\n"
                            + "If this happens " + retriesLeft + " more time" + (retriesLeft > 1 ? "s" : "") + ", the runtime will shutdown."
                    );
                } else {
                    // Retry limit reached. Error and shutdown.
                    ErrorManager.error(PERSISTENT_BLOCK_ERR);
                }
            }
        } else {
            everythingBlockedRetryCount = 0;
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
     * Triggers the creation of the initial VMs to acomplish the expected
     * minimum VMs and ensure that there are workers to run every type of task
     */
    protected void initialCreations() {
        int alreadyCreated = addBasicNodes();
        // Distributes the rest of the VM
        addExtraNodes(alreadyCreated);
    }

    /**
     * Asks for the VM needed for the runtime to be able to execute all method
     * cores.
     *
     * First it groups the constraints of all the methods per Architecture and
     * tries to merge included resource descriptions in order to reduce the
     * amount of required VMs. It also tries to join the unassigned architecture
     * methods with the closer constraints of a defined one. After that it
     * distributes the Initial VM Count among the architectures taking into
     * account the number of methods that can be run in each architecture.
     *
     * If the amount of different constraints is higher than the Initial VM
     * count it applies an agressive merge method to each architecture in order
     * to fulfill the initial Constraint. It creates a single VM for each final
     * method constraint.
     *
     * Although these aggressive merges, the amount of different constraints can
     * be higher than the initial VM Count constraint. In this case, it violates
     * the initial VM constraint and asks for more resources.
     *
     * @return the amount of requested VM
     */
    public static int addBasicNodes() {
        int coreCount = CoreManager.getCoreCount();
        LinkedList<ConstraintsCore>[] unfulfilledConstraints = getUnfulfilledConstraints();
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
         * constraintsPerArquitecture has loaded all constraint for each task.
         * architectures has a list of all the architecture names.
         *
         * e.g.
         * architectures                     constraintsPerArquitecture
         * Intel                =               |MR1|--|MR2|
         * AMD                  =               |MR3|
         * [unassigned]         =               |MR4|--|MR5|
         */
        HashMap<String, LinkedList<ConstraintsCore>> arch2Constraints = classifyArchitectures(unfulfilledConstraints);

        /*
         * Tries to reduce the number of machines per architecture by
         * entering constraints in another core's constraints
         *
         */
        reduceArchitecturesConstraints(arch2Constraints);

        /*
         * Checks if there are enough Vm for a Unassigned Arquitecture
         * If not it set each unassigned task into the architecture with the most similar task
         * e.g.
         * constraintsPerArquitecture
         * Intel --> |MR1|--|MR2|--|MR5|
         * AMD --> |MR3|--|MR4|
         *
         */
        reassignUnassignedConstraints(arch2Constraints);

        /*
         * Tries to reduce the number of machines per architecture by
         * entering constraints in another core's constraints
         *
         */
        reduceArchitecturesConstraints(arch2Constraints);

        int createdCount = 0;
        for (int coreId = 0; coreId < coreCount; coreId++) {
            while (!unfulfilledConstraints[coreId].isEmpty()) {
                ConstraintsCore cc = unfulfilledConstraints[coreId].removeFirst();
                cc.confirmed();
                ResourceCreationRequest rcr = askForResources(cc.desc, false);
                if (rcr != null) {
                    StringBuilder compositionString = new StringBuilder();
                    for (java.util.Map.Entry<CloudInstanceTypeDescription, int[]> entry : rcr.getRequested().getTypeComposition().entrySet()) {
                        compositionString.append(" \t\tTYPE = [\n")
                                .append("\t\t\tNAME = ").append(entry.getKey().getName())
                                .append("\t\t\tCOUNT= ").append(entry.getValue()[0])
                                .append("\t\t]\n");
                    }
                    RESOURCES_LOGGER.info("ORDER_CREATION = [\n"
                            + "\tTYPE = " + compositionString.toString() + "\n"
                            + "\tPROVIDER = " + rcr.getProvider().getName() + "\n"
                            + "]"
                    );
                    if (DEBUG) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("EXPECTED_SIM_TASKS = [").append("\n");
                        for (int i = 0; i < rcr.requestedSimultaneousTaskCount().length; i++) {
                            for (int j = 0; j < rcr.requestedSimultaneousTaskCount()[i].length; ++j) {
                                sb.append("\t").append("IMPLEMENTATION_INFO = [").append("\n");
                                sb.append("\t\t").append("COREID = ").append(i).append("\n");
                                sb.append("\t\t").append("IMPLID = ").append(j).append("\n");
                                sb.append("\t\t").append("SIM_TASKS = ").append(rcr.requestedSimultaneousTaskCount()[i][j]).append("\n");
                                sb.append("\t").append("]").append("\n");
                            }
                        }
                        sb.append("]");
                        RESOURCES_LOGGER.debug(sb.toString());
                    }
                    createdCount++;
                }
            }
        }

        if (DEBUG) {
            RESOURCES_LOGGER.debug("DEBUG_MSG = [\n"
                    + "\tIn order to be able to execute all cores, Resource Manager has asked for " + createdCount + " Cloud resources\n"
                    + "]");
        }
        return createdCount;
    }

    // Removes from the list all the Constraints fullfilled by existing resources
    @SuppressWarnings("unchecked")
    private static LinkedList<ConstraintsCore>[] getUnfulfilledConstraints() {
        int coreCount = CoreManager.getCoreCount();
        LinkedList<ConstraintsCore>[] unfulfilledConstraints = new LinkedList[coreCount];
        int[] maxSimTasks = ResourceManager.getTotalSlots();
        for (int coreId = 0; coreId < coreCount; coreId++) {
            unfulfilledConstraints[coreId] = new LinkedList<>();
            if (maxSimTasks[coreId] == 0) {
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
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
     * Classifies the constraints depending on their architecture and leaves it
     * on coreResourceList
     *
     * @param constraints
     * @return list with all the architectures' names
     */
    private static HashMap<String, LinkedList<ConstraintsCore>> classifyArchitectures(LinkedList<ConstraintsCore>[] constraints) {
        HashMap<String, LinkedList<ConstraintsCore>> archs = new HashMap<>();

        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            if (constraints[coreId] != null) {
                for (ConstraintsCore cc : constraints[coreId]) {
                    List<String> runnableArchitectures = cc.desc.getArchitectures();
                    for (String arch : runnableArchitectures) {
                        // Insert element into HashMap
                        LinkedList<ConstraintsCore> archConstr = archs.get(arch);
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

    private static void reduceArchitecturesConstraints(HashMap<String, LinkedList<ConstraintsCore>> arch2Ctrs) {
        for (LinkedList<ConstraintsCore> arch : arch2Ctrs.values()) {
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
                String OS = ctrs[i].desc.getOperatingSystemType();
                for (int j = i + 1; j < ctrs.length; j++) {
                    if (mergedTo[j] != null) {
                        continue;
                    }
                    if (OS.compareTo(ctrs[j].desc.getOperatingSystemType()) == 0
                            || OS.compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0
                            || ctrs[j].desc.getOperatingSystemType().compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0) {
                        mergedTo[j] = i;
                        ctrs[i].join(ctrs[j]);
                        arch.remove(ctrs[j]);
                    }
                }
            }
        }

    }

    private static void reassignUnassignedConstraints(HashMap<String, LinkedList<ConstraintsCore>> arch2Ctrs) {
        /*
         * ATTENTION: Since this method is only evaluated from constraints, there is only 1 PROCESSOR and 1 ARCHITECTURE
         */
        LinkedList<ConstraintsCore> unassignedList = arch2Ctrs.get(CloudMethodResourceDescription.UNASSIGNED_STR);
        if (unassignedList == null) {
            return;
        }
        if (arch2Ctrs.size() == 1) {
            return;
        }
        if (arch2Ctrs.size() == 2) {
            for (Map.Entry<String, LinkedList<ConstraintsCore>> ctrs : arch2Ctrs.entrySet()) {
                if (ctrs.getKey().compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0) {
                    continue;
                } else {
                    ctrs.getValue().addAll(unassignedList);
                    return;
                }
            }
        }

        LinkedList<ConstraintsCore> assignedList = new LinkedList<>();
        for (Map.Entry<String, LinkedList<ConstraintsCore>> ctrs : arch2Ctrs.entrySet()) {
            if (ctrs.getKey().compareTo(CloudMethodResourceDescription.UNASSIGNED_STR) == 0) {
                continue;
            } else {
                assignedList.addAll(ctrs.getValue());

            }
        }

        while (!unassignedList.isEmpty()) {
            ConstraintsCore unassigned = unassignedList.removeFirst();
            CloudMethodResourceDescription candidate = unassigned.desc;
            String bestArch = CloudMethodResourceDescription.UNASSIGNED_STR;
            Float bestDifference = Float.MAX_VALUE;
            for (ConstraintsCore assigned : assignedList) {
                CloudMethodResourceDescription option = assigned.desc;
                float difference = candidate.difference(option);
                if (bestDifference < 0) {
                    if (difference < 0) {
                        if (difference > bestDifference) {
                            List<String> avail_archs = option.getArchitectures();
                            if (avail_archs != null && !avail_archs.isEmpty()) {
                                bestArch = avail_archs.get(0);
                            }
                            bestDifference = difference;
                        }
                    }
                } else if (difference < bestDifference) {
                    List<String> avail_archs = option.getArchitectures();
                    if (avail_archs != null && !avail_archs.isEmpty()) {
                        bestArch = avail_archs.get(0);
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
     * Asks for the rest of VM that user wants to start with.
     *
     * After executing the addBasicNodes, it might happen that the number of
     * initial VMs constrained by the user is still not been fulfilled. The
     * addBasicNodes creates up to as much VMs as different methods. If the
     * initial VM Count is higher than this number of methods then there will be
     * still some VM requests missing.
     *
     * The addExtraNodes creates this difference of VMs. First it tries to merge
     * the method constraints that are included into another methods. And
     * performs a less aggressive and more equal distribution.
     *
     * @param alreadyCreated number of already requested VMs
     * @return the number of extra VMs created to fulfill the Initial VM Count
     * constraint
     */
    public static int addExtraNodes(int alreadyCreated) {
        int initialVMsCount = ResourceManager.getInitialCloudVMs();
        int vmCount = initialVMsCount - alreadyCreated;
        if (vmCount <= 0) {
            return 0;
        }

        if (DEBUG) {
            RESOURCES_LOGGER.debug("DEBUG_MSG = [\n"
                    + "\tALREADY_CREATED_INSTANCES = " + alreadyCreated + "\n"
                    + "\tMAXIMUM_NEW_PETITIONS = " + vmCount + "\n"
                    + "]");
        }

        /*
         * Tries to reduce the number of machines by
         * entering methodConstraints in another method's machine
         *
         */
 /*
        LinkedList<CloudWorkerDescription> requirements = new LinkedList<CloudWorkerDescription>();
        for (int coreId = 0; coreId < CoreManager.coreCount; coreId++) {
            Implementation impl = CoreManager.getCoreImplementations(coreId)[0];
            if (impl.getType() == Type.METHOD) {
                WorkerDescription wd = (WorkerDescription) impl.getRequirements();
                requirements.add(new CloudWorkerDescription(wd));
            }
        }
        if (requirements.size() == 0) {
            return 0;
        }
        requirements = reduceConstraints(requirements);

        int numTasks = requirements.size();
        int[] vmCountPerContraint = new int[numTasks];
        int[] coreCountPerConstraint = new int[numTasks];

        for (int index = 0; index < numTasks; index++) {
            vmCountPerContraint[index] = 1;
            coreCountPerConstraint[index] = requirements.get(index).getSlots();
        }

        for (int i = 0; i < vmCount; i++) {
            float millor = 0.0f;
            int opcio = 0;
            for (int j = 0; j < requirements.size(); j++) {
                if (millor < ((float) coreCountPerConstraint[j] / (float) vmCountPerContraint[j])) {
                    opcio = j;
                    millor = ((float) coreCountPerConstraint[j] / (float) vmCountPerContraint[j]);
                }
            }
            ResourceCreationRequest rcr = CloudManager.askForResources(requirements.get(opcio), false);

            LOGGER.info(
                    "CREATION_ORDER = [\n"
                    + "\tTYPE = " + rcr.getRequested().getType() + "\n"
                    + "\tPROVIDER = " + rcr.getProvider() + "\n"
                    + "\tREASON = Fulfill the initial Cloud instances constraint\n"
                    + "]");
            if (DEBUG) {
                StringBuilder sb = new StringBuilder("EXPECTED_INSTANCE_SIM_TASKS = [");
                int[][] simultaneousImpls = rcr.requestedSimultaneousTaskCount();
                for (int core = 0; core < simultaneousImpls.length; ++core) {
                    int simultaneousTasks = 0;
                    for (int j = 0; j < simultaneousImpls[core].length; ++j) {
                        if (simultaneousTasks < simultaneousImpls[core][j]) {
                            simultaneousTasks = simultaneousImpls[core][j];
                        }
                    }
                    sb.append("\t").append("CORE = [").append("\n");
                    sb.append("\t").append("\t").append("COREID = ").append(core).append("\n");
                    sb.append("\t").append("\t").append("SIM_TASKS = ").append(simultaneousTasks).append("\n");
                    sb.append("\t").append("]").append("\n");
                    sb.append(", ").append(simultaneousTasks);
                }
                sb.append("]");
                logger.debug(sb.toString());
            }

            vmCountPerContraint[opcio]++;
        }
         */
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
            creationTime = 120_000l;
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
        }
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Applying VM optimization policies (currentVMs: " + currentCloudVMCount + " maxVMs: "
                    + maxNumberOfVMs + " minVMs: " + minNumberOfVMs + ")");
        }

        // Check if there is some mandatory creation/destruction
        if (!cleanUp) {
            // For CE without resources where to run
            LinkedList<Integer> requiredVMs = checkNeededMachines(noResourceCount, noResourceCounts, totalSlots);

            if (!requiredVMs.isEmpty()) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] Required VMs. Mandatory Increase");
                float[] creationRecommendations = recommendCreations(coreCount, creationTime, readyMinCoreTime, readyMeanCoreTime,
                        readyMaxCoreTime, totalSlots, realSlots);
                for (Integer coreId : requiredVMs) {
                    // Ensure that we ask one slot for it
                    creationRecommendations[coreId] = Math.max(creationRecommendations[coreId], 1);
                }
                mandatoryIncrease(creationRecommendations, requiredVMs);
                return;
            }

            // For accomplishing the minimum amount of vms
            if (minNumberOfVMs != null && minNumberOfVMs > currentCloudVMCount) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount + ") count smaller than minimum VMs ("
                        + minNumberOfVMs + "). Mandatory Increase");
                float[] creationRecommendations = orderCreations(coreCount, creationTime, readyMinCoreTime, readyMeanCoreTime,
                        readyMaxCoreTime, totalSlots, realSlots);
                mandatoryIncrease(creationRecommendations, new LinkedList<>());
                return;
            }
            // For not exceeding the VM top limit
            if (maxNumberOfVMs != null && maxNumberOfVMs < currentCloudVMCount) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount + ") count bigger than maximum VMs ("
                        + maxNumberOfVMs + "). Mandatory reduction");
                float[] destroyRecommendations = deleteRecommendations(coreCount, SLEEP_TIME, pendingMinCoreTime, pendingMeanCoreTime,
                        pendingMaxCoreTime, totalSlots, realSlots);
                mandatoryReduction(destroyRecommendations);
                return;
            } else {
            }
        }

        // Check Recommended creations
        if ((maxNumberOfVMs == null || maxNumberOfVMs > currentCloudVMCount) && workload.getReadyCount() > 1) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Current VM (" + currentCloudVMCount + ") count smaller than maximum VMs (" + maxNumberOfVMs
                    + ")");
            float[] creationRecommendations = recommendCreations(coreCount, creationTime, readyMinCoreTime, readyMeanCoreTime,
                    readyMaxCoreTime, totalSlots, realSlots);
            if (optionalIncrease(creationRecommendations)) {
                return;
            }
        }
        // Check Recommended destructions
        if ((minNumberOfVMs == null || minNumberOfVMs < currentCloudVMCount) && totalPendingTasks == 0 && workload.getReadyCount() == 0) {
            RUNTIME_LOGGER.debug(
                    "[Resource Optimizer] Current VM (" + currentCloudVMCount + ") count bigger than minimum VMs (" + minNumberOfVMs + ")");
            float[] destroyRecommendations = deleteRecommendations(coreCount, SLEEP_TIME, pendingMinCoreTime, pendingMeanCoreTime,
                    pendingMaxCoreTime, totalSlots, realSlots);
            if (optionalReduction(destroyRecommendations)) {
                return;
            }
        }
    }

    private void mandatoryIncrease(float[] creationRecommendations, LinkedList<Integer> requiredVMs) {
        PriorityQueue<ValueResourceDescription> pq = new PriorityQueue<>();

        boolean[] required = new boolean[creationRecommendations.length];
        for (int coreId : requiredVMs) {
            required[coreId] = true;
        }

        for (int coreId = 0; coreId < creationRecommendations.length; coreId++) {
            List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
            for (Implementation impl : impls) {
                if (impl.getTaskType() == TaskType.SERVICE) {
                    continue;
                }

                MethodResourceDescription constraints = ((MethodImplementation) impl).getRequirements();
                ValueResourceDescription v = new ValueResourceDescription(constraints, creationRecommendations[coreId], false);
                pq.add(v);
            }
        }
        requestOneCreation(pq, true);
    }

    private boolean optionalIncrease(float[] creationRecommendations) {
        PriorityQueue<ValueResourceDescription> pq = new PriorityQueue<>();

        for (int coreId = 0; coreId < creationRecommendations.length; coreId++) {
            if (creationRecommendations[coreId] > 1) {
                List<Implementation> impls = CoreManager.getCoreImplementations(coreId);
                for (Implementation impl : impls) {
                    if (impl.getTaskType() == TaskType.SERVICE) {
                        continue;
                    }
                    MethodResourceDescription constraints = ((MethodImplementation) impl).getRequirements();
                    ValueResourceDescription v = new ValueResourceDescription(constraints, creationRecommendations[coreId], false);
                    pq.add(v);
                }
            }
        }

        ResourceCreationRequest rcr = requestOneCreation(pq, false);
        return (rcr != null);
    }

    private boolean optionalReduction(float[] destroyRecommendations) {
        LinkedList<CloudMethodWorker> nonCritical = trimReductionOptions(ResourceManager.getNonCriticalDynamicResources(),
                destroyRecommendations);
        Object[] nonCriticalSolution = getBestDestruction(nonCritical, destroyRecommendations);

        CloudMethodWorker res;
        float[] record;
        CloudMethodResourceDescription rd;
        // int[][] slotsRemovingCount;

        if (nonCriticalSolution == null) {
            return false;
        }
        res = (CloudMethodWorker) nonCriticalSolution[0];
        record = (float[]) nonCriticalSolution[1];
        // slotsRemovingCount = (int[][]) nonCriticalSolution[2];
        rd = (CloudMethodResourceDescription) nonCriticalSolution[3];
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Best resource to remove is " + res.getName() + "and record is [" + record[0] + ","
                    + record[1] + "," + record[2]);
        }
        if (record[1] > 0 && res.getUsedCPUTaskCount() > 0) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Optional destroy recommendation not applied");
            return false;
        } else {
            RUNTIME_LOGGER.debug("[Resource Optimizer] Optional destroy recommendation applied");
            CloudMethodResourceDescription finalDescription = rd;
            finalDescription.setName(res.getName());
            ResourceManager.reduceCloudWorker(res, finalDescription);
            return true;
        }
    }

    private void mandatoryReduction(float[] destroyRecommendations) {
        LinkedList<CloudMethodWorker> critical = trimReductionOptions(ResourceManager.getCriticalDynamicResources(),
                destroyRecommendations);
        // LinkedList<CloudMethodWorker> critical = checkCriticalSafeness (critical);
        LinkedList<CloudMethodWorker> nonCritical = trimReductionOptions(ResourceManager.getNonCriticalDynamicResources(),
                destroyRecommendations);
        Object[] criticalSolution = getBestDestruction(critical, destroyRecommendations);
        Object[] nonCriticalSolution = getBestDestruction(nonCritical, destroyRecommendations);

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
            float[] noncriticalValues = (float[]) nonCriticalSolution[2];
            float[] criticalValues = (float[]) criticalSolution[2];

            if (noncriticalValues[0] == criticalValues[0]) {
                if (noncriticalValues[1] == criticalValues[1]) {
                    if (noncriticalValues[2] < criticalValues[2]) {
                        criticalIsBetter = true;
                    }
                } else if (noncriticalValues[1] > criticalValues[1]) {
                    criticalIsBetter = true;
                }
            } else if (noncriticalValues[0] > criticalValues[0]) {
                criticalIsBetter = true;
            }
        }

        CloudMethodWorker res;
        CloudMethodResourceDescription cmrd;
        // float[] record;
        // int[][] slotsRemovingCount;
        CloudInstanceTypeDescription citd;

        if (criticalIsBetter) {
            res = (CloudMethodWorker) criticalSolution[0];
            citd = (CloudInstanceTypeDescription) criticalSolution[1];
        } else {
            if (nonCriticalSolution == null) {
                return;
            }
            res = (CloudMethodWorker) nonCriticalSolution[0];
            citd = (CloudInstanceTypeDescription) nonCriticalSolution[1];
        }
        cmrd = (CloudMethodResourceDescription) res.getDescription();
        CloudImageDescription cid = cmrd.getImage();

        CloudMethodResourceDescription finalDescription = new CloudMethodResourceDescription(citd, cid);
        finalDescription.setName(res.getName());
        ResourceManager.reduceCloudWorker(res, finalDescription);
    }

    private LinkedList<CloudMethodWorker> trimReductionOptions(Collection<CloudMethodWorker> options, float[] recommendations) {
        if (DEBUG) {
            RUNTIME_LOGGER.debug("[Resource Optimizer] * Trimming reduction options");
        }
        LinkedList<CloudMethodWorker> resources = new LinkedList<>();
        Iterator<CloudMethodWorker> it = options.iterator();
        while (it.hasNext()) {

            CloudMethodWorker resource = it.next();
            boolean aggressive = false;// (resource.getUsedTaskCount() == 0);
            boolean add = !aggressive;
            if (DEBUG) {
                RUNTIME_LOGGER.debug("\tEvaluating " + resource.getName() + ". Default reduction is " + add);
            }
            LinkedList<Integer> executableCores = resource.getExecutableCores();
            for (int coreId : executableCores) {

                if (!aggressive && recommendations[coreId] < 1) {
                    if (DEBUG) {
                        RUNTIME_LOGGER.debug(
                                "\t\tVM not removed because of not agressive and recomendations < 1 (" + recommendations[coreId] + ")");
                    }
                    add = false;
                    break;
                }
                if (aggressive && recommendations[coreId] > 0) {
                    if (DEBUG) {
                        RUNTIME_LOGGER.debug("\t\tVM removed because of agressive and recomendations > 0 (" + recommendations[coreId] + ")");
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

    private static ResourceCreationRequest requestOneCreation(PriorityQueue<ValueResourceDescription> pq, boolean include) {
        ValueResourceDescription v;
        while ((v = pq.poll()) != null) {
            ResourceCreationRequest rcr = askForResources(v.value < 1 ? 1 : (int) v.value, v.constraints, include);
            if (rcr != null) {
                StringBuilder compositionString = new StringBuilder();
                for (java.util.Map.Entry<CloudInstanceTypeDescription, int[]> entry : rcr.getRequested().getTypeComposition().entrySet()) {
                    compositionString.append(" \t\tTYPE = [\n")
                            .append("\t\t\tNAME = ").append(entry.getKey().getName())
                            .append("\t\t\tCOUNT= ").append(entry.getValue()[0])
                            .append("\t\t]\n");
                }
                RESOURCES_LOGGER.info(
                        "ORDER_CREATION = [\n"
                        + "\tTYPE_COMPOSITION = [" + compositionString.toString() + "]\n"
                        + "\tPROVIDER = " + rcr.getProvider() + "\n"
                        + "]"
                );
                if (DEBUG) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("EXPECTED_SIM_TASKS = [").append("\n");
                    for (int i = 0; i < CoreManager.getCoreCount(); i++) {
                        for (int j = 0; j < rcr.requestedSimultaneousTaskCount()[i].length; ++j) {
                            sb.append("\t").append("IMPLEMENTATION_INFO = [").append("\n");
                            sb.append("\t\t").append("COREID = ").append(i).append("\n");
                            sb.append("\t\t").append("IMPLID = ").append(j).append("\n");
                            sb.append("\t\t").append("SIM_TASKS = ").append(rcr.requestedSimultaneousTaskCount()[i][j]).append("\n");
                            sb.append("\t").append("]").append("\n");
                        }
                    }
                    sb.append("]");
                    RESOURCES_LOGGER.debug(sb.toString());
                }
                return rcr;
            }
        }

        return null;
    }

    /**
     * *****************************************************************************************************************
     * *****************************************************************************************************************
     * DYNAMIC RESOURCES RECOMMENDATIONS
     * *****************************************************************************************************************
     * *****************************************************************************************************************
     */
    private LinkedList<Integer> checkNeededMachines(int noResourceCount, int[] noResourceCountPerCore, int[] slotCountPerCore) {
        LinkedList<Integer> needed = new LinkedList<>();
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

    private float[] recommendCreations(int coreCount, long creationTime, long[] aggregatedMinCoreTime, long[] aggregatedMeanCoreTime,
            long[] aggregatedMaxCoreTime, int[] totalSlots, int[] realSlots) {

        float[] creations = new float[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            // long realTime = realSlots[coreId] * creationTime;
            long totalTime = totalSlots[coreId] * creationTime;
            // OLD version: did a mean of total (include pending creations) and real
            // long embraceableLoad = (realTime + totalTime) / 2;
            long embraceableLoad = totalTime;
            long remainingLoad = aggregatedMeanCoreTime[coreId] - embraceableLoad;
            if (DEBUG) {
                RUNTIME_LOGGER.debug("[Resource Optimizer] * Calculating increase recomendations");
                RUNTIME_LOGGER.debug("\tRemaining load = " + aggregatedMeanCoreTime[coreId] + "-( " + totalSlots[coreId] + " * " + creationTime
                        + " ) = " + remainingLoad);
            }
            if (remainingLoad > 0) {
                creations[coreId] = (int) (remainingLoad / creationTime);
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("\tRecomended slots = " + creations[coreId] + " ( " + remainingLoad + " / " + creationTime + " )");
                }
            } else {
                creations[coreId] = 0;
            }
        }
        return creations;
    }

    private float[] orderCreations(int coreCount, long creationTime, long[] aggregatedMinCoreTime, long[] aggregatedMeanCoreTime,
            long[] aggregatedMaxCoreTime, int[] totalSlots, int[] realSlots) {

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

    private float[] deleteRecommendations(int coreCount, long limitTime, long[] aggregatedMinCoreTime, long[] aggregatedMeanCoreTime,
            long[] aggregatedMaxCoreTime, int[] totalSlots, int[] realSlots) {

        if (DEBUG) {
            RUNTIME_LOGGER.debug(
                    "[Resource Optimizer] * Delete Recomendations calculations:\n\tcoreCount: " + coreCount + " limitTime: " + limitTime);
        }
        float[] destructions = new float[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {

            long embraceableLoad = limitTime * (realSlots[coreId]);

            if (embraceableLoad == 0l) {
                destructions[coreId] = 0;
                if (DEBUG) {
                    RUNTIME_LOGGER.debug(
                            "\tembraceableLoad [ " + coreId + "] = " + limitTime + " * " + realSlots[coreId] + " = " + embraceableLoad);
                    RUNTIME_LOGGER.debug("\tdestructions [ " + coreId + "] = 0");
                }
            } else if (aggregatedMinCoreTime[coreId] > 0) {
                double unusedTime = (((double) embraceableLoad) / (double) 2) - (double) aggregatedMeanCoreTime[coreId];
                destructions[coreId] = (float) (unusedTime / (double) limitTime);
                if (DEBUG) {
                    RUNTIME_LOGGER.debug("\tembraceableLoad [ " + coreId + "] = " + limitTime + " * " + realSlots[coreId] + " = " + embraceableLoad);
                    RUNTIME_LOGGER.debug("\tunused [ " + coreId + "] =  ( " + embraceableLoad + " / 2) - " + aggregatedMeanCoreTime[coreId] + " = " + unusedTime);
                    RUNTIME_LOGGER.debug("\tdestructions [ " + coreId + "] = " + destructions[coreId]);
                }
            } else {
                destructions[coreId] = embraceableLoad / limitTime;
            }

        }
        return destructions;
    }

    private static class ConstraintsCore {

        private CloudMethodResourceDescription desc;
        private LinkedList<ConstraintsCore>[] cores;

        @SuppressWarnings("unchecked")
        public ConstraintsCore(CloudMethodResourceDescription desc, int core, LinkedList<ConstraintsCore> coreList) {
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
            LinkedList<Integer> cores = new LinkedList<>();
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

    public ResourceCreationRequest askForResources(CloudProvider cp, String instanceName, String imageName) {
        CloudMethodResourceDescription constraints = cp.getResourceDescription(instanceName, imageName);
        if (constraints == null) {
            RUNTIME_LOGGER.warn(WARN_EXCEPTION_TURN_ON);
            return null;
        }

        return cp.requestResourceCreation(constraints);
    }

    /**
     * Asks for the described resources to a Cloud provider. The CloudManager
     * checks the best resource that each provider can offer. Then it picks one
     * of them and it constructs a resourceRequest describing the resource and
     * which cores can be executed on it. This ResourceRequest will be used to
     * ask for that resource creation to the Cloud Provider and returned if the
     * application is accepted.
     *
     * @param requirements description of the resource expected to receive
     * @param contained {@literal true} if we want the request to ask for a
     * resource contained in the description; else, the result contains the
     * passed in description.
     * @return Description of the ResourceRequest sent to the CloudProvider.
     * {@literal Null} if any of the Cloud Providers can offer a resource like
     * the requested one.
     */
    public static ResourceCreationRequest askForResources(MethodResourceDescription requirements, boolean contained) {
        return askForResources(1, requirements, contained);
    }

    /**
     * The CloudManager ask for resources that can execute certain amount of
     * cores at the same time. It checks the best resource that each provider
     * can offer to execute that amount of cores and picks one of them. It
     * constructs a resourceRequest describing the resource and which cores can
     * be executed on it. This ResourceRequest will be used to ask for that
     * resource creation to the Cloud Provider and returned if the application
     * is accepted.
     *
     * @param amount amount of slots
     * @param requirements features of the resource
     * @param contained {@literal true} if we want the request to ask for a
     * resource contained in the description; else, the result contains the
     * passed in description.
     * @return
     */
    public static ResourceCreationRequest askForResources(Integer amount, MethodResourceDescription requirements, boolean contained) {
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
            }
        }
        if (bestConstraints == null) {
            RUNTIME_LOGGER.warn(WARN_NO_RESOURCE_MATCHES);
            return null;
        }

        return bestProvider.requestResourceCreation(bestConstraints);
    }

    public static CloudMethodResourceDescription getBestIncreaseOnProvider(CloudProvider cp, Integer amount, MethodResourceDescription requirements, boolean contained) {
        // Check Cloud capabilities
        if (!cp.canHostMoreInstances()) {
            return null;
        }

        // Select all the compatible types
        LinkedList<CloudInstanceTypeDescription> instances = cp.getCompatibleTypes(requirements);
        if (instances.isEmpty()) {
            RESOURCES_LOGGER.warn(WARN_NO_COMPATIBLE_TYPE);
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
            LinkedList<CloudImageDescription> images = cp.getCompatibleImages(requirements);
            if (!images.isEmpty()) {
                CloudImageDescription image = images.get(0);
                result = new CloudMethodResourceDescription(type, image);
                result.setValue(cp.getInstanceCostPerHour(result));
            } else {
                RESOURCES_LOGGER.warn(WARN_NO_COMPATIBLE_IMAGE);
            }
        } else {
            RESOURCES_LOGGER.warn(WARN_NO_VALID_INSTANCE);
        }

        return result;
    }

    private static CloudInstanceTypeDescription selectContainingInstance(LinkedList<CloudInstanceTypeDescription> instances,
            MethodResourceDescription constraints, int amount) {

        CloudInstanceTypeDescription result = null;
        MethodResourceDescription bestDescription = null;
        float bestDistance = Integer.MIN_VALUE;

        for (CloudInstanceTypeDescription type : instances) {
            MethodResourceDescription rd = type.getResourceDescription();
            int slots = rd.canHostSimultaneously(constraints);
            float distance = slots - amount;
            RESOURCES_LOGGER.debug("Can host: slots = " + slots + " amount = " + amount + " distance = " + distance + " bestDistance = " + bestDistance);
            if (distance > 0.0) {
                continue;
            }

            if (distance > bestDistance) {
                result = type;
                bestDescription = type.getResourceDescription();
                bestDistance = distance;
            } else if (distance == bestDistance && bestDescription != null) {
                if (bestDescription.getValue() != null && rd.getValue() != null && bestDescription.getValue() > rd.getValue()) {
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

    private static CloudInstanceTypeDescription selectContainedInstance(LinkedList<CloudInstanceTypeDescription> instances,
            MethodResourceDescription constraints, int amount) {

        CloudInstanceTypeDescription bestType = null;
        MethodResourceDescription bestDescription = null;
        float bestDistance = Integer.MAX_VALUE;

        for (CloudInstanceTypeDescription type : instances) {
            MethodResourceDescription rd = type.getResourceDescription();
            int slots = rd.canHostSimultaneously(constraints);
            float distance = slots - amount;
            RESOURCES_LOGGER.debug("Can host: slots = " + slots + " amount = " + amount + " distance = " + distance + " bestDistance = " + bestDistance);
            if (distance < 0.0) {
                continue;
            }

            if (distance < bestDistance) {
                bestType = type;
                bestDescription = type.getResourceDescription();
                bestDistance = distance;
            } else if (distance == bestDistance && bestDescription != null) {
                if (bestDescription.getValue() != null && rd.getValue() != null && bestDescription.getValue() > rd.getValue()) {
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
     * Given a set of resources, it checks every possible modification of the
     * resource and returns the one that better fits with the destruction
     * recommendations.
     *
     * The decision-making algorithm tries to minimize the number of affected CE
     * that weren't recommended to be modified, minimize the number of slots
     * that weren't requested to be destroyed and maximize the number of slots
     * that can be removed and they were requested for.
     *
     * @param resourceSet set of resources
     * @param destroyRecommendations number of slots to be removed for each CE
     * @return an object array defining the best solution.
     *
     * 0-> (Resource) selected Resource.
     *
     * 1->(CloudTypeInstanceDescription) Type to be destroyed to be destroyed.
     *
     * 2-> (int[]) record of the #CE with removed slots and that they shouldn't
     * be modified, #slots that will be destroyed and they weren't recommended,
     * #slots that will be removed and they were asked to be.
     *
     */
    private Object[] getBestDestruction(Collection<CloudMethodWorker> resourceSet, float[] destroyRecommendations) {
        CloudProvider cp;
        float[] bestRecord = new float[3];
        bestRecord[0] = Float.MAX_VALUE;
        bestRecord[1] = Float.MAX_VALUE;
        bestRecord[2] = Float.MIN_VALUE;
        Resource bestResource = null;
        CloudInstanceTypeDescription bestType = null;

        for (CloudMethodWorker res : resourceSet) {
            cp = res.getProvider();
            if (cp == null) { // it's not a cloud machine
                continue;
            }

            HashMap<CloudInstanceTypeDescription, float[]> typeToPoints = getPossibleReductions(res, destroyRecommendations);

            for (Map.Entry<CloudInstanceTypeDescription, float[]> destruction : typeToPoints.entrySet()) {
                CloudInstanceTypeDescription type = destruction.getKey();
                float[] values = destruction.getValue();
                if (bestRecord[0] == values[0]) {
                    if (bestRecord[1] == values[1]) {
                        if (bestRecord[2] < values[2]) {
                            bestRecord = values;
                            bestResource = res;
                            bestType = type;
                        }
                    } else if (bestRecord[1] > values[1]) {
                        bestRecord = values;
                        bestResource = res;
                        bestType = type;
                    }
                } else if (bestRecord[0] > values[0]) {
                    bestRecord = values;
                    bestResource = res;
                    bestType = type;
                }
            }
        }
        if (bestResource != null) {
            Object[] ret = new Object[3];
            ret[0] = bestResource;
            ret[1] = bestType;
            ret[2] = bestRecord;
            return ret;
        } else {
            return null;
        }
    }

    // Type -> [# modified CE that weren't requested,
    // #slots removed that weren't requested,
    // #slots removed that were requested]
    private HashMap<CloudInstanceTypeDescription, float[]> getPossibleReductions(CloudMethodWorker res, float[] recommendedSlots) {
        HashMap<CloudInstanceTypeDescription, float[]> reductions = new HashMap<>();
        LinkedList<CloudInstanceTypeDescription> types = res.getDescription().getPossibleReductions();

        for (CloudInstanceTypeDescription type : types) {
            int[] reducedSlots = type.getSlotsCore();
            float[] values = new float[3];
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                if (res.canRun(coreId)) {
                    if (recommendedSlots[coreId] < 1 && reducedSlots[coreId] > 0) {
                        values[0]++; // Adding a desired CE whose slots will be
                        // destroyed
                        values[1] += reducedSlots[coreId]; // all reduced slots
                        // weren't requested
                    } else {
                        float dif = (float) reducedSlots[coreId] - recommendedSlots[coreId];
                        if (dif < 0) {
                            values[2] += reducedSlots[coreId];
                        } else {
                            values[2] += recommendedSlots[coreId];
                            values[1] += dif;
                        }
                    }
                }
            }
            reductions.put(type, values);
        }
        return reductions;
    }

}
