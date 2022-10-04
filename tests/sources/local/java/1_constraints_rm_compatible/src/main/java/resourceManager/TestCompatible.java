package resourceManager;

import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.MethodWorker;
import es.bsc.compss.types.resources.ResourceType;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.util.CoreManager;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import commons.Action;
import es.bsc.compss.types.CoreElement;


/**
 * Checks the resources information stored inside the Runtime. Takes the project.xml / resources information and the ITF
 * information Checks the mapping between ITF CoreElement and runnable resources (static constraint check)
 */
public class TestCompatible {

    private static final int WAIT_FOR_RUNTIME_TIME = 20_000; // ms

    // Interface data
    private static int coreCount;


    /*
     * *************************************** MAIN IMPLEMENTATION ***************************************
     */
    public static void main(String[] args) {
        // Wait for Runtime to be loaded
        System.out.println("[LOG] Waiting for Runtime to be loaded");
        try {
            Thread.sleep(WAIT_FOR_RUNTIME_TIME);
        } catch (InterruptedException e) {
            // No need to handle such exceptions
        }

        // Run Compatible Resource Manager Test
        System.out.println("[LOG] Running Compatible Resource Manager Test");
        resourceManagerTest();
    }

    /*
     * **********************************************************************************************************
     * RESOURCE MANAGER TEST IMPLEMENTATION
     * **********************************************************************************************************
     */
    private static void resourceManagerTest() {
        coreCount = CoreManager.getCoreCount();

        ActionOrchestrator orchestrator = COMPSsRuntimeImpl.getOrchestrator();

        // Check for each implementation the correctness of its resources
        System.out.println("[LOG] Number of cores = " + coreCount);
        for (CoreElement ce : CoreManager.getAllCores()) {
            int coreId = ce.getCoreId();
            System.out.println("[LOG] Checking Core" + coreId + " " + ce.getSignature());

            Action a = new Action(orchestrator, ce);
            Map<Worker<?>, List<Implementation>> m = a.findAvailableWorkers();

            // For the test construction, all implementations can be run. Check it
            if (m.isEmpty()) {
                System.err.println("[ERROR] CoreId " + coreId + " cannot be run");
                for (Implementation impl : ce.getImplementations()) {
                    System.out.println("-- Impl: " + impl.getRequirements().toString());
                }
                System.exit(-1);
            }

            // Check that all assigned resources are really valid
            checkCoreResources(coreId, m);
        }

    }

    private static void checkCoreResources(int coreId, Map<Worker<?>, List<Implementation>> hm) {
        // Revert Map
        Map<Implementation, List<Worker<?>>> hm_reverted = new HashMap<>();
        for (Entry<Worker<?>, List<Implementation>> entry_hm : hm.entrySet()) {
            for (Implementation impl : entry_hm.getValue()) {
                List<Worker<?>> aux = hm_reverted.get(impl);
                if (aux == null) {
                    aux = new LinkedList<>();
                }
                aux.add(entry_hm.getKey());
                hm_reverted.put(impl, aux);
            }
        }

        // Check Resources assigned to each implementation
        for (Entry<Implementation, List<Worker<?>>> entry : hm_reverted.entrySet()) {
            System.out.println("[LOG] ** Checking Implementation " + entry.getKey());
            System.out.println("[LOG] **** Number of resources = " + entry.getValue().size());
            for (Worker<?> resource : entry.getValue()) {
                System.out.println("[LOG] **** Checking Resource " + resource.getName());
                String res = checkResourcesAssignedToImpl(entry.getKey(), resource);
                if (res != null) {
                    String error = "Implementation: Core " + coreId + " Impl " + entry.getKey().getImplementationId()
                        + " and Resource " + resource.getName() + ". ";
                    error = error.concat("Implementation and resource not matching on: " + res);
                    System.out.println(error);
                    System.exit(-1);
                }
            }
        }
    }

    private static String checkResourcesAssignedToImpl(Implementation impl, Worker<?> resource) {
        if ((impl.getTaskType().equals(TaskType.METHOD) && resource.getType().equals(ResourceType.HTTP))
            || (impl.getTaskType().equals(TaskType.HTTP) && resource.getType().equals(ResourceType.WORKER))) {
            return "types";
        }

        if (resource.getType() == ResourceType.WORKER) {
            AbstractMethodImplementation mImpl = (AbstractMethodImplementation) impl;
            MethodResourceDescription iDescription = mImpl.getRequirements();
            MethodWorker worker = (MethodWorker) resource;
            MethodResourceDescription wDescription = (MethodResourceDescription) worker.getDescription();

            // System.out.println("-- Impl Details: " + iDescription);
            // System.out.println("-- Worker Details: " + wDescription);

            /*
             * *********************************************************************************************************
             * COMPUTING UNITS
             **********************************************************************************************************/
            if ((iDescription.getTotalCPUComputingUnits() >= MethodResourceDescription.ONE_INT)
                && (wDescription.getTotalCPUComputingUnits() >= MethodResourceDescription.ONE_INT)
                && (wDescription.getTotalCPUComputingUnits() < iDescription.getTotalCPUComputingUnits())) {
                return "computingUnits";
            }

            /*
             * *********************************************************************************************************
             * PROCESSOR
             ***********************************************************************************************************/
            for (Processor ip : iDescription.getProcessors()) {
                // Check if processor can be executed in worker
                boolean canBeHosted = false;
                for (Processor wp : wDescription.getProcessors()) {
                    // Static checks
                    if (!ip.getName().equals(MethodResourceDescription.UNASSIGNED_STR)
                        && !wp.getName().equals(MethodResourceDescription.UNASSIGNED_STR)
                        && !wp.getName().equals(ip.getName())) {
                        // System.out.println("DUE TO: " + ip.getName() + " != " + wp.getName());
                        continue;
                    }
                    if (ip.getSpeed() != MethodResourceDescription.UNASSIGNED_FLOAT
                        && wp.getSpeed() != MethodResourceDescription.UNASSIGNED_FLOAT
                        && wp.getSpeed() < ip.getSpeed()) {
                        // System.out.println("DUE TO: " + ip.getSpeed() + " != " + wp.getSpeed());
                        continue;
                    }
                    if (!ip.getArchitecture().equals(MethodResourceDescription.UNASSIGNED_STR)
                        && !wp.getArchitecture().equals(MethodResourceDescription.UNASSIGNED_STR)
                        && !wp.getArchitecture().equals(ip.getArchitecture())) {
                        // System.out.println("DUE TO: " + ip.getArchitecture() + " != " + wp.getArchitecture());
                        continue;
                    }
                    if ((!ip.getPropName().equals(MethodResourceDescription.UNASSIGNED_STR))
                        && (!wp.getPropName().equals(MethodResourceDescription.UNASSIGNED_STR))
                        && (!ip.getPropName().equals(wp.getPropName()))) {
                        // System.out.println("DUE TO: " + ip.getPropName() + " != " + wp.getPropName());
                        continue;
                    }
                    if ((!ip.getPropValue().equals(MethodResourceDescription.UNASSIGNED_STR))
                        && (!wp.getPropValue().equals(MethodResourceDescription.UNASSIGNED_STR))
                        && (!ip.getPropValue().equals(wp.getPropValue()))) {
                        // System.out.println("DUE TO: " + ip.getPropValue() + " != " + wp.getPropValue());
                        continue;
                    }

                    // Dynamic checks
                    if (wp.getComputingUnits() >= ip.getComputingUnits()) {
                        canBeHosted = true;
                        break;
                    } else {
                        // System.out.println("DUE TO: " + ip.getComputingUnits() + " != " + wp.getComputingUnits());
                    }
                }
                if (!canBeHosted) {
                    return "processor";
                }
            }

            /*
             * *********************************************************************************************************
             * MEMORY
             ***********************************************************************************************************/
            if ((iDescription.getMemorySize() != MethodResourceDescription.UNASSIGNED_FLOAT)
                && (wDescription.getMemorySize() != MethodResourceDescription.UNASSIGNED_FLOAT)
                && (wDescription.getMemorySize() < iDescription.getMemorySize())) {
                return "memorySize";
            }

            if ((!iDescription.getMemoryType().equals(MethodResourceDescription.UNASSIGNED_STR))
                && (!iDescription.getMemoryType().equals(MethodResourceDescription.UNASSIGNED_STR))
                && (!wDescription.getMemoryType().equals(iDescription.getMemoryType()))) {
                return "memoryType";
            }

            /*
             * *********************************************************************************************************
             * STORAGE
             ***********************************************************************************************************/
            if ((iDescription.getStorageSize() != MethodResourceDescription.UNASSIGNED_FLOAT)
                && (wDescription.getStorageSize() != MethodResourceDescription.UNASSIGNED_FLOAT)
                && (wDescription.getStorageSize() < iDescription.getStorageSize())) {
                return "storageSize";
            }

            if ((!iDescription.getStorageType().equals(MethodResourceDescription.UNASSIGNED_STR))
                && (!iDescription.getStorageType().equals(MethodResourceDescription.UNASSIGNED_STR))
                && (!wDescription.getStorageType().equals(iDescription.getStorageType()))) {
                return "storageType";
            }

            if ((iDescription.getStorageBW() != MethodResourceDescription.UNASSIGNED_INT)
                && (wDescription.getStorageBW() != MethodResourceDescription.UNASSIGNED_INT)
                && (wDescription.getStorageBW() < iDescription.getStorageBW())) {
                return "storageBW";
            }

            /*
             * *********************************************************************************************************
             * OPERATING SYSTEM
             ***********************************************************************************************************/
            if ((!iDescription.getOperatingSystemType().equals(MethodResourceDescription.UNASSIGNED_STR))
                && (!iDescription.getOperatingSystemType().equals(MethodResourceDescription.UNASSIGNED_STR))
                && (!wDescription.getOperatingSystemType().equals(iDescription.getOperatingSystemType()))) {
                return "operatingSystemType";
            }

            if ((!iDescription.getOperatingSystemDistribution().equals(MethodResourceDescription.UNASSIGNED_STR))
                && (!iDescription.getOperatingSystemDistribution().equals(MethodResourceDescription.UNASSIGNED_STR))
                && (!wDescription.getOperatingSystemDistribution()
                    .equals(iDescription.getOperatingSystemDistribution()))) {
                return "operatingSystemDistribution";
            }

            if ((!iDescription.getOperatingSystemVersion().equals(MethodResourceDescription.UNASSIGNED_STR))
                && (!iDescription.getOperatingSystemVersion().equals(MethodResourceDescription.UNASSIGNED_STR))
                && (!wDescription.getOperatingSystemVersion().equals(iDescription.getOperatingSystemVersion()))) {
                return "operatingSystemVersion";
            }

            /*
             * *********************************************************************************************************
             * APPLICATION SOFTWARE
             ***********************************************************************************************************/
            if (!(iDescription.getAppSoftware().isEmpty())
                && !(wDescription.getAppSoftware().containsAll(iDescription.getAppSoftware()))) {
                return "appSoftware";
            }

            /*
             * *********************************************************************************************************
             * HOST QUEUE
             ***********************************************************************************************************/
            if (!(iDescription.getHostQueues().isEmpty())
                && !(wDescription.getHostQueues().containsAll(iDescription.getHostQueues()))) {
                return "hostQueues";
            }

        } else {
            return "Unknown resource type";
        }

        /*
         * ************************************************************************************************************
         * ALL CONSTAINT VALUES OK
         *************************************************************************************************************/
        return null;
    }

}
