package resourceManager;

import java.util.LinkedList;
import java.util.Map.Entry;

import integratedtoolkit.api.impl.COMPSsRuntimeImpl;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ResourceManager;
import commons.Action;
import commons.ConstantValues;


/*
 * Checks the dynamic constraint management.
 * Uses the two coreElements defined in the interface allocating them to a unique Worker (XML files)
 */
public class TestAvailable {

    private static final String NAME_CORE_ELEMENT_1 = "coreElement1";
    private static final String NAME_CORE_ELEMENT_2 = "coreElement2";
    private static final String NAME_WORKER = "COMPSsWorker01";

    // CoreManagerData
    private static int coreCount;
    private static LinkedList<String>[] idToSignatures;
    private static String[] coreToName;


    /*
     * *************************************** MAIN IMPLEMENTATION ***************************************
     */
    public static void main(String[] args) {
        // Wait for Runtime to be loaded
        System.out.println("[LOG] Waiting for Runtime to be loaded");
        try {
            Thread.sleep(ConstantValues.WAIT_FOR_RUNTIME_TIME);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Run Available Resource Manager Test
        System.out.println("[LOG] Running Available Resource Manager Test");
        availableResourcesTest();
    }

    /*
     * ********************************************************************************************************
     * AVAILABLE RESOURCES TEST IMPLEMENTATION
     * ********************************************************************************************************
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static void availableResourcesTest() {
        // Get CoreCount
        coreCount = CoreManager.getCoreCount();

        // Loading Core names from the interface
        idToSignatures = new LinkedList[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            idToSignatures[coreId] = new LinkedList<String>();
        }
        for (Entry<String, Integer> entry : CoreManager.getSignaturesToId().entrySet()) {
            String signature = entry.getKey();
            Integer coreId = entry.getValue();
            idToSignatures[coreId].add(signature);
        }

        // Search for the specific CoreElement ids
        boolean found_ce1 = false;
        boolean found_ce2 = false;
        int ce1 = 0;
        int ce2 = 0;
        coreToName = new String[coreCount];
        for (int i = 0; i < coreCount; i++) {
            int cutValue = idToSignatures[i].getFirst().indexOf("(");
            coreToName[i] = idToSignatures[i].getFirst().substring(0, cutValue);
            if (coreToName[i].equals(NAME_CORE_ELEMENT_1)) {
                ce1 = i;
                found_ce1 = true;
            }
            if (coreToName[i].equals(NAME_CORE_ELEMENT_2)) {
                ce2 = i;
                found_ce2 = true;
            }
        }

        // Check results
        if (!found_ce1) {
            System.out.println("[ERROR] " + NAME_CORE_ELEMENT_1 + " not found.");
            System.exit(-1);
        }
        if (!found_ce2) {
            System.out.println("[ERROR] " + NAME_CORE_ELEMENT_2 + " not found.");
            System.exit(-1);
        }

        /*
         * ********************************************************************************************************
         * Reserve and free for computingUnits test
         * ********************************************************************************************************
         */
        Worker worker = ResourceManager.getWorker(NAME_WORKER);

        System.out.println("Worker " + NAME_WORKER + ": " + worker.getDescription());
        System.out.println("Implementation 1: " + CoreManager.getCoreImplementations(ce1).get(0));

        WorkerResourceDescription consumed1 = worker.runTask(CoreManager.getCoreImplementations(ce1).get(0).getRequirements());
        WorkerResourceDescription consumed2 = worker.runTask(CoreManager.getCoreImplementations(ce1).get(0).getRequirements());

        System.out.println("CONSUMED: " + consumed1);
        System.out.println("CONSUMED: " + consumed2);
        // System.out.println("REMAINING: " + ((MethodWorker)worker).getAvailable());

        ActionOrchestrator<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>> orchestrator = (ActionOrchestrator<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>>) COMPSsRuntimeImpl
                .getOrchestrator();
        Action a = new Action(orchestrator, ce1);
        if (a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources for CORE reserve is not working");
            System.exit(-1);
        }

        worker.endTask(consumed1);
        if (!a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources for CORE free is not working");
            System.exit(-1);
        }
        worker.endTask(consumed2);

        // System.out.println("FREE");
        // System.out.println("FREE");
        // System.out.println("TOTAL: " + ((MethodWorker)worker).getAvailable());
        // System.out.println();

        /*
         * ********************************************************************************************************
         * Reserve and free for memorySize test
         * ********************************************************************************************************
         */
        a = new Action(orchestrator, ce2);
        // System.out.println("Worker " + NAME_WORKER + ": " + worker.getDescription());
        // System.out.println("Implementation 1: " + CoreManager.getCoreImplementations(ce2)[0]);

        consumed1 = worker.runTask(CoreManager.getCoreImplementations(ce2).get(0).getRequirements());
        consumed2 = worker.runTask(CoreManager.getCoreImplementations(ce2).get(0).getRequirements());

        // System.out.println("CONSUMED: " + consumed1);
        // System.out.println("CONSUMED: " + consumed2);
        // System.out.println("REMAINING: " + ((MethodWorker)worker).getAvailable());

        if (a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources for MEMORY reserve is not working");
            System.exit(-1);
        }

        worker.endTask(consumed1);
        if (!a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources for MEMORY free is not working");
            System.exit(-1);
        }
        worker.endTask(consumed2);

        // System.out.println("FREE");
        // System.out.println("FREE");
        // System.out.println("TOTAL: " + ((MethodWorker)worker).getAvailable());
        // System.out.println();

        System.out.println("[LOG] * Available Resources test passed");
    }

}
