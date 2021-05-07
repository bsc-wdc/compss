
package resourceManager;

import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.ResourceManager;

import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import commons.Action;


/**
 * Checks the dynamic constraint management. Uses the two coreElements defined in the interface allocating them to a
 * unique Worker (XML files)
 */
public class TestAvailable {

    private static final int WAIT_FOR_RUNTIME_TIME = 10_000; // ms

    private static final String NAME_CORE_ELEMENT_1 = "coreElement1";
    private static final String NAME_CORE_ELEMENT_2 = "coreElement2";
    private static final String NAME_CORE_ELEMENT_3 = "coreElement3";
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
            Thread.sleep(WAIT_FOR_RUNTIME_TIME);
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
    @SuppressWarnings({ "unchecked",
        "rawtypes" })
    private static void availableResourcesTest() {
        // Get CoreCount
        coreCount = CoreManager.getCoreCount();

        // Loading Core names from the interface
        idToSignatures = new LinkedList[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            idToSignatures[coreId] = new LinkedList<>();
        }
        for (Entry<String, Integer> entry : CoreManager.getSignaturesToCeAndImpls().entrySet()) {
            String signature = entry.getKey();
            Integer coreId = entry.getValue();
            idToSignatures[coreId].add(signature);
        }

        // Search for the specific CoreElement ids
        boolean found_ce1 = false;
        boolean found_ce2 = false;
        boolean found_ce3 = false;
        int ceId1 = 0;
        int ceId2 = 0;
        int ceId3 = 0;
        coreToName = new String[coreCount];
        for (int i = 0; i < coreCount; i++) {
            int cutValue = idToSignatures[i].getFirst().indexOf("(");
            coreToName[i] = idToSignatures[i].getFirst().substring(0, cutValue);
            if (coreToName[i].equals(NAME_CORE_ELEMENT_1)) {
                ceId1 = i;
                found_ce1 = true;
            }
            if (coreToName[i].equals(NAME_CORE_ELEMENT_2)) {
                ceId2 = i;
                found_ce2 = true;
            }
            if (coreToName[i].equals(NAME_CORE_ELEMENT_3)) {
                ceId3 = i;
                found_ce3 = true;
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
        if (!found_ce3) {
            System.out.println("[ERROR] " + NAME_CORE_ELEMENT_3 + " not found.");
            System.exit(-1);
        }
        /*
         * ********************************************************************************************************
         * Reserve and free for computingUnits test
         * ********************************************************************************************************
         */
        Worker worker = ResourceManager.getWorker(NAME_WORKER);

        CoreElement ce1 = CoreManager.getCore(ceId1);
        List<Implementation> ce1Impls = ce1.getImplementations();
        // System.out.println("Worker " + NAME_WORKER + ": " + worker.getDescription());
        // System.out.println("Implementation 1: " + ce1Impls.get(0));

        WorkerResourceDescription consumed1 = worker.runTask(ce1Impls.get(0).getRequirements());
        WorkerResourceDescription consumed2 = worker.runTask(ce1Impls.get(0).getRequirements());

        // System.out.println("CONSUMED: " + consumed1);
        // System.out.println("CONSUMED: " + consumed2);
        // System.out.println("REMAINING: " + ((MethodWorker)worker).getAvailable());

        ActionOrchestrator orchestrator = COMPSsRuntimeImpl.getOrchestrator();
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
        CoreElement ce2 = CoreManager.getCore(ceId2);
        List<Implementation> ce2Impls = ce2.getImplementations();
        a = new Action(orchestrator, ce2);
        // System.out.println("Worker " + NAME_WORKER + ": " + worker.getDescription());
        // System.out.println("Implementation 1: " + ce2Impls.get(0));

        consumed1 = worker.runTask(ce2Impls.get(0).getRequirements());
        consumed2 = worker.runTask(ce2Impls.get(0).getRequirements());

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
        /*
         * ********************************************************************************************************
         * Reserve and free for bandwidth test
         * ********************************************************************************************************
         */
        CoreElement ce3 = CoreManager.getCore(ceId3);
        List<Implementation> ce3Impls = ce3.getImplementations();
        a = new Action(orchestrator, ce3);
        // System.out.println("Worker " + NAME_WORKER + ": " + worker.getDescription());
        // System.out.println("Implementation 1: " + ce2Impls.get(0));

        consumed1 = worker.runTask(ce3Impls.get(0).getRequirements());
        consumed2 = worker.runTask(ce3Impls.get(0).getRequirements());

        // System.out.println("CONSUMED: " + consumed1);
        // System.out.println("CONSUMED: " + consumed2);
        // System.out.println("REMAINING: " + ((MethodWorker)worker).getAvailable());
        if (a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources for STORAGEBW reserve is not working");
            System.exit(-1);
        }

        worker.endTask(consumed1);
        if (!a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources for STORAGEBW free is not working");
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
