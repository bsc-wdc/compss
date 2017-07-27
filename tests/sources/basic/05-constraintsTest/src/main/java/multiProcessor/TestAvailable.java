package multiProcessor;

import java.util.LinkedList;
import java.util.Map.Entry;

import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.MethodWorker;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.ResourceManager;

import commons.Action;
import commons.ConstantValues;


/**
 * Checks the dynamic constraint management. Uses the two coreElements defined in the interface allocating them to a
 * unique Worker (XML files)
 */
public class TestAvailable {

    private static final String NAME_CORE_ELEMENT_1 = "coreElement1";
    private static final String NAME_CORE_ELEMENT_2 = "coreElement2";
    private static final String NAME_CORE_ELEMENT_3 = "coreElement3";
    private static final String NAME_CORE_ELEMENT_4 = "coreElement4";
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
     * *************************************** AVAILABLE RESOURCES TEST IMPLEMENTATION
     * 
     * Resource 4 CPU CUs (internalMemory=1), 3GPUs CUS (internalMemory=2), 3 FPGA CUs, 3 OTHER CUs CE1 -> 2 CPU CUs, 2
     * GPU CUs (internalMemory=1), 1 FPGA CU) CE2 -> 1 CPU CUs, 1 FPGA , 2 OTHER CUs , nodeMemSize= 2.0) CE3 -> 1 CPU
     * CUs, 2 GPU CU (internalMemory=3) CE4 -> 1 CPU CUs (internalMemory=3); ***************************************
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
        boolean found_ce3 = false;
        boolean found_ce4 = false;
        int ce1 = 0;
        int ce2 = 0;
        int ce3 = 0;
        int ce4 = 0;
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
            if (coreToName[i].equals(NAME_CORE_ELEMENT_3)) {
                ce3 = i;
                found_ce3 = true;
            }
            if (coreToName[i].equals(NAME_CORE_ELEMENT_4)) {
                ce4 = i;
                found_ce4 = true;
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
        if (!found_ce4) {
            System.out.println("[ERROR] " + NAME_CORE_ELEMENT_4 + " not found.");
            System.exit(-1);
        }

        Worker worker = ResourceManager.getWorker(NAME_WORKER);

        System.out.println("Worker " + NAME_WORKER + ": " + worker.getDescription());

        /*
         * ************************************************* Check internal memory
         * ***********************************************
         */
        ActionOrchestrator orchestrator = COMPSsRuntimeImpl.getOrchestrator();
        Action a = new Action(orchestrator, ce3);
        if (a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources internalMemorySize filter inside Processor annotation is not working");
            System.exit(-1);
        }

        a = new Action(orchestrator, ce4);
        if (a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources processorInternalMemorySize filter is not working");
            System.exit(-1);
        }

        /*
         * ************************************************* Reserve and free for GPU and FPGA computingUnits test
         * ***********************************************
         */
        System.out.println("Worker " + NAME_WORKER + ": " + worker.getDescription());
        System.out.println("Implementation 1: " + CoreManager.getCoreImplementations(ce1).get(0));

        MethodResourceDescription consumed1 = (MethodResourceDescription) worker
                .runTask(CoreManager.getCoreImplementations(ce1).get(0).getRequirements());

        System.out.println("CONSUMED: " + consumed1);
        // Check Consumed: 2 CPUs 2 GPUs 1 FPGA
        if (!checkDescription(consumed1, 2, 2, 1, 0)) {
            System.out.println("[ERROR] consumed resources for CPU + GPU + FPGA is not working");
            System.exit(-1);
        }
        ;
        MethodResourceDescription remaining = ((MethodWorker) worker).getAvailable();
        System.out.println("REMAINING: " + remaining);
        // Check Remaining: 2CPUs 1GPU , 2FPGA 3 OTHER
        if (!checkDescription(remaining, 2, 1, 2, 3)) {
            System.out.println("[ERROR] remaining resources for CPU + GPU + FPGA is not working");
            System.exit(-1);
        }
        MethodResourceDescription consumed2 = (MethodResourceDescription) worker
                .runTask(CoreManager.getCoreImplementations(ce2).get(0).getRequirements());
        System.out.println("CONSUMED: " + consumed2);

        // Check consumed 1 CPU 2 OTHER 1 FPGA
        if (!checkDescription(consumed2, 1, 0, 1, 2)) {
            System.out.println("[ERROR] consumed resources for CPU + OTHER + FPGA is not working");
            System.exit(-1);
        }
        remaining = ((MethodWorker) worker).getAvailable();
        System.out.println("REMAINING: " + remaining);
        // Check remaining 1CPU 1 GPU, 1 OTHER, 1 FPGA.
        if (!checkDescription(remaining, 1, 1, 1, 1)) {
            System.out.println("[ERROR] remaining resources for CPU + OTHER + FPGA is not working");
            System.exit(-1);
        }

        a = new Action(orchestrator, ce1);
        if (a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources for CPU + GPU + FPGA reserve is not working");
            System.exit(-1);
        }

        a = new Action(orchestrator, ce2);
        if (a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources for CPU + OTHER + FPGA reserve is not working");
            System.exit(-1);
        }

        worker.endTask(consumed1);
        a = new Action(orchestrator, ce1);
        if (!a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources for CPU + GPU + FPGA free is not working");
            System.exit(-1);
        }

        worker.endTask(consumed2);
        a = new Action(orchestrator, ce2);
        if (!a.findAvailableWorkers().containsKey(worker)) {
            System.out.println("[ERROR] Available resources for CPU + OTHER + FPGA free is not working");
            System.exit(-1);
        }

        // System.out.println("FREE");
        // System.out.println("FREE");
        // System.out.println("TOTAL: " + ((MethodWorker)worker).getAvailable());
        // System.out.println();
        System.out.println("[LOG] * Multi-processors test passed");
    }

    private static boolean checkDescription(MethodResourceDescription description, int CPUcus, int GPUcus, int FPGAcus, int OTHERcus) {

        return description != null && description.getTotalCPUComputingUnits() == CPUcus && description.getTotalGPUComputingUnits() == GPUcus
                && description.getTotalFPGAComputingUnits() == FPGAcus && description.getTotalOTHERComputingUnits() == OTHERcus;

    }

}
