package distributedTask;

import integratedtoolkit.api.COMPSs;


public class Main {

    private static final int WAIT_RUNTIME = 10_000;
    private static final int NUM_WORKERS = 2;


    public static void main(String[] args) {

        // ----------------------------------------------------------------------------
        // Wait for Runtime to have both workers available
        try {
            Thread.sleep(WAIT_RUNTIME);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ----------------------------------------------------------------------------
        // Launch 2 normal tasks (should go to the same worker)
        System.out.println("[LOG] Launch normal tasks");
        String msg = "Hello World!";
        for (int i = 0; i < NUM_WORKERS; ++i) {
            MainImpl.normalTask(msg);
        }

        // ----------------------------------------------------------------------------
        // Wait completion
        System.out.println("[LOG] Wait for normal tasks completion");
        COMPSs.barrier();

        // ----------------------------------------------------------------------------
        // Launch 2 distributed tasks (should go 1 to each worker)
        System.out.println("[LOG] Launch distributed tasks");
        for (int i = 0; i < NUM_WORKERS; ++i) {
            MainImpl.distributedTask(msg);
        }

    }

}
