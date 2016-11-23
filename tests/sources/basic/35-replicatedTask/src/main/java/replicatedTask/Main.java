package replicatedTask;

import types.Pair;


public class Main {

    private static final int WAIT_RUNTIME = 5_000;
    private static final int SIZE = 8;


    public static void main(String[] args) {

        // ----------------------------------------------------------------------------
        // Wait for Runtime to have both workers available
        try {
            Thread.sleep(WAIT_RUNTIME);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // ----------------------------------------------------------------------------
        // Launch an initial non global task
        System.out.println("[LOG] Launch init task");
        Pair initialP = new Pair();
        MainImpl.initInitialP(initialP);

        // ----------------------------------------------------------------------------
        // Launch a global task
        System.out.println("[LOG] Launch global task test");
        Pair p = MainImpl.globalTask(initialP, 2);

        // ----------------------------------------------------------------------------
        // Launch a dependent bunch of tasks
        System.out.println("[LOG] Launch normal tasks");
        for (int i = 0; i < SIZE; ++i) {
            MainImpl.normalTask(p);
        }

    }

}
