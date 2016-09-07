package dynamicTest;

import commons.ConstantValues;


public class DynamicTest {

    private static final int NUM_TASKS = 2;


    public static void main(String[] args) {
        // Wait for Runtime to be loaded
        System.out.println("[LOG] Waiting for Runtime to be loaded");
        try {
            Thread.sleep(ConstantValues.WAIT_FOR_RUNTIME_TIME);
        } catch (Exception e) {
            // No need to handle such exceptions
        }

        // Run Dynamic Test
        System.out.println("[LOG] Running Dynamic Test");
        // Launch NUM_TASKS tasks on Core constraints Core = 3
        System.out.println("[LOG] Creating tasks Core = 3");
        for (int i = 0; i < NUM_TASKS; ++i) {
            DynamicTestImpl.coreElementDynamic1();
        }

        // Launch NUM_TASKS tasks on Core constraints Core = 1
        System.out.println("[LOG] Creating tasks Core = 1");
        for (int i = 0; i < NUM_TASKS; ++i) {
            DynamicTestImpl.coreElementDynamic2();
        }

        // Result is checked on runtime.log
        System.out.println("[LOG] Main program finished. No more tasks.");
    }

}
