package dynamicTest;

public class DynamicTestImpl {

    private static final int SLEEP_TIME = 3_000; // ms


    public static void coreElementDynamic1() {
        System.out.println("Running coreElementDynamic1.");
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (Exception e) {
            // No need to handle such exceptions
        }
    }

    public static void coreElementDynamic2() {
        System.out.println("Running coreElementDynamic2.");
        try {
            Thread.sleep(SLEEP_TIME);
        } catch (Exception e) {
            // No need to handle such exceptions
        }
    }
}
