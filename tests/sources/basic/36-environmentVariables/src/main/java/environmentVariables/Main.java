package environmentVariables;

public class Main {

    private static final int WAIT_RUNTIME = 5_000;


    public static void main(String[] args) {

        // ----------------------------------------------------------------------------
        // Wait for Runtime to have the worker available
        System.out.println("[LOG] Wait for Runtime to be ready");
        try {
            Thread.sleep(WAIT_RUNTIME);
        } catch (InterruptedException e) {
            // No need to handle such exception
        }

        // ----------------------------------------------------------------------------
        // Launch an initial non global task
        System.out.println("[LOG] Launch task");
        int result = MainImpl.task("Hello World!");

        // ----------------------------------------------------------------------------
        // Wait for task completion
        System.out.println("[LOG] Task result = " + result);

    }

}
