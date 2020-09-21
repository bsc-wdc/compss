package basicTest;

public class DataLocality {

    private static int numTasks;
    private static String fileName;


    public static void main(String[] args) {
        // Check arguments
        if (args.length != 2) {
            System.out.println("[ERROR] Bad number of parameters");
            System.out.println("Usage: dataLocality <numTasks> <outputFile>");
            System.exit(-1);
        }
        // Add for test stability
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            // Nothing to do
        }
        // Throw tasks
        numTasks = Integer.parseInt(args[0]);
        fileName = args[1];
        System.out.println("[LOG] Creating single chain of tasks.");
        for (int i = 0; i < numTasks; ++i) {
            DataLocalityImpl.task(i, fileName);
        }

        // End result
        System.out.println("[LOG] No more work for main app. Waiting tasks to finish");
        System.out.println("[LOG] Result must be checked (script)");
    }

}
