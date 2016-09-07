package fileTransfer;

public class FaultTolerance {

    private static int N;


    public static void main(String[] args) {
        // Check arguments
        if (args.length != 1) {
            System.out.println("[ERROR] Bad number of arguments");
            System.out.println("Usage: fileTransfer <NumTasks>");
            System.exit(-1);
        }
        N = Integer.parseInt(args[0]);

        // Running File Transfer Test
        System.out.println("[LOG] Running File Transfer test.");
        for (int i = 0; i < N; ++i) {
            FaultToleranceImpl.taskLocalhost();
        }
        for (int i = 0; i < N; ++i) {
            FaultToleranceImpl.taskHostDown();
        }

        System.out.println("[LOG] All tests finished.");
        System.out.println("[LOG] No more jobs for main.");
    }

}
