package taskSubmission;

public class FaultTolerance {

    private static int N;
    private static int iter;


    public static void main(String[] args) {
        // Check arguments
        if (args.length != 2) {
            System.out.println("[ERROR] Bad number of arguments");
            System.out.println("Usage: taskSubmission <NumTasks> <numIterPerTask>");
            System.exit(-1);
        }
        N = Integer.parseInt(args[0]);
        iter = Integer.parseInt(args[1]);

        // Reporting parameters
        System.out.println("[LOG] Number of tasks created: " + N);
        System.out.println("[LOG] Number of iterations per task: " + iter);

        // Running File Transfer Test
        System.out.println("[LOG] Running Task Submission Test.");
        for (int i = 0; i < N; ++i) {
            FaultToleranceImpl.task(iter);
        }

        // Reporting end of main
        System.out.println("[LOG] No more jobs for main.");
    }

}
