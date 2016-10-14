package cbm2.objects;

public class Cbm2 {

    private static void usage() {
        System.out.println(
                ":::: Usage: runcompss cbm2.objects.Cbm2 (num_Tasks) (deepness) (task_Sleep_Time) (txSizeInBytes) (INOUT | IN):::: ");
        System.out.println("Exiting cbm2...!");
    }

    public static void main(String[] args) {
        if (args.length < 5 || (!args[4].equals("INOUT") && !args[4].equals("IN"))) {
            usage();
            return;
        }

        int numTasks = Integer.parseInt(args[0]);
        int deepness = Integer.parseInt(args[1]);
        int taskSleepTime = Integer.parseInt(args[2]);
        int txSizeInBytes = Integer.parseInt(args[3]); // Size of the transference
        boolean inout = args[4].equals("INOUT"); // INOUT or IN ???

        System.out.println(":::::::::::");
        System.out.println("Number of tasks: {{" + numTasks + "}}");
        System.out.println("Deps graph deepness: {{" + deepness + "}}");
        System.out.println("Tasks sleep time: {{" + taskSleepTime + "}}");
        System.out.println("Transference size in bytes: {{" + txSizeInBytes + "}}");
        System.out.println("Execution type (INOUT || IN): {{" + (inout ? "INOUT" : "IN") + "}}");
        System.out.println("Execution type (FILES || OBJECTS): {{OBJECTS}}");
        System.out.println(":::::::::::");
        System.out.println("");
        System.out.println(":::::::::::");
        System.out.println("Starting cbm2 with objects...");

        double compssTime = System.nanoTime();

        // CREATE A DummyPayload FOR EACH TASK
        System.out.println("Creating pool of objects...");
        DummyPayload[] dummyObjects = new DummyPayload[numTasks];
        for (int i = 0; i < numTasks; ++i)
            dummyObjects[i] = new DummyPayload(txSizeInBytes);
        System.out.println("Pool of objects created.");
        System.out.println("Time for objects to be created: " + ((System.nanoTime() - compssTime) / 1000000) + " ms");
        //

        // Create tasks and dependencies
        System.out.println("Starting to measure time from now on...");
        compssTime = System.nanoTime();
        System.out.println("Creating tasks...");
        for (int d = 1; d <= deepness; ++d) {
            for (int i = 0; i < numTasks; ++i) {
                if (inout)
                    Cbm2Impl.runTaskInOut(taskSleepTime, dummyObjects[i]);
                else {
                    dummyObjects[i] = Cbm2Impl.runTaskIn(taskSleepTime, dummyObjects[i]);
                }
            }
        }
        System.out.println("Finished creating tasks...");

        // Final sync point
        boolean startedGettingObjects = false;
        for (int i = 0; i < numTasks; ++i) {
            dummyObjects[i].foo(); // sync, foo does nothing
            if (!startedGettingObjects) {
                startedGettingObjects = true;
                System.out.println("Started getting objects...");
            }
            System.out.println("Got object " + i);
        }

        compssTime = (System.nanoTime() - compssTime) / 1000000; // Get total time
        System.out.println("Finished cbm2!!!");
        System.out.println(":::::::::::");
        System.out.println("");
        System.out.println(":::::::::::");
        System.out.println("Results:");
        System.out.println("Time: " + compssTime + " ms    ({{" + compssTime + "}})");
        System.out.println(":::::::::::");
    }

}