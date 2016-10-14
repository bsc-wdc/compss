package cbm2.files;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;


public class Cbm2 {

    private static void usage() {
        System.out.println(
                ":::: Usage: runcompss cbm2.files.Cbm2 (num_Tasks) (deepness) (task_Sleep_Time) (txSizeInBytes) (INOUT | IN):::: ");
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
        System.out.println("Execution type (FILES || OBJECTS): {{FILES}}");
        System.out.println(":::::::::::");
        System.out.println("");
        System.out.println(":::::::::::");
        System.out.println("Starting cbm2 with files...");

        double compssTime = System.nanoTime();

        // CREATE A FILE FOR EACH TASK
        System.out.println("Creating pool of files...");
        String[] filePaths = new String[numTasks];
        for (int i = 0; i < numTasks; ++i) {
            try {
                String filePath = "dummyFile_Task" + String.valueOf(i);
                filePaths[i] = filePath;
                FileOutputStream fos = new FileOutputStream(filePath);
                byte dummyLoad[] = new byte[txSizeInBytes];
                new Random().nextBytes(dummyLoad);
                fos.write(dummyLoad);
                fos.close();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        //

        // Create tasks
        System.out.println("Pool of files created.");
        System.out.println("Time for files to be created: " + ((System.nanoTime() - compssTime) / 1000000) + " ms");
        System.out.println("Starting to measure time from now on...");
        compssTime = System.nanoTime();
        for (int d = 1; d <= deepness; ++d) {
            for (int i = 0; i < numTasks; ++i) {
                if (inout)
                    Cbm2Impl.runTaskInOut(taskSleepTime, filePaths[i]);
                else
                    Cbm2Impl.runTaskIn(taskSleepTime, filePaths[i], filePaths[i]);
            }
        }
        //

        // Final sync point
        boolean startedGettingObjects = false;
        for (int i = 0; i < numTasks; ++i) {
            if (!startedGettingObjects) {
                startedGettingObjects = true;
                System.out.println("Started getting files...");
            }

            FileInputStream fis = null;
            try {
                fis = new FileInputStream(new File(filePaths[i]));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            System.out.println("Got file \"" + filePaths[i] + "\"");
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //

        // Final output
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