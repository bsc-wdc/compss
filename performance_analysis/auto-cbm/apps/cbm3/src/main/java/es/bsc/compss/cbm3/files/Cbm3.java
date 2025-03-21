/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.cbm3.files;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;


public class Cbm3 {

    // Compss BenchMark to test mergesort-like apps performance.
    private static void usage() {
        System.out.println(
            ":::: Usage: runcompss cbm3.files.Cbm3 (deepness) (task_Sleep_Time) (txSizeInBytes) (INOUT | IN) ::::");
        System.out.println("Exiting cbm3...!");
    }

    /**
     * Application's main.
     * 
     * @param args Command line args (check usage).
     */
    public static void main(String[] args) {
        // Get args
        if (args.length < 4 || (!args[3].equals("INOUT") && !args[3].equals("IN"))) {
            usage();
            return;
        }

        int deepness = Integer.parseInt(args[0]);
        int taskSleepTime = Integer.parseInt(args[1]);
        int txSizeInBytes = Integer.parseInt(args[2]); // Size of the transference
        boolean inout = args[3].equals("INOUT"); // INOUT or IN ???

        System.out.println(":::::::::::");
        System.out.println("Dependency graph deepness: {{" + deepness + "}}");
        System.out.println("Tasks sleep time: {{" + taskSleepTime + "}}");
        System.out.println("Transference size in bytes: {{" + txSizeInBytes + "}}");
        System.out.println("Execution type (INOUT || IN): {{" + (inout ? "INOUT" : "IN") + "}}");
        System.out.println("Execution type (FILES || OBJECTS): {{FILES}}");
        System.out.println(":::::::::::");
        System.out.println("");
        System.out.println(":::::::::::");
        System.out.println("Starting cbm3 with files...");
        /////////////////////////////////////////////////////

        deepness += 1;

        // Create all the objects.
        System.out.println("Creating pool of files...");
        // Create a file for each task
        int numTasks = (int) Math.pow(2, deepness - 1);
        String[] dummyFilePaths = new String[numTasks];
        for (int i = 0; i < numTasks; ++i) {
            try {
                String filePath = "dummyFile_Task" + String.valueOf(i);
                dummyFilePaths[i] = filePath;
                FileOutputStream fos = new FileOutputStream(filePath);
                byte[] dummyLoad = new byte[txSizeInBytes];
                new Random().nextBytes(dummyLoad);
                fos.write(dummyLoad);
                fos.close();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        System.out.println("Pool of files created (" + dummyFilePaths.length + " created)");
        //////////////////////////////////////////////////////////

        System.out.println("From now on we start to measure time...");
        System.out.println("Creating tasks...");
        double compssTime = System.nanoTime(); // Start measuring time
        // TASKS CREATION //////////////////////////////////////////////
        int step = 1;
        for (int d = 0; d <= deepness; ++d) {
            for (int i = 0; i + step < numTasks; i += step * 2) {
                if (inout) {
                    Cbm3Impl.runTaskInOut(taskSleepTime, dummyFilePaths[i], dummyFilePaths[i + step]);
                } else {
                    Cbm3Impl.runTaskIn(taskSleepTime, dummyFilePaths[i], dummyFilePaths[i + step], dummyFilePaths[i]);
                }
            }
            step *= 2;
        }
        /////////////////////////////////////////////////////
        System.out.println("Tasks created in " + ((System.nanoTime() - compssTime) / 1000000) + " ms");

        System.out.println("Waiting to sync...");

        FileInputStream fis = null;
        try {
            fis = new FileInputStream(new File(dummyFilePaths[0]));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        System.out.println("Got file \"" + dummyFilePaths[0] + "\"");
        if (fis != null) {
            try {
                fis.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        System.out.println(":::::::::::");
        System.out.println("cbm3 Finished!");
        System.out.println("Time: {{" + ((System.nanoTime() - compssTime) / 1000000) + "}}");
        System.out.println(":::::::::::");
    }

}
