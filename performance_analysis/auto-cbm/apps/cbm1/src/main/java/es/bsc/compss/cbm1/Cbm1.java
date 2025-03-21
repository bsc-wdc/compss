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
package es.bsc.compss.cbm1;

public class Cbm1 {

    private static void usage() {
        System.out.println("Usage: runcompss cbm1.Cbm1 num_Tasks");
        System.out.println("Exiting cbm1...!");
    }

    /**
     * Application's main.
     * 
     * @param args Command line args (check usage).
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            usage();
            return;
        }

        int numTasks = Integer.parseInt(args[0]);

        System.out.println(":::::::::::");
        System.out.println("Number of tasks: {{" + numTasks + "}}");
        System.out.println(":::::::::::");

        System.out.println("");
        System.out.println(":::::::::::");
        System.out.println("Starting cbm1...");

        String a = "";
        for (int i = 0; i < numTasks; ++i) {
            System.out.println("Iteration: " + i);
            a = Cbm1Impl.runTaskI(i);
        }

        // if(a.contains("a")) System.out.println("hajsddjashsdj");
        System.out.println(a);

        System.out.println("Finished cbm1!!!");
        System.out.println(":::::::::::");
        System.out.println("");
    }

}
