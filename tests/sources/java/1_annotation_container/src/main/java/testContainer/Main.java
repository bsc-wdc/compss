/*
 *  Copyright 2002-2015 Barcelona Supercomputing Center (www.bsc.es)
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
 */
package testContainer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import binary.BINARY;
import container.CONTAINER;


public class Main {

    private static final String FILE_IN = "in";
    private static final String FILE_OUT = "out";


    private static void usage() {
        System.out.println("- Usage: increment.Increment <numIterations> <counter>");
    }

    public static void main(String[] args) throws Exception {
        // Check and get parameters
        if (args.length != 2) {
            usage();
            throw new Exception("[ERROR] Incorrect number of parameters");
        }
        final int N = Integer.parseInt(args[0]);
        final int counter = Integer.parseInt(args[1]);

        // Initialize counter files
        System.out.println("Initial counter values:");
        initializeCounters(counter);

        // Execute increment tasks
        System.out.println("Launch binary and container tasks");
        for (int i = 0; i < N; ++i) {
            BINARY.ls(FILE_OUT);
            CONTAINER.lsContainer(FILE_IN);
        }

        System.out.println("Final counter values:");
        printCounterValues();
    }

    private static void initializeCounters(int counter) throws Exception {
        // FILE IN
        writeFileContent(FILE_IN, counter);

        // FILE OUT
        writeFileContent(FILE_OUT, counter);
    }

    private static void printCounterValues() throws Exception {
        // FILE_OUT
        printFileContent(FILE_OUT);

        // FILE IN
        printFileContent(FILE_IN);
    }

    private static void writeFileContent(String filename, int value) throws Exception {
        try (FileOutputStream fos = new FileOutputStream(filename)) {
            fos.write(value);
        } catch (IOException ioe) {
            throw new Exception("[ERROR] Error initializing file: " + filename, ioe);
        }
    }

    private static void printFileContent(String filename) throws Exception {
        // FILE_OUT
        System.out.println("FILE: " + filename);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ioe) {
            throw new Exception("[ERROR] Error reading file " + filename, ioe);
        }
    }

}
