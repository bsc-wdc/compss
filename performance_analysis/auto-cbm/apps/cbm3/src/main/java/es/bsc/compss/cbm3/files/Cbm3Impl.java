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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Random;


public class Cbm3Impl {

    /**
     * Dummy In task.
     * 
     * @param sleepTime Sleep time.
     * @param fileinLeft File in left.
     * @param fileinRight File in right.
     * @param fileout File out.
     */
    public static void runTaskIn(int sleepTime, String fileinLeft, String fileinRight, String fileout) {
        // Create file out.
        try {
            Files.copy(Paths.get(fileinLeft), Paths.get(fileout), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        computeSleep(sleepTime);
    }

    /**
     * Dummy inout task.
     * 
     * @param sleepTime Sleep time.
     * @param fileinoutLeft File inout left.
     * @param fileinRight File in right.
     */
    public static void runTaskInOut(int sleepTime, String fileinoutLeft, String fileinRight) {
        // The copy is only to compare against runTaskIn
        try {
            Files.copy(Paths.get(fileinRight), Paths.get(fileinoutLeft), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
        computeSleep(sleepTime);
    }

    private static void computeSleep(int time) {
        long t = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId());
        while ((ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId()) - t)
            / 1000000 < time) {
            double x = new Random().nextDouble();
            for (int i = 0; i < 1000; ++i) {
                x = Math.atan(Math.sqrt(Math.pow(x, 10)));
            }
        }
    }

}
