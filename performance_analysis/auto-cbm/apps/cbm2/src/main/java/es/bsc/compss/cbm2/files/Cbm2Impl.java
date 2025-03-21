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
package es.bsc.compss.cbm2.files;

import java.lang.management.ManagementFactory;
import java.util.Random;


public class Cbm2Impl {

    /**
     * Dummy task with inout.
     * 
     * @param sleepTime Sleep time.
     * @param dummyFilePath Dummy file path.
     */
    public static void runTaskInOut(int sleepTime, String dummyFilePath) {
        computeSleep(sleepTime);
    }

    /**
     * Dummy task with in.
     * 
     * @param sleepTime Sleep task.
     * @param dummyFilePath Dummy file path.
     * @param dummyFilePathOut Dummy file out path.
     */
    public static void runTaskIn(int sleepTime, String dummyFilePath, String dummyFilePathOut) {
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
