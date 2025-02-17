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
package es.bsc.compss.cbm3.objects;

import java.lang.management.ManagementFactory;
import java.util.Random;


public class Cbm3Impl {

    /**
     * Dummy in task.
     * 
     * @param sleepTime Sleep time.
     * @param objinLeft Object in left.
     * @param objinRight Object in right.
     * @return Resulting object.
     */
    public static DummyPayload runTaskIn(int sleepTime, DummyPayload objinLeft, DummyPayload objinRight) {
        computeSleep(sleepTime);
        objinRight.regen(objinRight.getSize());
        return objinLeft;
    }

    /**
     * Dummy inout task.
     * 
     * @param sleepTime Sleep time.
     * @param objinoutLeft Object inout left.
     * @param objinRight Object in right.
     */
    public static void runTaskInOut(int sleepTime, DummyPayload objinoutLeft, DummyPayload objinRight) {
        computeSleep(sleepTime);
        objinoutLeft.regen(objinoutLeft.getSize());
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
