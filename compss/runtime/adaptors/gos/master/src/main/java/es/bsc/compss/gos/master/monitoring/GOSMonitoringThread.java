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
package es.bsc.compss.gos.master.monitoring;

import java.util.concurrent.TimeUnit;


public class GOSMonitoringThread extends Thread {

    private final GOSMonitoring gosMonitoring;

    public boolean running = true;

    private final int sleepTime;


    public GOSMonitoringThread(GOSMonitoring gosMonitoring, int constantSleepTime) {
        this.gosMonitoring = gosMonitoring;
        this.sleepTime = constantSleepTime;
    }

    @Override
    public void run() {
        Thread.currentThread().setName("Monitoring Thread");
        while (running) {
            try {
                TimeUnit.MILLISECONDS.sleep(sleepTime);
            } catch (InterruptedException e) {
                if (gosMonitoring.shutdown) {
                    return;
                }
            }
            running = gosMonitoring.monitoringJobsAndTransfers();
        }
        if (gosMonitoring != null) {
            gosMonitoring.dormant();
        }

    }

}
