/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.util.CoreManager;


public class WorkloadState {

    // Core Information
    private int coreCount;
    private long[] coreMinTime;
    private long[] coreMeanTime;
    private long[] coreMaxTime;

    // Action counters
    private int noResourceCount;
    private int[] noResourceCounts;
    private int readyCount;
    private int[] readyCounts;

    // Running Tasks
    private int[] runningCounts;
    private int runningCount;
    private long[] runningCoreMeanTime;


    public WorkloadState() {
        coreCount = CoreManager.getCoreCount();
        coreMinTime = new long[coreCount];
        coreMeanTime = new long[coreCount];
        coreMaxTime = new long[coreCount];

        noResourceCounts = new int[coreCount];
        readyCounts = new int[coreCount];

        runningCounts = new int[coreCount];
        runningCoreMeanTime = new long[coreCount];
    }

    /*--------------------------------------------
     ------------- CORE INFORMATION --------------
     ---------------------------------------------*/
    public int getCoreCount() {
        return coreCount;
    }

    public void registerTimes(int coreId, long minTime, long avgTime, long maxTime) {
        if (coreId < coreCount) {
            coreMinTime[coreId] = minTime;
            coreMeanTime[coreId] = avgTime;
            coreMaxTime[coreId] = maxTime;
        }
    }

    public long getCoreMeanTime(int coreId) {
        return coreMeanTime[coreId];
    }

    public long getCoreMaxTime(int coreId) {
        return coreMaxTime[coreId];
    }

    public long getCoreMinTime(int coreId) {
        return coreMinTime[coreId];
    }

    /*--------------------------------------------
     -------------- ACTION INFORMATION -----------
     ---------------------------------------------*/
    public int getNoResourceCount() {
        return noResourceCount;
    }

    public int[] getNoResourceCounts() {
        return noResourceCounts;
    }

    public int getReadyCount() {
        return readyCount;
    }

    public int[] getReadyCounts() {
        return readyCounts;
    }

    public void registerNoResources(int coreId, int count) {
        this.noResourceCount -= this.noResourceCounts[coreId];
        this.noResourceCounts[coreId] = count;
        this.noResourceCount += this.noResourceCounts[coreId];
    }

    public void registerReady(int coreId, int count) {
        this.readyCount -= this.readyCounts[coreId];
        this.readyCounts[coreId] = count;
        this.readyCount += this.readyCounts[coreId];
    }

    /*--------------------------------------------
     --------- RUNNING ACTION INFORMATION --------
     ---------------------------------------------*/
    public long getRunningCoreMeanTime(int coreId) {
        return runningCoreMeanTime[coreId];
    }

    public int[] getRunningTaskCounts() {
        return this.runningCounts;
    }

    public int getRunningTaskCount() {
        return this.runningCount;
    }

    public void registerRunning(int coreId, long executedTime) {
        runningCoreMeanTime[coreId] = (runningCoreMeanTime[coreId] + executedTime) / (runningCounts[coreId] + 1);
        runningCounts[coreId]++;
        runningCount++;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        // Time Stamp
        sb.append("TIMESTAMP = ").append(String.valueOf(System.currentTimeMillis())).append("\n");

        // Load Information
        sb.append("LOAD_INFO = [").append("\n");
        for (int coreId = 0; coreId < coreCount; coreId++) {
            sb.append("\t").append("CORE_INFO = [").append("\n");
            sb.append("\t").append("\t").append("COREID = ").append(coreId).append("\n");
            sb.append("\t").append("\t").append("NO_RESOURCE = ").append(noResourceCounts[coreId]).append("\n");
            sb.append("\t").append("\t").append("READY = ").append(readyCounts[coreId]).append("\n");
            sb.append("\t").append("\t").append("RUNNING = ").append(runningCounts[coreId]).append("\n");
            sb.append("\t").append("\t").append("MIN = ").append(coreMinTime[coreId]).append("\n");
            sb.append("\t").append("\t").append("MEAN = ").append(coreMeanTime[coreId]).append("\n");
            sb.append("\t").append("\t").append("MAX = ").append(coreMaxTime[coreId]).append("\n");
            sb.append("\t").append("\t").append("RUNNING_MEAN = ").append(runningCoreMeanTime[coreId]).append("\n");
            sb.append("\t").append("]").append("\n");
        }
        sb.append("]").append("\n");

        return sb.toString();
    }

}
