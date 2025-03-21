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


    /**
     * Creates a new WorkloadState instance.
     */
    public WorkloadState() {
        this.coreCount = CoreManager.getCoreCount();
        this.coreMinTime = new long[this.coreCount];
        this.coreMeanTime = new long[this.coreCount];
        this.coreMaxTime = new long[this.coreCount];

        this.noResourceCounts = new int[this.coreCount];
        this.readyCounts = new int[this.coreCount];

        this.runningCounts = new int[this.coreCount];
        this.runningCoreMeanTime = new long[this.coreCount];
    }

    /*--------------------------------------------
     ------------- CORE INFORMATION --------------
     ---------------------------------------------*/

    /**
     * Returns the number of cores.
     * 
     * @return The number of cores.
     */
    public int getCoreCount() {
        return this.coreCount;
    }

    /**
     * Registers the execution times of a given core Id.
     * 
     * @param coreId Core Id.
     * @param minTime Minimum execution time.
     * @param avgTime Average execution time.
     * @param maxTime Maximum execution time.
     */
    public void registerTimes(int coreId, long minTime, long avgTime, long maxTime) {
        if (coreId < this.coreCount) {
            this.coreMinTime[coreId] = minTime;
            this.coreMeanTime[coreId] = avgTime;
            this.coreMaxTime[coreId] = maxTime;
        }
    }

    /**
     * Returns the minimum execution time of the given core Id.
     * 
     * @param coreId Core Id.
     * @return The minimum execution time
     */
    public long getCoreMinTime(int coreId) {
        return this.coreMinTime[coreId];
    }

    /**
     * Returns the average execution time of the given core Id.
     * 
     * @param coreId Core Id.
     * @return The average execution time
     */
    public long getCoreMeanTime(int coreId) {
        return this.coreMeanTime[coreId];
    }

    /**
     * Returns the maximum execution time of the given core Id.
     * 
     * @param coreId Core Id.
     * @return The maximum execution time
     */
    public long getCoreMaxTime(int coreId) {
        return this.coreMaxTime[coreId];
    }

    /*--------------------------------------------
     -------------- ACTION INFORMATION -----------
     ---------------------------------------------*/
    /**
     * Returns the number of cores without assigned resource.
     * 
     * @return The number of cores without assigned resource.
     */
    public int getNoResourceCount() {
        return this.noResourceCount;
    }

    /**
     * Returns the coreIds without assigned resource.
     * 
     * @return The corIds without assigned resource.
     */
    public int[] getNoResourceCounts() {
        return this.noResourceCounts;
    }

    /**
     * Returns the number of core Ids that are in ready state.
     * 
     * @return The number of core Ids that are in ready state.
     */
    public int getReadyCount() {
        return this.readyCount;
    }

    /**
     * Returns the coreIds that are in ready state.
     * 
     * @return The coreIds that are in ready state.
     */
    public int[] getReadyCounts() {
        return this.readyCounts;
    }

    /**
     * Registers a {@code count} tasks of the given coreId without resources.
     * 
     * @param coreId CoreId
     * @param count Number of tasks of this core.
     */
    public void registerNoResources(int coreId, int count) {
        this.noResourceCount -= this.noResourceCounts[coreId];
        this.noResourceCounts[coreId] = count;
        this.noResourceCount += this.noResourceCounts[coreId];
    }

    /**
     * Registers a {@code count} tasks of the given coreId as ready.
     * 
     * @param coreId CoreId
     * @param count Number of tasks of this core.
     */
    public void registerReady(int coreId, int count) {
        this.readyCount -= this.readyCounts[coreId];
        this.readyCounts[coreId] = count;
        this.readyCount += this.readyCounts[coreId];
    }

    /*--------------------------------------------
     --------- RUNNING ACTION INFORMATION --------
     ---------------------------------------------*/
    /**
     * Returns the running mean time of the given core Id.
     * 
     * @param coreId Core Id.
     * @return The running mean time.
     */
    public long getRunningCoreMeanTime(int coreId) {
        return this.runningCoreMeanTime[coreId];
    }

    /**
     * Returns the number of running tasks of each core.
     * 
     * @return The number of running tasks of each core.
     */
    public int[] getRunningTaskCounts() {
        return this.runningCounts;
    }

    /**
     * Returns the total number of running tasks.
     * 
     * @return The total number of running tasks.
     */
    public int getRunningTaskCount() {
        return this.runningCount;
    }

    /**
     * Registers a running task of the given coreId.
     * 
     * @param coreId Core Id.
     * @param executedTime Execution time.
     */
    public void registerRunning(int coreId, long executedTime) {
        this.runningCoreMeanTime[coreId] =
            (this.runningCoreMeanTime[coreId] + executedTime) / (this.runningCounts[coreId] + 1);
        this.runningCounts[coreId]++;
        this.runningCount++;
    }

    /**
     * Dumps the current workload information into a string.
     * 
     * @return String containing the workload information.
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        // Time Stamp
        sb.append("TIMESTAMP = ").append(String.valueOf(System.currentTimeMillis())).append("\n");

        // Load Information
        sb.append("LOAD_INFO = [").append("\n");
        for (int coreId = 0; coreId < this.coreCount; coreId++) {
            sb.append("\t").append("CORE_INFO = [").append("\n");
            sb.append("\t").append("\t").append("COREID = ").append(coreId).append("\n");
            sb.append("\t").append("\t").append("NO_RESOURCE = ").append(this.noResourceCounts[coreId]).append("\n");
            sb.append("\t").append("\t").append("READY = ").append(this.readyCounts[coreId]).append("\n");
            sb.append("\t").append("\t").append("RUNNING = ").append(this.runningCounts[coreId]).append("\n");
            sb.append("\t").append("\t").append("MIN = ").append(this.coreMinTime[coreId]).append("\n");
            sb.append("\t").append("\t").append("MEAN = ").append(this.coreMeanTime[coreId]).append("\n");
            sb.append("\t").append("\t").append("MAX = ").append(this.coreMaxTime[coreId]).append("\n");
            sb.append("\t").append("\t").append("RUNNING_MEAN = ").append(this.runningCoreMeanTime[coreId])
                .append("\n");
            sb.append("\t").append("]").append("\n");
        }
        sb.append("]").append("\n");

        return sb.toString();
    }

}
