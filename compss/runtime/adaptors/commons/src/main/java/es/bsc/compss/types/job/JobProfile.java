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
package es.bsc.compss.types.job;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Class containing all the information related to a job execution in a worker.
 */
public class JobProfile implements Externalizable {

    private long submissionTS;
    private long arrivalTS;
    private long fetchedDataTS;
    private long executionStartTS;
    private long executionEndTS;
    private long endNotificationTS;
    private long endTS;


    /**
     * Constructs a new empty profile of a job.
     */
    public JobProfile() {
        submissionTS = 0;
        fetchedDataTS = 0;
        executionStartTS = 0;
        executionEndTS = 0;
        endNotificationTS = 0;
    }

    public void submitted() {
        this.submissionTS = System.currentTimeMillis();
    }

    public void arrived() {
        this.arrivalTS = System.currentTimeMillis();
    }

    public void fetchedAllInputData() {
        this.fetchedDataTS = System.currentTimeMillis();
    }

    public void startingExecution() {
        this.executionStartTS = System.currentTimeMillis();
    }

    public void endedExecution() {
        this.executionEndTS = System.currentTimeMillis();
    }

    public void endNotified() {
        this.endNotificationTS = System.currentTimeMillis();
    }

    public void completed() {
        this.endTS = System.currentTimeMillis();
    }

    public long getExecutionLength() {
        return this.executionEndTS - this.executionStartTS;
    }

    public long getTotalTime() {
        return this.endTS - this.submissionTS;
    }

    /**
     * Overrides the JobProfile with the execution-related content of another job profile.
     * 
     * @param jp job profile with the execution related information.
     */
    public void mergeRemoteData(JobProfile jp) {
        this.fetchedDataTS = jp.fetchedDataTS;
        this.executionStartTS = jp.executionStartTS;
        this.executionEndTS = jp.executionEndTS;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(this.fetchedDataTS);
        out.writeLong(this.executionStartTS);
        out.writeLong(this.executionEndTS);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.fetchedDataTS = in.readLong();
        this.executionStartTS = in.readLong();
        this.executionEndTS = in.readLong();
    }

}
