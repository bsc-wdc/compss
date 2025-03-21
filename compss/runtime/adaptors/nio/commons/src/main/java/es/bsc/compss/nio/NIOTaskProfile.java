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
package es.bsc.compss.nio;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class NIOTaskProfile implements Externalizable {

    private long arrivalTS;
    private long fetchedDataTS;
    private long executionStartTS;
    private long executionEndTS;
    private long resultNotificationTS;


    public NIOTaskProfile() {
    }

    public void arrived() {
        arrivalTS = System.currentTimeMillis();
    }

    public long getArrivalTime() {
        return arrivalTS;
    }

    public void dataFetched() {
        this.fetchedDataTS = System.currentTimeMillis();
    }

    public long getFetchedDataTime() {
        return fetchedDataTS;
    }

    public void executionStarts() {
        this.executionStartTS = System.currentTimeMillis();
    }

    public void executionStartsAt(long ts) {
        this.executionStartTS = ts;
    }

    public long getExecutionStartTime() {
        return executionStartTS;
    }

    public void executionEnds() {
        this.executionEndTS = System.currentTimeMillis();
    }

    public void executionEndsAt(long ts) {
        this.executionEndTS = ts;
    }

    public long getExecutionEndTime() {
        return executionEndTS;
    }

    public void end() {
        this.resultNotificationTS = System.currentTimeMillis();
    }

    public long getEndTime() {
        return resultNotificationTS;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(arrivalTS);
        out.writeLong(fetchedDataTS);
        out.writeLong(executionStartTS);
        out.writeLong(executionEndTS);
        out.writeLong(resultNotificationTS);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        arrivalTS = in.readLong();
        fetchedDataTS = in.readLong();
        executionStartTS = in.readLong();
        executionEndTS = in.readLong();
        resultNotificationTS = in.readLong();
    }

}
