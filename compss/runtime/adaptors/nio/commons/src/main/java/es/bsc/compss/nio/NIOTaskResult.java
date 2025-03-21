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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class NIOTaskResult implements Externalizable {

    private int jobId;

    private List<NIOResult> results = new LinkedList<>();


    /**
     * Only for externalization.
     */
    public NIOTaskResult() {
        // All attributes are initialized statically
    }

    /**
     * New task result with the given information.
     *
     * @param jobId Job Id.
     */
    public NIOTaskResult(int jobId) {
        this.jobId = jobId;
    }

    /**
     * Returns the job id associated to the result.
     *
     * @return The job Id associated to the result.
     */
    public int getJobId() {
        return this.jobId;
    }

    /**
     * Returns the list of res.
     *
     * @return The parameter types
     */
    public List<NIOResult> getParamResults() {
        return this.results;
    }

    /**
     * Adds a new parameter result to the task results.
     *
     * @param pr task result to be added
     */
    public void addParamResult(NIOResult pr) {
        this.results.add(pr);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.jobId = in.readInt();
        this.results = (List<NIOResult>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.jobId);
        out.writeObject(this.results);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        sb.append("\"job_id\":").append(this.jobId).append(",");
        sb.append("\"params\":[");
        Iterator<NIOResult> itr = this.results.iterator();
        if (itr.hasNext()) {
            NIOResult nr = itr.next();
            sb.append(nr);
            while (itr.hasNext()) {
                nr = itr.next();
                sb.append("," + nr);
            }
        }
        sb.append("]");
        sb.append("}");
        return sb.toString();
    }

}
