/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.api.TaskMonitor.CollectionTaskResult;
import es.bsc.compss.api.TaskMonitor.TaskResult;
import es.bsc.compss.types.annotations.parameter.DataType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
     * @param params Job parameters' final types
     */
    public NIOTaskResult(int jobId, TaskResult[] params) {
        this.jobId = jobId;
        for (TaskResult param : params) {
            if (param == null) {
                this.results.add(new NIOResult(null, null));
            } else {
                if (param.getType() == DataType.COLLECTION_T) {
                    this.results.add(new NIOResultCollection((CollectionTaskResult) param));
                } else {
                    this.results.add(new NIOResult(param.getType(), param.getDataLocation()));
                }
            }
        }
    }

    /**
     * New task result with the given information.
     *
     * @param jobId Job Id.
     * @param arguments Job arguments.
     * @param targetParam Job target.
     * @param results Job results.
     */
    public NIOTaskResult(int jobId, List<NIOParam> arguments, NIOParam targetParam, List<NIOParam> results) {
        this.jobId = jobId;

        for (NIOParam np : arguments) {
            this.results.add(np.getResult());
        }

        if (targetParam != null) {
            this.results.add(targetParam.getResult());
        }

        for (NIOParam np : results) {
            this.results.add(np.getResult());
        }
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
        StringBuilder sb = new StringBuilder("[JOB_RESULT ");
        sb.append("[JOB ID= ").append(this.jobId).append("]");
        sb.append("[PARAM_RESULTS");
        for (NIOResult param : this.results) {
            sb.append(" ").append(param);
        }
        sb.append("]");
        sb.append("]");
        return sb.toString();
    }

}
