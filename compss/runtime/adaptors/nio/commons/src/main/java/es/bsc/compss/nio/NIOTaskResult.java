/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NIOTaskResult implements Externalizable {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);

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
    public NIOTaskResult(int jobId, Object[][] params) {
        this.jobId = jobId;
        for (int i = 0; i < params.length; i++) {
            if (params[i] == null) {
                this.results.add(new NIOResult(null, null));
            } else if (params[i][TaskMonitor.TYPE_POS] == DataType.COLLECTION_T) {
                this.results.add(new NIOResultCollection(params[i]));
            } else {
                this.results.add(new NIOResult((DataType) params[i][TaskMonitor.TYPE_POS],
                    params[i][TaskMonitor.LOCATION_POS].toString()));
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
