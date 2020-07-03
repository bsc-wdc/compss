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
package es.bsc.compss.nio;

import es.bsc.compss.types.annotations.parameter.DataType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;


public class NIOTaskResult implements Externalizable {

    private int jobId;

    private List<DataType> paramTypes = new LinkedList<>();
    private List<String> paramLocations = new LinkedList<>();


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
     * @param paramTypes Job parameters' final types
     * @param paramLocations Job parameters' values locations
     */
    public NIOTaskResult(int jobId, DataType[] paramTypes, String[] paramLocations) {
        this.jobId = jobId;
        for (int i = 0; i < paramTypes.length; i++) {
            this.paramTypes.add(paramTypes[i]);
            this.paramLocations.add(paramLocations[i]);
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
            this.paramTypes.add(np.getType());

            if (np.isWriteFinalValue()) {
                // Object has direction INOUT or OUT
                this.paramLocations.add(np.getTargetPath());
            } else {
                // Object has direction IN
                this.paramLocations.add(null);
            }
        }
        if (targetParam != null) {
            this.paramTypes.add(targetParam.getType());

            if (targetParam.isWriteFinalValue()) {
                // Target is marked with isModifier = true
                this.paramLocations.add(targetParam.getTargetPath());
            } else {
                // Target is marked with isModifier = false
                this.paramLocations.add(null);
            }
        }

        for (NIOParam np : results) {
            this.paramTypes.add(np.getType());
            this.paramLocations.add(np.getTargetPath());
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
     * Returns the parameter types.
     *
     * @return The parameter types
     */
    public List<DataType> getParamTypes() {
        return this.paramTypes;
    }

    /**
     * Returns the value of the parameter {@code i}.
     *
     * @param i Parameter index.
     * @return The value of the parameter {@code i}.
     */
    public String getParamValue(int i) {
        return this.paramLocations.get(i);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.jobId = in.readInt();
        this.paramTypes = (LinkedList<DataType>) in.readObject();
        this.paramLocations = (LinkedList<String>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.jobId);
        out.writeObject(this.paramTypes);
        out.writeObject(this.paramLocations);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[JOB_RESULT ");
        sb.append("[JOB ID= ").append(this.jobId).append("]");
        sb.append("[PARAM_TYPES");
        for (DataType param : this.paramTypes) {
            sb.append(" ").append(param);
        }
        sb.append("]");
        sb.append("[PARAM_VALUES");
        for (Object param : this.paramLocations) {
            sb.append(" ").append(param);
        }
        sb.append("]");
        sb.append("]");
        return sb.toString();
    }

}
