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
package es.bsc.compss.types;

import es.bsc.compss.types.annotations.Constants;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class MPIProgram implements Externalizable {

    public static final int NUM_OF_PARAMS = 3;
    private String binary;
    private String params;
    private int processes;

    // when executed from cmd, spaces in the original params string should be kept
    // and distinguished from the spaces inside parameter strings
    private String[] paramsArray;


    /**
     * Default Constructor.
     */
    public MPIProgram() {
        this.binary = "";
        this.params = "";
        this.processes = -1;
    }

    /**
     * MPI Program constructor. It represents a single program from MPMD MPI programs.
     *
     * @param binary program's binary.
     * @param params program params to be added after the binary.
     * @param processes number of processes.
     */
    public MPIProgram(String binary, String params, int processes) {
        this.binary = binary;
        this.params = params;
        this.processes = processes;
    }

    public String getBinary() {
        return binary;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public int getProcesses() {
        return processes;
    }

    public String[] getParamsArray() {
        return paramsArray;
    }

    public void setParamsArray(String[] paramsArray) {
        this.paramsArray = paramsArray;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.binary = (String) in.readObject();
        this.params = (String) in.readObject();
        this.processes = in.readInt();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.binary);
        out.writeObject(this.params);
        out.writeInt(this.processes);
    }

    /**
     * Check if it is an empty MPI program definition.
     * 
     * @return True only if binary is not provided
     */
    public boolean isEmpty() {
        return this.binary == null || this.binary.isEmpty();
    }

    @Override
    public String toString() {
        return "MPIProgram{" + "binary='" + binary + '\'' + ", params='" + params + '\'' + ", processes=" + processes
            + '}';
    }

    public boolean hasParamsString() {
        return this.params != null && !this.params.isEmpty() && !this.params.equals(Constants.UNASSIGNED);
    }
}
