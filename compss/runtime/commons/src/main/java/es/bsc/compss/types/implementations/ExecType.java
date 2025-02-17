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
package es.bsc.compss.types.implementations;

import es.bsc.compss.types.annotations.Constants;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Prolog or Epilog executables.
 */
public class ExecType implements Externalizable {

    public static int ARRAY_LENGTH = 3;

    private String binary;
    private String params;
    private boolean failByExitValue;


    /**
     * Closes any stream parameter of the task.
     *
     * @param binary executable binary
     * @param params binary arguments
     * @param failByExitValue exit value indicates whether the execution fails or not
     */
    public ExecType(String binary, String params, boolean failByExitValue) {
        this.binary = binary;
        this.params = params;
        this.failByExitValue = failByExitValue;
    }

    public ExecType() {
    }

    public String getBinary() {
        return binary;
    }

    public void setBinary(String binary) {
        this.binary = binary;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public boolean isFailByExitValue() {
        return failByExitValue;
    }

    public boolean isAssigned() {
        return this.binary != null && !this.binary.isEmpty() && !this.binary.equals(Constants.UNASSIGNED);
    }

    public void setFailByExitValue(boolean failByExitValue) {
        this.failByExitValue = failByExitValue;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.binary);
        out.writeObject(this.params);
        out.writeBoolean(this.failByExitValue);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.binary = (String) in.readObject();
        this.params = (String) in.readObject();
        this.failByExitValue = in.readBoolean();
    }

    @Override
    public String toString() {
        return "{\"binary\":\"" + this.binary + "\",\"params\":\"" + this.params + "\",\"fail_by_exit\":"
            + this.failByExitValue + "}";
    }
}
