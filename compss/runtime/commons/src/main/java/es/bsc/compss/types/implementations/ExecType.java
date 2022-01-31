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
package es.bsc.compss.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Prolog or Epilog executables.
 */
public class ExecType implements Externalizable {

    public static int ARRAY_LENGTH = 2;

    private String binary;
    private String params;
    private ExecutionOrder order;


    /**
     * Closes any stream parameter of the task.
     *
     * @param order to indicate if it is a prolog or an epilog.
     * @param binary executable binary
     * @param params binary arguments
     */
    public ExecType(ExecutionOrder order, String binary, String params) {
        this.binary = binary;
        this.params = params;
        this.order = order;
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

    public ExecutionOrder getOrder() {
        return order;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.order);
        out.writeObject(this.binary);
        out.writeObject(this.params);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.order = (ExecutionOrder) in.readObject();
        this.binary = (String) in.readObject();
        this.params = (String) in.readObject();
    }
}
