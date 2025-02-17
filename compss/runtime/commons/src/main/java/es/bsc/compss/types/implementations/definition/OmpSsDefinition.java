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
package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.MPIProgram;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class OmpSsDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 3;
    public static final String SIGNATURE = "ompss.OMPSS";

    private String binary;
    private String workingDir;
    private boolean failByEV;


    /**
     * Creates a new OmpSsImplementation for serialization.
     */
    public OmpSsDefinition() {
        // For externalizable
    }

    /**
     * Creates a new OmpSsImplementation instance from the given parameters.
     * 
     * @param binary Path to the OmpSs binary.
     * @param workingDir Binary working directory.
     * @param failByEV Flag to enable failure with EV.
     */
    public OmpSsDefinition(String binary, String workingDir, boolean failByEV) {
        this.binary = binary;
        this.workingDir = workingDir;
        this.failByEV = failByEV;
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public OmpSsDefinition(String[] implTypeArgs, int offset) {
        this.binary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.failByEV = Boolean.parseBoolean(implTypeArgs[offset + 2]);
        if (binary == null || binary.isEmpty()) {
            throw new IllegalArgumentException("Empty binary annotation for OmpSs method");
        }
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(binary);
        lArgs.add(workingDir);
        lArgs.add(Boolean.toString(failByEV));
    }

    /**
     * Returns the path to the OmpSs binary.
     * 
     * @return The path to the OmpSs binary.
     */
    public String getBinary() {
        return this.binary;
    }

    /**
     * Returns the binary working directory.
     * 
     * @return The binary working directory.
     */
    public String getWorkingDir() {
        return this.workingDir;
    }

    /**
     * Check if fail by exit value is enabled.
     * 
     * @return True is fail by exit value is enabled.
     */
    public boolean isFailByEV() {
        return failByEV;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.OMPSS;
    }

    @Override
    public String toJSON() {
        StringBuilder sb = new StringBuilder("{\"type\":\"OMPSs\",");
        sb.append("\"binary\":\"").append(this.binary).append("\",");
        sb.append("\"fail_by_ev\":").append(this.failByEV);
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("OmpSs Implementation \n");
        sb.append("\t Binary: ").append(binary).append("\n");
        sb.append("\t Working directory: ").append(workingDir).append("\n");
        sb.append("\t Fail by EV: ").append(this.failByEV).append("\n");
        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return "OmpSs Method with binary " + this.binary;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.binary = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.failByEV = in.readBoolean();

    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.binary);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.failByEV);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
