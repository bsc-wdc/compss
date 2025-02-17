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

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


/**
 * Class containing all the necessary information to generate a Binary implementation of a CE.
 */
public class BinaryDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 4;
    public static final String SIGNATURE = "binary.BINARY";

    private String binary;
    private String workingDir;
    private String params;
    private boolean failByEV;
    private ContainerDescription container;


    public BinaryDefinition() {
        // Externalizable
    }

    /**
     * Creates a new ImplementationDefinition to create a binary core element implementation.
     *
     * @param binary Binary path.
     * @param workingDir Working directory.
     * @param failByEV Flag to enable failure with EV.
     */
    public BinaryDefinition(String binary, String workingDir, String params, boolean failByEV) {
        this.binary = binary;
        this.workingDir = workingDir;
        this.params = params;
        this.failByEV = failByEV;
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     * @param container Container description.
     */
    public BinaryDefinition(String[] implTypeArgs, int offset, ContainerDescription container) {
        this.binary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.params = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.failByEV = Boolean.parseBoolean(implTypeArgs[offset + 3]);

        if (binary == null || binary.isEmpty() || binary.equals("[unassigned]")) {
            throw new IllegalArgumentException("Empty binary annotation for BINARY method");
        }
        this.container = container;
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(binary);
        lArgs.add(workingDir);
        lArgs.add(params);
        lArgs.add(Boolean.toString(failByEV));
    }

    /**
     * Returns the binary path.
     * 
     * @return The binary path.
     */
    public String getBinary() {
        return this.binary;
    }

    public String getParams() {
        return params;
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
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.BINARY;
    }

    /**
     * Returns the container.
     *
     * @return The container implementation.
     */
    public ContainerDescription getContainer() {
        return this.container;
    }

    @Override
    public String toJSON() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"type\":\"BINARY\",");
        sb.append("\"binary\":\"").append(this.binary).append("\",");
        sb.append("\"params\":\"").append(this.params).append("\",");
        sb.append("\"container\":").append(this.container == null ? null : this.container.toJSON());
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return "Binary Method with binary " + this.binary;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.binary = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.params = (String) in.readObject();
        this.failByEV = in.readBoolean();
        this.container = (ContainerDescription) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.binary);
        out.writeObject(this.workingDir);
        out.writeObject(this.params);
        out.writeBoolean(this.failByEV);
        out.writeObject(this.container);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Binary Definition \n");
        sb.append("\t Binary: ").append(this.binary).append("\n");
        sb.append("\t Working directory: ").append(this.workingDir).append("\n");
        sb.append("\t Params String: ").append(this.params).append("\n");
        sb.append("\t Fail by EV: ").append(this.failByEV).append("\n");
        sb.append("\t Container: ").append(this.container).append("\n");
        return sb.toString();
    }

    public boolean hasParamsString() {
        return this.params != null && !this.params.equals(Constants.UNASSIGNED);
    }
}
