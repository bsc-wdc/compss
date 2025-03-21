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

import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.implementations.definition.ContainerDescription.ContainerEngine;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class ContainerDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 9;
    public static final String SIGNATURE = "container.CONTAINER";


    public static enum ContainerExecutionType {
        CET_PYTHON, // For Python CET executions
        CET_BINARY; // For Binary CET executions
    }


    private ContainerDescription container;
    private ContainerExecutionType internalExecutionType;
    private String internalBinary;
    private String internalParams;
    private String internalFunc;

    private String workingDir;
    private boolean failByEV;


    /**
     * Creates a new ContainerImplementation instance for serialization.
     */
    public ContainerDefinition() {
        // For externalizable
    }

    /**
     * Creates a new ContainerImplementation from the given parameters.
     * 
     * @param internalExecutionType ContainerExecutionType "PYTHON"/"BINARY"
     * @param internalFunc Python function path.
     * @param internalBinary Binary path.
     * @param workingDir Working directory.
     * @param failByEV Flag to enable failure with EV.
     * @param container Container Description.
     */
    public ContainerDefinition(ContainerExecutionType internalExecutionType, String internalFunc, String internalBinary,
        String internalParams, String workingDir, boolean failByEV, ContainerDescription container) {

        this.internalExecutionType = internalExecutionType;
        this.internalBinary = internalBinary;
        this.internalParams = internalParams;
        this.internalFunc = internalFunc;

        this.workingDir = workingDir;
        this.failByEV = failByEV;

        this.container = container;
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public ContainerDefinition(String[] implTypeArgs, int offset) {
        String engineStr = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        engineStr = engineStr.toUpperCase();
        ContainerEngine engine = ContainerEngine.valueOf(engineStr);
        String image = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        String options = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.container = new ContainerDescription(engine, image, options);

        String internalTypeContainerStr = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 3]);
        internalTypeContainerStr = internalTypeContainerStr.toUpperCase();
        // String to ENUM can throw IllegalArgumentException
        this.internalExecutionType = ContainerExecutionType.valueOf(internalTypeContainerStr);
        this.internalBinary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 4]);
        this.internalParams = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 5]);
        this.internalFunc = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 6]);

        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 7]);
        this.failByEV = Boolean.parseBoolean(implTypeArgs[offset + 8]);

        // Check empty arguments
        switch (this.internalExecutionType) {
            case CET_BINARY:
                if (internalBinary == null || internalBinary.isEmpty() || internalBinary.equals("[unassigned]")) {
                    throw new IllegalArgumentException("Empty binary annotation for CONTAINER method");
                }
                break;
            case CET_PYTHON:
                if (internalFunc == null || internalFunc.isEmpty() || internalFunc.equals("[unassigned]")) {
                    throw new IllegalArgumentException("Empty python function annotation for CONTAINER method");
                }
                break;
        }
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(this.container.getEngine().toString());
        lArgs.add(this.container.getImage());
        lArgs.add(this.container.getOptions());
        lArgs.add(this.internalExecutionType.toString());
        lArgs.add(this.internalBinary);
        lArgs.add(this.internalParams);
        lArgs.add(this.internalFunc);
        lArgs.add(this.workingDir);
        lArgs.add(Boolean.toString(this.failByEV));
    }

    /**
     * Returns the internal type.
     * 
     * @return The internal type.
     */
    public ContainerExecutionType getInternalExecutionType() {
        return this.internalExecutionType;
    }

    /**
     * Returns the binary path.
     * 
     * @return The binary path.
     */
    public String getInternalBinary() {
        return this.internalBinary;
    }

    /**
     * Returns the binary params.
     * 
     * @return The binary params.
     */
    public String getInternalParams() {
        return this.internalParams;
    }

    /**
     * Returns the Python function path.
     * 
     * @return The Python function path.
     */
    public String getInternalFunction() {
        return this.internalFunc;
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
        return this.failByEV;
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
    public MethodType getMethodType() {
        return MethodType.CONTAINER;
    }

    @Override
    public String toJSON() {
        StringBuilder sb = new StringBuilder("{\"type\":\"CONTAINER\",");
        sb.append("\"container\":").append(this.container == null ? null : this.container.toJSON());
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return "ContainerImplementation [container=" + this.container + ", internalExecutionType="
            + this.internalExecutionType + ", binary=" + this.internalBinary + ", pyFunc=" + this.internalFunc + "]";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

        this.internalExecutionType = (ContainerExecutionType) in.readObject();
        this.internalFunc = (String) in.readObject();
        this.internalBinary = (String) in.readObject();
        this.internalParams = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.failByEV = in.readBoolean();

        this.container = (ContainerDescription) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

        out.writeObject(this.internalExecutionType);
        out.writeObject(this.internalFunc);
        out.writeObject(this.internalBinary);
        out.writeObject(this.internalParams);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.failByEV);

        out.writeObject(this.container);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Container Implementation \n");
        ;
        sb.append("\t Container: ").append(this.container).append("\n");
        sb.append("\t InternalExecutionType: ").append(this.internalExecutionType).append("\n");
        sb.append("\t InternalBinary: ").append(this.internalBinary).append("\n");
        sb.append("\t InternalParams: ").append(this.internalParams).append("\n");
        sb.append("\t InternalFunction: ").append(this.internalFunc).append("\n");
        return sb.toString();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
