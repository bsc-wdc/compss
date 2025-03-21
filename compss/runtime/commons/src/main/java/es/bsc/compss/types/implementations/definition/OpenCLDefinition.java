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
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class OpenCLDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 2;
    public static final String SIGNATURE = "opencl.OPENCL";

    private String kernel;
    private String workingDir;


    /**
     * Creates a new OpenCLImplementation for serialization.
     */
    public OpenCLDefinition() {
        // For externalizable
    }

    /**
     * Creates a new OpenCLImplementation instance from the given parameters.
     *
     * @param kernel Path to the OpenCL kernel.
     * @param workingDir Binary working directory.
     */
    public OpenCLDefinition(String kernel, String workingDir) {
        this.kernel = kernel;
        this.workingDir = workingDir;
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public OpenCLDefinition(String[] implTypeArgs, int offset) {
        this.kernel = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        if (this.kernel == null || this.kernel.isEmpty()) {
            throw new IllegalArgumentException("Empty kernel annotation for OpenCL method ");
        }
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(kernel);
        lArgs.add(workingDir);
    }

    /**
     * Returns the path to the OpenCL kernel.
     *
     * @return The path to the OpenCL kernel.
     */
    public String getKernel() {
        return this.kernel;
    }

    /**
     * Returns the binary working directory.
     *
     * @return The binary working directory.
     */
    public String getWorkingDir() {
        return this.workingDir;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.OPENCL;
    }

    @Override
    public String toJSON() {
        StringBuilder sb = new StringBuilder("{\"type\":\"OPENCL\",");
        sb.append("\"kernel\":\"").append(this.kernel).append("\"");
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("OpenCL Implementation \n");
        sb.append("\t Kernel: ").append(kernel).append("\n");
        sb.append("\t Working directory: ").append(workingDir).append("\n");
        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return " OpenCL Method with kernel " + this.kernel;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.kernel = (String) in.readObject();
        this.workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.kernel);
        out.writeObject(this.workingDir);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
