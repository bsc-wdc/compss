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


public class MPIDefinition extends CommonMPIDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 8;
    public static final String SIGNATURE = "mpi.MPI";

    private static final String ERROR_MPI_BINARY = "ERROR: Empty binary annotation for MPI method";

    private String binary;
    private String params;

    private ContainerDescription container;


    /**
     * Creates a new MPIImplementation for serialization.
     */
    public MPIDefinition() {
        // For externalizable
    }

    /**
     * Creates a new MPIImplementation instance from the given parameters.
     *
     * @param binary MPI binary path.
     * @param workingDir Binary working directory.
     * @param mpiRunner Path to the MPI command.
     * @param ppn Process per node.
     * @param scaleByCU Scale by computing units property.
     * @param params params string to be appended to the end of the command.
     * @param failByEV Flag to enable failure with EV.
     */
    public MPIDefinition(String binary, String workingDir, String mpiRunner, int ppn, String mpiFlags,
        boolean scaleByCU, String params, boolean failByEV) {
        super(workingDir, mpiRunner, ppn, mpiFlags, scaleByCU, failByEV);
        this.binary = binary;
        this.params = params;
    }

    /**
     * Creates a new Definition from string array.
     *
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public MPIDefinition(String[] implTypeArgs, int offset) {
        this(implTypeArgs, offset, null);
    }

    /**
     * Creates a new Definition from string array.
     *
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     * @param container Container description.
     */
    public MPIDefinition(String[] implTypeArgs, int offset, ContainerDescription container) {
        this.binary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.mpiRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.ppn = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 3]));
        this.mpiFlags = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 4]);
        this.scaleByCU = Boolean.parseBoolean(implTypeArgs[offset + 5]);
        this.params = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 6]);
        this.failByEV = Boolean.parseBoolean(implTypeArgs[offset + 7]);
        this.container = container;

        checkArguments();
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(this.binary);
        lArgs.add(this.workingDir);
        lArgs.add(this.mpiRunner);
        lArgs.add(Integer.toString(this.ppn));
        lArgs.add(this.mpiFlags);
        lArgs.add(Boolean.toString(scaleByCU));
        lArgs.add(this.params);
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
        return this.params;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.MPI;
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
        StringBuilder sb = new StringBuilder("{\"type\":\"MPI\",");
        sb.append("\"mpi_runner\":\"").append(this.mpiRunner).append("\",");
        sb.append("\"mpi_ppn\":").append(this.ppn).append(",");
        sb.append("\"mpi_flags\":\"").append(this.mpiFlags).append("\",");
        sb.append("\"binary\":\"").append(this.binary).append("\",");
        sb.append("\"params\":\"").append(this.params).append("\",");
        sb.append("\"container\":").append(this.container == null ? null : this.container.toJSON());
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return "MPI Method with binary " + this.binary + " and MPIrunner " + this.mpiRunner;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.mpiRunner = (String) in.readObject();
        this.ppn = in.readInt();
        this.mpiFlags = (String) in.readObject();
        this.binary = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.scaleByCU = in.readBoolean();
        this.params = (String) in.readObject();
        this.failByEV = in.readBoolean();
        this.container = (ContainerDescription) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.mpiRunner);
        out.writeInt(this.ppn);
        out.writeObject(this.mpiFlags);
        out.writeObject(this.binary);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.scaleByCU);
        out.writeObject(this.params);
        out.writeBoolean(this.failByEV);
        out.writeObject(this.container);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MPI Implementation \n");
        sb.append("\t Binary: ").append(binary).append("\n");
        sb.append("\t MPI runner: ").append(mpiRunner).append("\n");
        sb.append("\t MPI PPN: ").append(ppn).append("\n");
        sb.append("\t MPI flags: ").append(mpiFlags).append("\n");
        sb.append("\t Working directory: ").append(workingDir).append("\n");
        sb.append("\t Scale by Computing Units: ").append(scaleByCU).append("\n");
        sb.append("\t Params String: ").append(this.params).append("\n");
        sb.append("\t Fail by EV: ").append(this.failByEV).append("\n");
        sb.append("\t Container: ").append(this.container).append("\n");
        return sb.toString();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    @Override
    public void checkArguments() {
        super.checkArguments();
        if (this.binary == null || this.binary.isEmpty()) {
            throw new IllegalArgumentException(ERROR_MPI_BINARY);
        }
    }

    public boolean hasParamsString() {
        return this.params != null && !this.params.equals(Constants.UNASSIGNED);
    }
}
