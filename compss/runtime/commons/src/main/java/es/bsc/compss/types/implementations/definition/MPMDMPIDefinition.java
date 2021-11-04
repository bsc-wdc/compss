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
package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.MPIProgram;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class MPMDMPIDefinition extends CommonMPIDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 5;
    public static final String SIGNATURE = "mpmdmpi.MPMDMPI";

    private static final String ERROR_MPI_BINARY = "ERROR: Empty binary annotation for MPMDMPI method";
    private MPIProgram[] programs;

    /**
     * Creates a new MPIImplementation for serialization.
     */
    public MPMDMPIDefinition() {
        // For externalizable
    }

    /**
     * Creates a new MPIImplementation instance from the given parameters.
     *
     * @param workingDir Binary working directory.
     * @param mpiRunner Path to the MPI command.
     * @param ppn Process per node.
     * @param failByEV Flag to enable failure with EV.
     * @param programs program definitions.
     */
    public MPMDMPIDefinition(String workingDir, String mpiRunner, int ppn, boolean failByEV,
                             MPIProgram[] programs) {
        super(workingDir, mpiRunner, ppn, "", false, failByEV);
        this.programs = programs;
    }

    /**
     * Creates a new Definition from string array.
     *
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public MPMDMPIDefinition(String[] implTypeArgs, int offset) {
        this.mpiRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.ppn = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]));
        this.failByEV = Boolean.parseBoolean(implTypeArgs[offset + 3]);

        // Multi Program
        int numOfProgs = Integer.parseInt(implTypeArgs[offset + 4]);
        this.programs = new MPIProgram[numOfProgs];

        for (int i = 0; i < numOfProgs; i++) {
            int index = offset + NUM_PARAMS + (i * MPIProgram.NUM_OF_PARAMS);
            String binary = EnvironmentLoader.loadFromEnvironment(implTypeArgs[index]);
            String params = EnvironmentLoader.loadFromEnvironment(implTypeArgs[index + 1]);
            int procs = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[index + 2]));
            this.programs[i] = new MPIProgram(binary, params, procs);
        }
        checkArguments();
    }

    public MPIProgram[] getPrograms() {
        return this.programs;
    }

    @Override
    public void checkArguments() {
        super.checkArguments();
        for (MPIProgram mpiProgram: this.programs) {
            if (mpiProgram.isEmpty()){
                throw new IllegalArgumentException(ERROR_MPI_BINARY);
            }
        }
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(this.mpiRunner);
        lArgs.add(this.workingDir);
        lArgs.add(Integer.toString(this.ppn));
        lArgs.add(Boolean.toString(failByEV));
        lArgs.add(Integer.toString(this.programs.length));
        for(MPIProgram program: this.programs){
            lArgs.add(program.getBinary());
            lArgs.add(program.getParams());
            lArgs.add(Integer.toString(program.getProcesses()));
        }
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.MPMDMPI;
    }

    @Override
    public String toMethodDefinitionFormat() {
        StringBuilder sb = new StringBuilder();

        sb.append("[MPMDMPI").append(this.mpiRunner);
        sb.append(", MPI RUNNER=").append(this.mpiRunner);
        sb.append(", WORKING DIR=").append(this.workingDir);
        sb.append(", PPN=").append(this.ppn);
        sb.append(", FAIL_BY_EV=").append(this.failByEV);
        sb.append(", NUM_OF_PROGRAMS=").append(this.programs.length);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return "MPMDMPI Method with MPIRunner " + this.mpiRunner + ", and "
                + this.programs.length+ " programs.";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.mpiRunner = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.ppn = in.readInt();
        this.failByEV = in.readBoolean();
        this.programs = (MPIProgram[]) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.mpiRunner);
        out.writeInt(this.ppn);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.failByEV);
        out.writeObject(this.programs);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MPMDMPI Implementation \n");
        sb.append("\t MPI runner: ").append(this.mpiRunner).append("\n");
        sb.append("\t Working directory: ").append(this.workingDir).append("\n");
        sb.append("\t MPI PPN: ").append(this.ppn).append("\n");
        sb.append("\t Fail by EV: ").append(this.failByEV).append("\n");
        sb.append("\t Programs: ").append("\n");

        for(MPIProgram prog: this.programs){
            sb.append("\t\t").append(prog).append("\n");
        }
        return sb.toString();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    @Override
    public String generateNumberOfProcesses(int numWorkers, int computingUnits) {
        throw new NotImplementedException();
    }

}
