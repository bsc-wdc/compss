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

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class DecafDefinition extends CommonMPIDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 6;
    public static final String SIGNATURE = "decaf.DECAF";
    private static final String ERROR_DECAF_BINARY = "ERROR: Invalid wfScript";

    public static final String SCRIPT_PATH = File.separator + "Runtime" + File.separator + "scripts" + File.separator
        + "system" + File.separator + "decaf" + File.separator + "run_decaf.sh";

    private String dfScript;
    private String dfExecutor;
    private String dfLib;


    /**
     * Creates a new DecafImplementation instance for serialization.
     */
    public DecafDefinition() {
        // For externalizable
    }

    /**
     * Creates a new DecafImplementation instance from the given parameters.
     * 
     * @param dfScript Path to df script.
     * @param dfExecutor Path to df executor.
     * @param dfLib Path to df library.
     * @param workingDir Working directory.
     * @param mpiRunner Path to MPI binary command.
     * @param failByEV Flag to enable failure with EV.
     */
    public DecafDefinition(String dfScript, String dfExecutor, String dfLib, String workingDir, String mpiRunner,
        boolean failByEV) {
        super(workingDir, mpiRunner, 1, "", true, failByEV);
        this.dfScript = dfScript;
        this.dfExecutor = dfExecutor;
        this.dfLib = dfLib;
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public DecafDefinition(String[] implTypeArgs, int offset) {
        this.dfScript = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.dfExecutor = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.dfLib = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 3]);
        this.mpiRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 4]);
        this.failByEV = Boolean.parseBoolean(implTypeArgs[offset + 5]);
        if (mpiRunner == null || mpiRunner.isEmpty()) {
            throw new IllegalArgumentException("Empty mpiRunner annotation for DECAF method ");
        }
        if (dfScript == null || dfScript.isEmpty()) {
            throw new IllegalArgumentException("Empty dfScript annotation for DECAF method ");
        }
        // Set default values for this extra mpi parameters
        this.scaleByCU = true;
        this.mpiFlags = "";
        this.ppn = 1;
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        String script = this.dfScript;
        if (!script.startsWith(File.separator)) {
            script = auxParam + File.separator + script;
        }
        lArgs.add(script);

        String executor = this.dfExecutor;
        if (executor == null || executor.isEmpty() || executor.equals(Constants.UNASSIGNED)) {
            executor = "executor.sh";
        }
        if (!executor.startsWith(File.separator) && !executor.startsWith("./")) {
            executor = "./" + executor;
        }
        lArgs.add(executor);

        String lib = this.dfLib;
        if (lib == null || lib.isEmpty()) {
            lib = Constants.UNASSIGNED;
        }
        lArgs.add(lib);
        lArgs.add(this.workingDir);
        lArgs.add(this.mpiRunner);
        lArgs.add(Boolean.toString(this.failByEV));

    }

    /**
     * Returns the df script.
     * 
     * @return The df script.
     */
    public String getDfScript() {
        return this.dfScript;
    }

    /**
     * Returns the df executor.
     * 
     * @return The df executor.
     */
    public String getDfExecutor() {
        return this.dfExecutor;
    }

    /**
     * Returns the df library.
     * 
     * @return The df library.
     */
    public String getDfLib() {
        return this.dfLib;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.DECAF;
    }

    @Override
    public String toJSON() {
        StringBuilder sb = new StringBuilder("{\"type\":\"DECAF\",");
        sb.append("\"mpi_runner\":\"").append(this.mpiRunner).append("\",");
        sb.append("\"df_script\":\"").append(this.dfScript).append("\",");
        sb.append("\"df_executor\":\"").append(this.dfExecutor).append("\",");
        sb.append("\"df_library\":\"").append(this.dfLib).append("\"");
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return super.toString() + " Decaf Method with script " + this.dfScript + ", executor " + this.dfScript
            + ", library " + this.dfLib + " and MPIrunner " + this.mpiRunner;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DECAF Implementation \n");
        sb.append("\t Decaf script: ").append(dfScript).append("\n");
        sb.append("\t Decaf executor: ").append(dfExecutor).append("\n");
        sb.append("\t Decaf lib: ").append(dfLib).append("\n");
        sb.append("\t MPI runner: ").append(mpiRunner).append("\n");
        sb.append("\t Working directory: ").append(workingDir).append("\n");
        sb.append("\t Fail by EV: ").append(this.failByEV).append("\n");
        return sb.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.mpiRunner = (String) in.readObject();
        this.dfScript = (String) in.readObject();
        this.dfExecutor = (String) in.readObject();
        this.dfLib = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.failByEV = in.readBoolean();

        // Set default values for this extra mpi parameters
        this.scaleByCU = true;
        this.mpiFlags = "";
        this.ppn = 1;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.mpiRunner);
        out.writeObject(this.dfScript);
        out.writeObject(this.dfExecutor);
        out.writeObject(this.dfLib);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.failByEV);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    @Override
    public void checkArguments() {
        if (this.dfScript == null || this.dfScript.isEmpty()) {
            throw new IllegalArgumentException(ERROR_DECAF_BINARY);
        }

        if (this.dfExecutor == null || this.dfExecutor.isEmpty() || this.dfExecutor.equals(Constants.UNASSIGNED)) {
            this.dfExecutor = "executor.sh";
        }
        if (!this.dfExecutor.startsWith(File.separator) && !this.dfExecutor.startsWith("./")) {
            this.dfExecutor = "./" + this.dfExecutor;
        }
        if (this.dfLib == null || this.dfLib.isEmpty()) {
            this.dfLib = "null";
        }
    }

    public void setDfScript(String dfScript) {
        this.dfScript = dfScript;

    }

}
