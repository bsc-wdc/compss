/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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


public class JuliaDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 5;
    private static final String ERROR_JULIA_SCRIPT = "ERROR: Invalid juliaScript";

    public static final String SCRIPT_PATH = File.separator + "Runtime" + File.separator + "scripts" + File.separator
        + "system" + File.separator + "julia" + File.separator + "run_julia.sh";

    private String juliaExecutor;
    private String juliaScript;
    private String workingDir;
    private boolean failByEV;
    private int computingNodes;


    /**
     * Creates a new JuliafImplementation instance for serialization.
     */
    public JuliaDefinition() {
        // For externalizable
    }

    /**
     * Creates a new JuliaImplementation instance from the given parameters.
     * 
     * @param juliaScript Path to julia script.
     * @param juliaExecutor Path to julia executor.
     * @param workingDir Working directory.
     * @param failByEV Flag to enable failure with EV.
     * @param computingNodes Number of computing nodes.
     */
    public JuliaDefinition(String juliaExecutor, String juliaScript, String workingDir, boolean failByEV,
        int computingNodes) {
        this.juliaExecutor = juliaExecutor;
        this.juliaScript = juliaScript;
        this.workingDir = workingDir;
        this.failByEV = failByEV;
        this.computingNodes = computingNodes;
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public JuliaDefinition(String[] implTypeArgs, int offset) {
        this.juliaExecutor = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.juliaScript = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.failByEV = Boolean.parseBoolean(implTypeArgs[offset + 3]);
        this.computingNodes = Integer.parseInt(implTypeArgs[offset + 4]);
        if (juliaScript == null || juliaScript.isEmpty()) {
            throw new IllegalArgumentException("Empty juliaScript annotation for JULIA method ");
        }
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        String script = this.juliaScript;
        if (!script.startsWith(File.separator)) {
            script = auxParam + File.separator + script;
        }
        lArgs.add(script);

        String executor = this.juliaExecutor;
        if (executor == null || executor.isEmpty() || executor.equals(Constants.UNASSIGNED)) {
            executor = "julia";
        }
        lArgs.add(executor);

        String workingDir = this.workingDir;
        if (workingDir == null || workingDir.isEmpty() || workingDir.equals(Constants.UNASSIGNED)) {
            workingDir = ".";
        }
        lArgs.add(workingDir);

        lArgs.add(Boolean.toString(failByEV));
        lArgs.add(Integer.toString(this.computingNodes));
    }

    /**
     * Returns the julia script.
     * 
     * @return The julia script.
     */
    public String getJuliaScript() {
        return this.juliaScript;
    }

    /**
     * Returns the julia executor.
     * 
     * @return The julia executor.
     */
    public String getJuliaExecutor() {
        return this.juliaExecutor;
    }

    /**
     * Returns the julia working dir.
     * 
     * @return The julia working dir.
     */
    public String getWorkingDir() {
        return this.workingDir;
    }

    /**
     * Returns the julia fail by ev.
     * 
     * @return The julia fail by ev.
     */
    public boolean getFailByEV() {
        return this.failByEV;
    }

    /**
     * Returns the julia computing nodes.
     * 
     * @return The julia computing nodes.
     */
    public int getComputingNodes() {
        return this.computingNodes;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.JULIA;
    }

    @Override
    public String toMethodDefinitionFormat() {
        StringBuilder sb = new StringBuilder();
        sb.append("[JULIA_EXECUTOR=").append(this.juliaExecutor);
        sb.append(", JULIA_SCRIPT=").append(this.juliaScript);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return super.toString() + " Julia Method with executor " + this.juliaExecutor + " and script "
            + this.juliaScript;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("JULIA Implementation \n");
        sb.append("\t Julia executor: ").append(juliaExecutor).append("\n");
        sb.append("\t Julia script: ").append(juliaScript).append("\n");
        sb.append("\t Working directory: ").append(workingDir).append("\n");
        sb.append("\t Fail by EV: ").append(this.failByEV).append("\n");
        sb.append("\t Computing Nodes: ").append(this.computingNodes).append("\n");
        return sb.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.juliaScript = (String) in.readObject();
        this.juliaExecutor = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.failByEV = in.readBoolean();
        this.computingNodes = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.juliaScript);
        out.writeObject(this.juliaExecutor);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.failByEV);
        out.writeInt(this.computingNodes);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    /**
     * Check Julia arguments.
     */
    public void checkArguments() {
        if (this.juliaExecutor == null || this.juliaExecutor.isEmpty()
            || this.juliaExecutor.equals(Constants.UNASSIGNED)) {
            this.juliaExecutor = "julia";
        }
        if (this.juliaScript == null || this.juliaScript.isEmpty()) {
            throw new IllegalArgumentException(ERROR_JULIA_SCRIPT);
        }
    }

    public void setJuliaScript(String juliaScript) {
        this.juliaScript = juliaScript;
    }

}
