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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class COMPSsDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 6;
    public static final String SIGNATURE = "compss.NESTED";

    private static final String DEFAULT_RUNCOMPSS = "runcompss";
    private static final String DEFAULT_FLAGS = "";
    // Annotation properties

    private String runcompss;
    private String flags;
    private String appName;
    private String workerInMaster;
    private String workingDir;
    private boolean failByEV;

    // Parent application details
    private String parentAppId;


    /**
     * Creates a new COMPSsImplementation for serialization.
     */
    public COMPSsDefinition() {
        // For externalizable
    }

    /**
     * Creates a new COMPSsImplementation from the given parameters.
     * 
     * @param runcompss Runcompss binary path.
     * @param flags Runcompss user flags.
     * @param appName Application name.
     * @param workerInMaster Whether the nested COMPSs execution should spawn a worker in the master node or not.
     * @param workingDir The nested COMPSs working directory.
     * @param failByEV Flag to enable failure with EV.
     */
    public COMPSsDefinition(String runcompss, String flags, String appName, String workerInMaster, String workingDir,
        boolean failByEV) {

        if (runcompss != null && !runcompss.isEmpty() && !runcompss.equals(Constants.UNASSIGNED)) {
            this.runcompss = runcompss;
        } else {
            this.runcompss = DEFAULT_RUNCOMPSS;
        }
        if (flags != null && !flags.isEmpty() && !flags.equals(Constants.UNASSIGNED)) {
            this.flags = flags;
        } else {
            this.flags = DEFAULT_FLAGS;
        }
        this.appName = appName;
        this.workerInMaster = workerInMaster;
        this.workingDir = workingDir;
        this.failByEV = failByEV;
        this.parentAppId = new File(System.getProperty(COMPSsConstants.APP_LOG_DIR)).getName();
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public COMPSsDefinition(String[] implTypeArgs, int offset) {
        this.runcompss = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.flags = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        if (this.runcompss == null || this.runcompss.isEmpty() | this.runcompss.equals(Constants.UNASSIGNED)) {
            this.runcompss = DEFAULT_RUNCOMPSS;
        }
        if (this.flags == null || this.flags.isEmpty() || this.flags.equals(Constants.UNASSIGNED)) {
            this.flags = DEFAULT_FLAGS;
        }
        this.appName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.workerInMaster = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 3]);
        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 4]);
        this.failByEV = Boolean.parseBoolean(implTypeArgs[5]);
        String appLogDir = System.getProperty(COMPSsConstants.APP_LOG_DIR);
        if (appLogDir != null && !appLogDir.isEmpty()) {
            this.parentAppId = new File(System.getProperty(COMPSsConstants.APP_LOG_DIR)).getName();
        }
        if (appName == null || appName.isEmpty()) {
            throw new IllegalArgumentException("Empty appName annotation for COMPSs method ");
        }
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(this.runcompss);
        lArgs.add(this.flags);
        lArgs.add(this.appName);
        lArgs.add(this.workerInMaster);
        lArgs.add(this.workingDir);
        lArgs.add(Boolean.toString(this.failByEV));
    }

    /**
     * Returns the runcompss binary path.
     * 
     * @return The runcompss binary path.
     */
    public String getRuncompss() {
        return this.runcompss;
    }

    /**
     * Returns the runcompss user flags.
     * 
     * @return The runcompss user flags.
     */
    public String getFlags() {
        return this.flags;
    }

    /**
     * Returns the nested application name.
     * 
     * @return The nested application name.
     */
    public String getAppName() {
        return this.appName;
    }

    /**
     * Returns whether the nested COMPSs execution should spawn a worker in the master node or not.
     * 
     * @return A string containing the expression to spawn a worker in the master node or not.
     */
    public String getWorkerInMaster() {
        return this.workerInMaster;
    }

    /**
     * Returns the nested COMPSs working directory.
     * 
     * @return The nested COMPSs working directory.
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

    /**
     * Returns the parent app Identifier.
     * 
     * @return The parent app Identifier.
     */
    public String getParentAppId() {
        return this.parentAppId;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.COMPSs;
    }

    @Override
    public String toMethodDefinitionFormat() {
        StringBuilder sb = new StringBuilder();
        sb.append("[RUNCOMPSS=").append(this.runcompss);
        sb.append(", FLAGS=").append(this.flags);
        sb.append(", APP_NAME=").append(this.appName);
        sb.append(", WORKER_IN_MASTER=").append(this.workerInMaster);
        sb.append(", WORKING_DIR=").append(this.workingDir);
        sb.append(", FAIL_BY_EV=").append(this.failByEV);
        sb.append(", PARENT_APP_ID=").append(this.parentAppId);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return " COMPSs Method with appName " + this.appName;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.runcompss = (String) in.readObject();
        this.flags = (String) in.readObject();
        this.appName = (String) in.readObject();
        this.workerInMaster = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.failByEV = in.readBoolean();
        this.parentAppId = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.runcompss);
        out.writeObject(this.flags);
        out.writeObject(this.appName);
        out.writeObject(this.workerInMaster);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.failByEV);
        out.writeObject(this.parentAppId);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
