/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class COMPSsImplementation extends AbstractMethodImplementation implements Externalizable {

    public static final int NUM_PARAMS = 5;

    private static final String DEFAULT_RUNCOMPSS = "runcompss";
    private static final String DEFAULT_FLAGS = "";

    private String runcompss;
    private String flags;
    private String appName;
    private String workerInMaster;
    private String workingDir;


    /**
     * Creates a new COMPSsImplementation for serialization.
     */
    public COMPSsImplementation() {
        // For externalizable
        super();
    }

    /**
     * Creates a new COMPSsImplementation from the given parameters.
     * 
     * @param runcompss Runcompss binary path.
     * @param flags Runcompss user flags.
     * @param appName Application name.
     * @param workerInMaster Whether the nested COMPSs execution should spawn a worker in the master node or not.
     * @param workingDir The nested COMPSs working directory.
     * @param coreId Core Id.
     * @param implementationId Implementation Id.
     * @param annot Method annotations.
     */
    public COMPSsImplementation(String runcompss, String flags, String appName, String workerInMaster,
            String workingDir, Integer coreId, Integer implementationId, MethodResourceDescription annot) {

        super(coreId, implementationId, annot);

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

    @Override
    public MethodType getMethodType() {
        return MethodType.COMPSs;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[RUNCOMPSS=").append(this.runcompss);
        sb.append(", FLAGS=").append(this.flags);
        sb.append(", APP_NAME=").append(this.appName);
        sb.append(", WORKER_IN_MASTER=").append(this.workerInMaster);
        sb.append(", WORKING_DIR=").append(this.workingDir);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " COMPSs Method with appName " + this.appName;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.runcompss = (String) in.readObject();
        this.flags = (String) in.readObject();
        this.appName = (String) in.readObject();
        this.workerInMaster = (String) in.readObject();
        this.workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.runcompss);
        out.writeObject(this.flags);
        out.writeObject(this.appName);
        out.writeObject(this.workerInMaster);
        out.writeObject(this.workingDir);
    }

}
