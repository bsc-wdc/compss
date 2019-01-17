/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.resources.MethodResourceDescription;


public class COMPSsImplementation extends AbstractMethodImplementation implements Externalizable {

    public static final int NUM_PARAMS = 4;

    private static final String DEFAULT_RUNCOMPSS = "runcompss";
    private static final String DEFAULT_FLAGS = "";

    private String runcompss;
    private String flags;
    private String appName;
    private String workingDir;


    public COMPSsImplementation() {
        // For externalizable
        super();
    }

    public COMPSsImplementation(String runcompss, String flags, String appName, String workingDir, Integer coreId, Integer implementationId,
            MethodResourceDescription annot) {

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
        this.workingDir = workingDir;
    }

    public String getRuncompss() {
        return this.runcompss;
    }

    public String getFlags() {
        return this.flags;
    }

    public String getAppName() {
        return this.appName;
    }

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
        this.workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.runcompss);
        out.writeObject(this.flags);
        out.writeObject(this.appName);
        out.writeObject(this.workingDir);
    }

}
