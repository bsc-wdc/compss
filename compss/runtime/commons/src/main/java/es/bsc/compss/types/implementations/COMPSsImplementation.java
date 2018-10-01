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

import es.bsc.compss.types.resources.MethodResourceDescription;


public class COMPSsImplementation extends AbstractMethodImplementation implements Externalizable {

    private String mainClass;
    private String flags;
    private String workingDir;


    public COMPSsImplementation() {
        // For externalizable
        super();
    }

    public COMPSsImplementation(String mainClass, String flags, String workingDir, Integer coreId, Integer implementationId,
            MethodResourceDescription annot) {

        super(coreId, implementationId, annot);

        this.mainClass = mainClass;
        this.flags = flags;
        this.workingDir = workingDir;
    }

    public String getMainClass() {
        return this.mainClass;
    }

    public String getFlags() {
        return this.flags;
    }

    public String getWorkingDir() {
        return this.workingDir;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.MPI;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[MAIN_CLASS=").append(mainClass);
        sb.append(", FLAGS=").append(flags);
        sb.append(", WORKING_DIR=").append(workingDir);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " COMPSs Method with mainClass " + mainClass;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        mainClass = (String) in.readObject();
        flags = (String) in.readObject();
        workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(mainClass);
        out.writeObject(flags);
        out.writeObject(workingDir);
    }

}
