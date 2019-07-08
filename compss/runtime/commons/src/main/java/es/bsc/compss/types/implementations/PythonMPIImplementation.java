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

import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class PythonMPIImplementation extends AbstractMethodImplementation implements Externalizable {

    public static final int NUM_PARAMS = 3;

    private String declaringClass;
    private String alternativeMethod;
    private String mpiRunner;
    private String workingDir;


    /**
     * Creates a new MPIImplementation for serialization.
     */
    public PythonMPIImplementation() {
        // For externalizable
        super();
    }

    /**
     * Creates a new Python MPI Implementation.
     * 
     * @param methodClass Method class.
     * @param altMethodName Alternative method name.
     * @param workingDir Binary working directory.
     * @param mpiRunner Path to the MPI command.
     * @param coreId Core Id.
     * @param implementationId Implementation Id.
     * @param requirements Method annotations.
     */
    public PythonMPIImplementation(String methodClass, String altMethodName, String workingDir, String mpiRunner,
            Integer coreId, Integer implementationId, MethodResourceDescription requirements) {

        super(coreId, implementationId, requirements);

        this.declaringClass = methodClass;
        this.alternativeMethod = altMethodName;
        this.mpiRunner = mpiRunner;
        this.workingDir = workingDir;
    }

    /**
     * Returns the method declaring class.
     * 
     * @return The method declaring class.
     */
    public String getDeclaringClass() {
        return this.declaringClass;
    }

    /**
     * Returns the alternative method name.
     * 
     * @return The alternative method name.
     */
    public String getAlternativeMethodName() {
        return this.alternativeMethod;
    }

    /**
     * Sets a new alternative method name.
     * 
     * @param alternativeMethod The new alternative method name.
     */
    public void setAlternativeMethodName(String alternativeMethod) {
        this.alternativeMethod = alternativeMethod;
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
     * Returns the path to the MPI command.
     * 
     * @return The path to the MPI command.
     */
    public String getMpiRunner() {
        return this.mpiRunner;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.PYTHON_MPI;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[DECLARING CLASS=").append(this.declaringClass);
        sb.append(", METHOD NAME=").append(this.alternativeMethod);
        sb.append(", MPI RUNNER=").append(this.mpiRunner);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " Python MPI Method declared in class " + this.declaringClass + "."
                + this.alternativeMethod + " with MPIrunner " + this.mpiRunner + ": " + this.requirements.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.declaringClass = (String) in.readObject();
        this.alternativeMethod = (String) in.readObject();
        this.mpiRunner = (String) in.readObject();
        this.workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.declaringClass);
        out.writeObject(this.alternativeMethod);
        out.writeObject(this.mpiRunner);
        out.writeObject(this.workingDir);
    }

}
