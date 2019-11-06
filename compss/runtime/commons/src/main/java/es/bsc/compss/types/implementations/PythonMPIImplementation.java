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

    public static final int NUM_PARAMS = 7;

    private String declaringClass;
    private String alternativeMethod;
    private String mpiRunner;
    private String workingDir;
    private String mpiFlags;
    private boolean scaleByCU;
    private boolean failByEV;


    public class CollectionLayout {

        private String paramName = "";
        private int blockCount = -1;
        private int blockLen = -1;
        private int blockStride = -1;


        public String getParamName() {
            return this.paramName;
        }

        public int getBlockCount() {
            return this.blockCount;
        }

        public int getBlockLen() {
            return this.blockLen;
        }

        public int getBlockStride() {
            return this.blockStride;
        }
    }


    private CollectionLayout cl;


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
     * @param scaleByCU Scale by computing units property.
     * @param failByEV Flag to enable failure with EV.
     * @param coreId Core Id.
     * @param implementationId Implementation Id.
     * @param paramName Collection Layout's Parameter name.
     * @param blockCount Collection Layout's block count.
     * @param blockLen Collection Layout's block length.
     * @param blockStride Collection Layout's block stride.
     * @param signature Method signature.
     * @param requirements Method requirements.
     */
    public PythonMPIImplementation(String methodClass, String altMethodName, String workingDir, String mpiRunner,
        String mpiFlags, boolean scaleByCU, boolean failByEV, Integer coreId, Integer implementationId,
        String paramName, int blockCount, int blockLen, int blockStride,
        String signature, MethodResourceDescription requirements) {

        super(coreId, implementationId, signature, requirements);

        this.declaringClass = methodClass;
        this.alternativeMethod = altMethodName;
        this.mpiRunner = mpiRunner;
        this.workingDir = workingDir;
        this.mpiFlags = mpiFlags;
        this.scaleByCU = scaleByCU;
        this.failByEV = failByEV;
        this.cl = new CollectionLayout();
        this.cl.paramName = paramName;
        this.cl.blockCount = blockCount;
        this.cl.blockLen = blockLen;
        this.cl.blockStride = blockStride;
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

    /**
     * Returns the flags for the MPI command.
     * 
     * @return Flags for the MPI command.
     */
    public String getMpiFlags() {
        return this.mpiFlags;
    }

    /**
     * Returns the scale by computing units property.
     * 
     * @return scale by computing units property value.
     */
    public boolean getScaleByCU() {
        return this.scaleByCU;
    }

    /**
     * Check if fail by exit value is enabled.
     * 
     * @return True is fail by exit value is enabled.
     */
    public boolean isFailByEV() {
        return failByEV;

    /** 
     * Returns the collection layout.
     * 
     * @return The collection layout.
     */
    public CollectionLayout getCollectionLayout() {
        return this.cl;
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
        sb.append(", MPI FLAGS=").append(this.mpiFlags);
        sb.append(", SCALE_BY_CU=").append(this.scaleByCU);
        sb.append(", FAIL_BY_EV=").append(this.failByEV);
        sb.append(", Collection Layouts: ").append(this.cl.paramName).append(", ").append(this.cl.blockCount)
            .append(", ").append(this.cl.blockLen).append(", ").append(this.cl.blockStride);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " Python MPI Method declared in class " + this.declaringClass + "."
            + this.alternativeMethod + " with MPIrunner " + this.mpiRunner
            + ". with the following Collection Layout: \n" + "- Parameter Name: " + this.cl.paramName + "\n"
            + "- Block Count: " + cl.blockCount + "\n- Block Length: " + this.cl.blockLen + " \n- Block Stride: "
            + this.cl.blockStride + "\n " + this.requirements.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.declaringClass = (String) in.readObject();
        this.alternativeMethod = (String) in.readObject();
        this.mpiRunner = (String) in.readObject();
        this.mpiFlags = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.scaleByCU = in.readBoolean();
        this.failByEV = in.readBoolean();
        this.cl = new CollectionLayout();
        this.cl.paramName = (String) in.readObject();
        this.cl.blockCount = (int) in.readObject();
        this.cl.blockLen = (int) in.readObject();
        this.cl.blockStride = (int) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.declaringClass);
        out.writeObject(this.alternativeMethod);
        out.writeObject(this.mpiRunner);
        out.writeObject(this.mpiFlags);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.scaleByCU);
        out.writeBoolean(this.failByEV);
        out.writeObject(this.cl.paramName);
        out.writeObject(this.cl.blockCount);
        out.writeObject(this.cl.blockLen);
        out.writeObject(this.cl.blockStride);
    }

}
