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

import es.bsc.compss.types.CollectionLayout;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class PythonMPIDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 8;

    private String declaringClass;
    private String methodName;
    private String mpiRunner;
    private String workingDir;
    private String mpiFlags;
    private boolean scaleByCU;
    private boolean failByEV;
    private CollectionLayout[] cls;


    /**
     * Creates a new MPIImplementation for serialization.
     */
    public PythonMPIDefinition() {
        // For externalizable
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
     * @param cls Collections layouts.
     */
    public PythonMPIDefinition(String methodClass, String altMethodName, String workingDir, String mpiRunner,
        String mpiFlags, boolean scaleByCU, boolean failByEV, CollectionLayout[] cls) {
        this.declaringClass = methodClass;
        this.methodName = altMethodName;
        this.mpiRunner = mpiRunner;
        this.workingDir = workingDir;
        this.mpiFlags = mpiFlags;
        this.scaleByCU = scaleByCU;
        this.failByEV = failByEV;
        this.cls = cls;
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public PythonMPIDefinition(String[] implTypeArgs, int offset) {
        this.declaringClass = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        this.methodName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        if (declaringClass == null || declaringClass.isEmpty()) {
            throw new IllegalArgumentException("Empty declaringClass annotation for PythonMPI " + methodName);
        }
        if (methodName == null || methodName.isEmpty()) {
            throw new IllegalArgumentException("Empty methodName annotation for method PythonMPI" + declaringClass);
        }
        this.workingDir = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        this.mpiRunner = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 3]);
        this.mpiFlags = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 4]);
        this.scaleByCU = Boolean.parseBoolean(implTypeArgs[offset + 5]);
        this.failByEV = Boolean.parseBoolean(implTypeArgs[offset + 6]);
        int numLayouts = Integer.parseInt(implTypeArgs[offset + 7]);
        this.cls = new CollectionLayout[numLayouts];
        for (int i = 0; i < numLayouts; i++) {
            int index = offset + NUM_PARAMS + (i * 4);
            String pythonMPILayoutParam = EnvironmentLoader.loadFromEnvironment(implTypeArgs[index]);
            int pythonMPIBlockSize = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[index + 1]));
            int pythonMPIBlockLen = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[index + 2]));
            int pythonMPIBlockStride = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[index + 3]));
            cls[i] =
                new CollectionLayout(pythonMPILayoutParam, pythonMPIBlockSize, pythonMPIBlockLen, pythonMPIBlockStride);
        }

    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(this.declaringClass);
        lArgs.add(this.methodName);
        lArgs.add(this.workingDir);
        lArgs.add(this.mpiRunner);
        lArgs.add(this.mpiFlags);
        lArgs.add(Boolean.toString(scaleByCU));
        lArgs.add(Boolean.toString(failByEV));
        lArgs.add(Integer.toString(this.cls.length));
        for (CollectionLayout cl : this.cls) {
            lArgs.add(cl.getParamName());
            lArgs.add(Integer.toString(cl.getBlockCount()));
            lArgs.add(Integer.toString(cl.getBlockLen()));
            lArgs.add(Integer.toString(cl.getBlockStride()));
        }
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
        return this.methodName;
    }

    /**
     * Sets a new alternative method name.
     *
     * @param alternativeMethod The new alternative method name.
     */
    public void setAlternativeMethodName(String alternativeMethod) {
        this.methodName = alternativeMethod;
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
    }

    /**
     * Returns the collection layout.
     *
     * @return The collection layout.
     */
    public CollectionLayout[] getCollectionLayouts() {
        return this.cls;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.PYTHON_MPI;
    }

    @Override
    public String toMethodDefinitionFormat() {
        StringBuilder sb = new StringBuilder();
        sb.append("[DECLARING CLASS=").append(this.declaringClass);
        sb.append(", METHOD NAME=").append(this.methodName);
        sb.append(", MPI RUNNER=").append(this.mpiRunner);
        sb.append(", MPI FLAGS=").append(this.mpiFlags);
        sb.append(", SCALE_BY_CU=").append(this.scaleByCU);
        sb.append(", FAIL_BY_EV=").append(this.failByEV);
        sb.append(", Collection Layouts=").append(this.cls.length);

        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return super.toString() + " Python MPI Method declared in class " + this.declaringClass + "." + this.methodName
            + " with MPIrunner " + this.mpiRunner;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PYTHON MPI Implementation \n");
        sb.append("\t Declaring class: ").append(declaringClass).append("\n");
        sb.append("\t Method name: ").append(methodName).append("\n");
        sb.append("\t MPI runner: ").append(mpiRunner).append("\n");
        sb.append("\t MPI flags: ").append(mpiFlags).append("\n");
        sb.append("\t Working directory: ").append(workingDir).append("\n");
        sb.append("\t Scale by Computing Units: ").append(scaleByCU).append("\n");
        sb.append("\t Fail by EV: ").append(this.failByEV).append("\n");
        sb.append("\t Collection Layouts: ").append("\n");
        for (CollectionLayout cl : cls) {
            sb.append("\t\t").append(cl).append("\n");
        }
        return sb.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.declaringClass = (String) in.readObject();
        this.methodName = (String) in.readObject();
        this.mpiRunner = (String) in.readObject();
        this.mpiFlags = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.scaleByCU = in.readBoolean();
        this.failByEV = in.readBoolean();
        this.cls = (CollectionLayout[]) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.declaringClass);
        out.writeObject(this.methodName);
        out.writeObject(this.mpiRunner);
        out.writeObject(this.mpiFlags);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.scaleByCU);
        out.writeBoolean(this.failByEV);
        out.writeObject(this.cls);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
