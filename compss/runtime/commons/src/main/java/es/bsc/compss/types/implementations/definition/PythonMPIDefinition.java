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

import es.bsc.compss.types.CollectionLayout;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class PythonMPIDefinition extends CommonMPIDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 10;
    private static final String ERROR_MPI_DC = "ERROR: Empty declaring class for Python MPI method";
    private static final String ERROR_MPI_METHOD = "ERROR: Empty method name for Python MPI method";
    private String declaringClass;
    private String methodName;
    private String params;
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
    public PythonMPIDefinition(String methodClass, String altMethodName, String workingDir, String mpiRunner, int ppn,
        String mpiFlags, boolean scaleByCU, String params, boolean failByEV, CollectionLayout[] cls) {
        super(workingDir, mpiRunner, ppn, mpiFlags, scaleByCU, failByEV);
        this.declaringClass = methodClass;
        this.methodName = altMethodName;
        this.params = params;
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
        this.ppn = Integer.parseInt(EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 4]));
        this.mpiFlags = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 5]);
        this.scaleByCU = Boolean.parseBoolean(implTypeArgs[offset + 6]);
        this.params = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 7]);
        this.failByEV = Boolean.parseBoolean(implTypeArgs[offset + 8]);
        int numLayouts = Integer.parseInt(implTypeArgs[offset + 9]);
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
        lArgs.add(Integer.toString(this.ppn));
        lArgs.add(this.mpiFlags);
        lArgs.add(Boolean.toString(scaleByCU));
        lArgs.add(this.params);
        lArgs.add(Boolean.toString(failByEV));
        lArgs.add(Integer.toString(this.cls.length));
        for (CollectionLayout cl : this.cls) {
            lArgs.add(cl.getParamName());
            lArgs.add(Integer.toString(cl.getBlockCount()));
            lArgs.add(Integer.toString(cl.getBlockLen()));
            lArgs.add(Integer.toString(cl.getBlockStride()));
        }
    }

    public String getParams() {
        return this.params;
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
    public String toJSON() {
        StringBuilder sb = new StringBuilder("{\"type\":\"PYTHON_MPI\",");
        sb.append("\"declaring_class\":\"").append(this.declaringClass).append("\",");
        sb.append("\"method_name\":\"").append(this.methodName).append("\",");
        sb.append("\"mpi_runner\":\"").append(this.mpiRunner).append("\",");
        sb.append("\"mpi_ppn\":").append(this.ppn).append(",");
        sb.append("\"mpi_flags\":\"").append(this.mpiFlags).append("\",");
        sb.append("\"scale_by_cu\":").append(this.scaleByCU).append(",");
        sb.append("\"params\":\"").append(this.params).append("\",");
        sb.append("\"fail_by_ev\":").append(this.failByEV).append(",");
        sb.append("\"collection_layouts\":").append(this.cls.length);
        sb.append("}");
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
        sb.append("\t MPI ppn: ").append(ppn).append("\n");
        sb.append("\t MPI flags: ").append(mpiFlags).append("\n");
        sb.append("\t Working directory: ").append(workingDir).append("\n");
        sb.append("\t Scale by Computing Units: ").append(scaleByCU).append("\n");
        sb.append("\t Params String: ").append(this.params).append("\n");
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
        this.ppn = in.readInt();
        this.mpiFlags = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.scaleByCU = in.readBoolean();
        this.params = (String) in.readObject();
        this.failByEV = in.readBoolean();
        this.cls = (CollectionLayout[]) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.declaringClass);
        out.writeObject(this.methodName);
        out.writeObject(this.mpiRunner);
        out.writeInt(this.ppn);
        out.writeObject(this.mpiFlags);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.scaleByCU);
        out.writeObject(this.params);
        out.writeBoolean(this.failByEV);
        out.writeObject(this.cls);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    @Override
    public void checkArguments() {
        super.checkArguments();
        if (this.declaringClass == null || this.declaringClass.isEmpty()) {
            throw new IllegalArgumentException(ERROR_MPI_DC);
        }
        if (this.methodName == null || this.methodName.isEmpty()) {
            throw new IllegalArgumentException(ERROR_MPI_METHOD);
        }
    }

}
