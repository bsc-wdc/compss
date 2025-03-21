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

import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class MultiNodeDefinition implements AbstractMethodImplementationDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 3;

    private String declaringClass;
    private String methodName;
    private int ppn = 1;


    /**
     * Creates a new MultiNodeImplementation for serialization.
     */
    public MultiNodeDefinition() {
        // For externalizable
    }

    /**
     * Creates a new MultiNodeImplementation instance from the given parameters.
     *
     * @param methodClass Class name.
     * @param methodName Method name.
     */
    public MultiNodeDefinition(String methodClass, String methodName, int ppn) {

        this.declaringClass = methodClass;
        this.methodName = methodName;
        this.ppn = ppn;
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public MultiNodeDefinition(String[] implTypeArgs, int offset) {
        declaringClass = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset]);
        methodName = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        if (declaringClass == null || declaringClass.isEmpty()) {
            throw new IllegalArgumentException("Empty declaringClass annotation for method ");
        }
        if (methodName == null || methodName.isEmpty()) {
            throw new IllegalArgumentException("Empty methodName annotation for method ");
        }
        String ppnStr = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        if (ppnStr == null || ppnStr.isEmpty()) {
            this.ppn = 1;
        } else {
            this.ppn = Integer.parseInt(ppnStr);
        }
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        lArgs.add(this.declaringClass);
        lArgs.add(this.methodName);
        lArgs.add(Integer.toString(this.ppn));
    }

    /**
     * Returns the method declaring class.
     *
     * @return The method declaring class.
     */
    public String getDeclaringClass() {
        return declaringClass;
    }

    /**
     * Returns the method name.
     *
     * @return The method name.
     */
    public String getMethodName() {
        return this.methodName;
    }

    /**
     * Sets a new method name.
     *
     * @param methodName New method name.
     */
    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public int getPPN() {
        return ppn;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.MULTI_NODE;
    }

    @Override
    public String toJSON() {
        StringBuilder sb = new StringBuilder("{\"type\":\"MULTI_NODE\",");
        sb.append("\"declaring_class\":\"").append(this.declaringClass).append("\",");
        sb.append("\"method_name\":\"").append(this.methodName).append("\",");
        sb.append("\"ppn\":").append(this.ppn).append("");
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("MultiNode Implementation \n");
        sb.append("\t Class: ").append(this.declaringClass).append("\n");
        sb.append("\t Name: ").append(this.methodName).append("\n");
        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return " Multi-Node Method declared in class " + this.declaringClass + "." + methodName;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.declaringClass = (String) in.readObject();
        this.methodName = (String) in.readObject();
        this.ppn = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.declaringClass);
        out.writeObject(this.methodName);
        out.writeInt(this.ppn);
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
