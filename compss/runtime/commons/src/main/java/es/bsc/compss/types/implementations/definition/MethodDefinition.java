/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.util.EnvironmentLoader;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;


public class MethodDefinition extends NativeDefinition {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 3;

    private String declaringClass;
    // In C implementations could have different method names
    private String alternativeMethod;


    /**
     * Creates a new MethodImplementation for serialization.
     */
    public MethodDefinition() {
        // For externalizable
    }

    /**
     * Creates a new MethodImplementation instance from the given parameters. * @param lang Language implementing the
     * method
     * 
     * @param methodClass Method class.
     * @param altMethodName Method name.
     */
    public MethodDefinition(Lang lang, String methodClass, String altMethodName) {
        super(lang);
        this.declaringClass = methodClass;
        this.alternativeMethod = altMethodName;
    }

    /**
     * Creates a new Definition from string array.
     * 
     * @param implTypeArgs String array.
     * @param offset Element from the beginning of the string array.
     */
    public MethodDefinition(String[] implTypeArgs, int offset) {
        super(implTypeArgs, offset);
        this.declaringClass = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 1]);
        this.alternativeMethod = EnvironmentLoader.loadFromEnvironment(implTypeArgs[offset + 2]);
        if (declaringClass == null || declaringClass.isEmpty()) {
            throw new IllegalArgumentException("Empty declaringClass annotation for method " + this.alternativeMethod);
        }
        if (alternativeMethod == null || alternativeMethod.isEmpty()) {
            throw new IllegalArgumentException("Empty methodName annotation for method " + declaringClass);
        }
    }

    @Override
    public void appendToArgs(List<String> lArgs, String auxParam) {
        super.appendToArgs(lArgs, auxParam);
        lArgs.add(this.declaringClass);
        lArgs.add(this.alternativeMethod);
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

    @Override
    public MethodType getMethodType() {
        return MethodType.METHOD;
    }

    @Override
    public String toJSON() {
        StringBuilder sb = new StringBuilder("{\"type\":\"METHOD\",");
        sb.append(super.toJSON()).append(",");
        sb.append("\"declaring_class\":\"").append(this.declaringClass).append("\",");
        sb.append("\"method_name\":\"").append(this.alternativeMethod).append("\"");
        sb.append("}");
        return sb.toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("METHOD Implementation \n");
        sb.append(super.toString()).append("\n");
        sb.append("\t Declaring class: ").append(declaringClass).append("\n");
        sb.append("\t Method name: ").append(alternativeMethod).append("\n");
        return sb.toString();
    }

    @Override
    public String toShortFormat() {
        return super.toShortFormat() + " method declared in class " + this.declaringClass + "." + alternativeMethod
            + ": " + this.alternativeMethod;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.declaringClass = (String) in.readObject();
        this.alternativeMethod = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.declaringClass);
        out.writeObject(this.alternativeMethod);
    }

}
