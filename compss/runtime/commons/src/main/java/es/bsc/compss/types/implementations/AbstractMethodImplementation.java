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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.exceptions.LangNotDefinedException;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public abstract class AbstractMethodImplementation extends Implementation implements Externalizable {

    private static final Lang LANG;


    /**
     * Enum matching the different method types.
     */
    public enum MethodType {
        METHOD, // For native methods
        BINARY, // For binary methods
        MPI, // For MPI methods
        COMPSs, // For COMPSs nested applications
        DECAF, // For decaf methods
        MULTI_NODE, // For native multi-node methods
        OMPSS, // For OmpSs methods
        OPENCL // For OpenCL methods
    }


    static {
        // Compute language
        Lang l = Lang.JAVA;

        String langProperty = System.getProperty(COMPSsConstants.LANG);
        if (langProperty != null) {
            if (langProperty.equalsIgnoreCase("python")) {
                l = Lang.PYTHON;
            } else if (langProperty.equalsIgnoreCase("c")) {
                l = Lang.C;
            }
        }

        LANG = l;
    }


    /**
     * New AbstractMethodImplementation instance for serialization.
     */
    public AbstractMethodImplementation() {
        // For externalizable
        super();
    }

    /**
     * New AbstractMethodImplementation instance for the given core Id {@code coreId} - implementation Id
     * {@code implementationId}, and with the annotations {@code annot}.
     * 
     * @param coreId Associated core Id.
     * @param implementationId Associated implementation Id.
     * @param annot Associated annotations.
     */
    public AbstractMethodImplementation(Integer coreId, Integer implementationId, MethodResourceDescription annot) {
        super(coreId, implementationId, annot);
    }

    /**
     * Builds the signature from the given parameters.
     * 
     * @param declaringClass Method declaring classs.
     * @param methodName Method name.
     * @param hasTarget Whether the method has target object or not.
     * @param numReturns The number of return values of the method.
     * @param parameters The number of parameters of the method.
     * @return The signature built from the given parameters.
     */
    public static String getSignature(String declaringClass, String methodName, boolean hasTarget, int numReturns,
            Parameter[] parameters) {

        StringBuilder buffer = new StringBuilder();
        buffer.append(methodName).append("(");

        switch (LANG) {
            case JAVA:
            case C:
                int numPars = parameters.length;
                if (hasTarget) {
                    numPars--;
                }
                numPars -= numReturns;
                if (numPars > 0) {
                    DataType type = parameters[0].getType();
                    type = (type == DataType.PSCO_T) ? DataType.OBJECT_T : type;
                    buffer.append(type);
                    for (int i = 1; i < numPars; i++) {
                        type = parameters[i].getType();
                        type = (type == DataType.PSCO_T) ? DataType.OBJECT_T : type;
                        buffer.append(",").append(type);
                    }
                }
                break;
            case PYTHON:
                // There is no function overloading in Python
                break;
            case UNKNOWN:
                throw new LangNotDefinedException();
        }

        buffer.append(")").append(declaringClass);
        return buffer.toString();
    }

    /**
     * Returns the internal method type.
     * 
     * @return The internal method type.
     */
    public abstract MethodType getMethodType();

    /**
     * Returns the method definition.
     * 
     * @return The method definition.
     */
    public abstract String getMethodDefinition();

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    @Override
    public MethodResourceDescription getRequirements() {
        return (MethodResourceDescription) this.requirements;
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

}
