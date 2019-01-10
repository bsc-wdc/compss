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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;

import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.exceptions.LangNotDefinedException;


public abstract class AbstractMethodImplementation extends Implementation implements Externalizable {

    private static final Lang LANG;


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


    public AbstractMethodImplementation() {
        // For externalizable
        super();
    }

    public AbstractMethodImplementation(Integer coreId, Integer implementationId, MethodResourceDescription annot) {
        super(coreId, implementationId, annot);
    }

    public static String getSignature(String declaringClass, String methodName, boolean hasTarget, int numReturns, Parameter[] parameters) {

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

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

    public abstract MethodType getMethodType();

    public abstract String getMethodDefinition();

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
