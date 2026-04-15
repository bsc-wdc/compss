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
package es.bsc.compss.util;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.exceptions.LangNotDefinedException;
import es.bsc.compss.types.parameter.impl.Parameter;
import java.util.List;

public class SignatureBuilder {

    private static final Lang LANG;

    static {
        // Compute language
        Lang l = Lang.JAVA;

        String langProperty = System.getProperty(COMPSsConstants.LANG);
        if (langProperty != null) {
            if (langProperty.equalsIgnoreCase("python")) {
                l = Lang.PYTHON;
            } else if (langProperty.equalsIgnoreCase("c")) {
                l = Lang.C;
            } else if (langProperty.equalsIgnoreCase("r")) {
                l = Lang.R;
            }
        }

        LANG = l;
    }


    /**
     * Private constructor to avoid the instantiation of the class.
     */
    private SignatureBuilder() {
    }

    /**
     * Builds the signature from the given parameters.
     * 
     * @param declaringClass Method declaring class.
     * @param methodName Method name.
     * @param hasTarget Whether the method has target object or not.
     * @param numReturns The number of return values of the method.
     * @param parameters The number of parameters of the method.
     * @return The signature built from the given parameters.
     */
    public static String getMethodSignature(String declaringClass, String methodName, boolean hasTarget, int numReturns,
        List<Parameter> parameters) {

        StringBuilder buffer = new StringBuilder();
        buffer.append(methodName).append("(");

        switch (LANG) {
            case JAVA:
            case C:
                int numPars = parameters.size();
                if (hasTarget) {
                    numPars--;
                }
                numPars -= numReturns;
                if (numPars > 0) {
                    DataType type = parameters.get(0).getType();
                    type = (type == DataType.PSCO_T) ? DataType.OBJECT_T : type;
                    buffer.append(type);
                    for (int i = 1; i < numPars; i++) {
                        type = parameters.get(i).getType();
                        type = (type == DataType.PSCO_T) ? DataType.OBJECT_T : type;
                        buffer.append(",").append(type);
                    }
                }
                break;
            case PYTHON:
            case R:
                // There is no function overloading in Python
                break;
            case UNKNOWN:
                throw new LangNotDefinedException();
        }

        buffer.append(")");
        if (declaringClass != null && !"NULL".equals(declaringClass)) {
            buffer.append(declaringClass);
        }
        return buffer.toString();
    }

}
