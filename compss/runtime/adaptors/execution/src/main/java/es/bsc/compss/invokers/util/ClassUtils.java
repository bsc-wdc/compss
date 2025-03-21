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
package es.bsc.compss.invokers.util;

import es.bsc.compss.types.execution.InvocationParam;
import java.lang.reflect.Method;
import java.util.List;


public class ClassUtils {

    /**
     * Inspects a class to find a method matching the given parameters within a class.
     * 
     * @param methodClass Class to inspect
     * @param methodName name of the method to fins
     * @param params parameters that should invoke
     * @return method within the class with matching argumetns
     * @throws NoSuchMethodException Method not found in the class
     * @throws SecurityException method has wrong attributes and cannot be accessed publicly
     */
    public static Method findMethod(Class<?> methodClass, String methodName, List<? extends InvocationParam> params)
        throws NoSuchMethodException, SecurityException {

        try {
            Method method = null;
            try {
                Class<?>[] types = new Class<?>[params.size()];
                int paramIdx = 0;
                for (InvocationParam param : params) {
                    types[paramIdx++] = param.getValueClass();
                }
                method = methodClass.getMethod(methodName, types);
            } catch (NoSuchMethodException | SecurityException e) {
                for (Method m : methodClass.getDeclaredMethods()) {
                    if (m.getName().equals(methodName)) {
                        java.lang.reflect.Parameter[] reflectionParams = m.getParameters();
                        boolean arbitraryParamCount;
                        if (reflectionParams.length > 0) {
                            // If the method has an arbitrary number of parameters, the last parameter is an array.
                            arbitraryParamCount = reflectionParams[reflectionParams.length - 1].isVarArgs();
                        } else {
                            arbitraryParamCount = false;
                        }
                        if (arbitraryParamCount) {
                            // If it has an arbitrary number of parameters, the call may have more parameters or 1 less
                            if (params.size() < m.getParameterCount() - 1) {
                                continue;
                            }
                        } else {
                            if (params.size() != m.getParameterCount()) {
                                continue;
                            }
                        }

                        boolean isMatch = true;
                        for (int paramId = 0; paramId < reflectionParams.length && isMatch; paramId++) {
                            java.lang.reflect.Parameter reflectionParam = reflectionParams[paramId];
                            if (arbitraryParamCount && paramId == reflectionParams.length - 1) {
                                Class<?> componentType = reflectionParam.getType().getComponentType();
                                for (; paramId < params.size() && isMatch; paramId++) {
                                    Object paramValue = params.get(paramId).getValue();
                                    isMatch = areTypesMatching(componentType, paramValue);
                                }
                            } else {
                                Object paramValue = params.get(paramId).getValue();
                                isMatch = areTypesMatching(reflectionParam.getType(), paramValue);
                            }
                        }
                        if (isMatch) {
                            method = m;
                        }
                    }
                }
                if (method == null) {
                    throw e;
                }
            }
            return method;
        } catch (SecurityException e) {
            throw e;
        }
    }

    private static boolean areTypesMatching(Class<?> reflectionParam, Object paramValue) {
        boolean isMatch = true;
        if (reflectionParam.isPrimitive()) {
            if (reflectionParam != paramValue.getClass()) {
                switch (reflectionParam.getCanonicalName()) {
                    case "byte":
                        isMatch = paramValue.getClass().getCanonicalName().equals("java.lang.Byte");
                        break;
                    case "char":
                        isMatch = paramValue.getClass().getCanonicalName().equals("java.lang.Char");
                        break;
                    case "short":
                        isMatch = paramValue.getClass().getCanonicalName().equals("java.lang.Short");
                        break;
                    case "int":
                        isMatch = paramValue.getClass().getCanonicalName().equals("java.lang.Integer");
                        break;
                    case "long":
                        isMatch = paramValue.getClass().getCanonicalName().equals("java.lang.Long");
                        break;
                    case "float":
                        isMatch = paramValue.getClass().getCanonicalName().equals("java.lang.Float");
                        break;
                    case "double":
                        isMatch = paramValue.getClass().getCanonicalName().equals("java.lang.Double");
                        break;
                    case "boolean":
                        isMatch = paramValue.getClass().getCanonicalName().equals("java.lang.Boolean");
                        break;
                }
            }
        } else {
            try {
                reflectionParam.cast(paramValue);
            } catch (ClassCastException cce) {
                isMatch = false;
            }
        }
        return isMatch;
    }
}
