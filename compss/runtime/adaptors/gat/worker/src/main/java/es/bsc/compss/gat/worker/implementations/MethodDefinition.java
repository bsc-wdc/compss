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
package es.bsc.compss.gat.worker.implementations;

import es.bsc.compss.gat.worker.ImplementationDefinition;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.util.ErrorManager;
import java.io.File;
import java.lang.reflect.Method;


public class MethodDefinition implements ImplementationDefinition {

    private final String className;
    private final String methodName;

    public MethodDefinition(String className, String methodName) {
        this.className = className;
        this.methodName = methodName;
    }

    @Override
    public MethodType getType() {
        return MethodType.METHOD;
    }

    @Override
    public String toCommandString() {
        return className + " " + methodName;
    }

    @Override
    public String toLogString() {
        return "["
                + "DECLARING CLASS=" + className
                + ", METHOD NAME=" + methodName
                + "]";
    }

    @Override
    public Object process(Object target, Class<?> types[], Object values[], boolean[] areFiles, Stream[] streams, String[] prefixes, File sandBoxDir) {

        // Use reflection to get the requested method
        Method method = null;
        try {
            Class<?> methodClass = Class.forName(className);
            method = methodClass.getMethod(methodName, types);
        } catch (ClassNotFoundException e) {
            ErrorManager.error("Application class not found");
        } catch (SecurityException e) {
            ErrorManager.error("Security exception");
        } catch (NoSuchMethodException e) {
            ErrorManager.error("Requested method not found");
        }

        if (method == null) {
            ErrorManager.error("Requested method is null");
        }

        // Invoke the requested method
        Object retValue = null;
        try {
            retValue = method.invoke(target, values);
        } catch (Exception e) {
            ErrorManager.error(ERROR_INVOKE, e);
        }

        return retValue;
    }
}
