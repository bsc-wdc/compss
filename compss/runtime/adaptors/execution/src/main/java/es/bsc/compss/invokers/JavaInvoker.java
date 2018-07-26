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
package es.bsc.compss.invokers;

import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.implementations.MethodImplementation;

import java.io.File;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;
import java.util.List;
import storage.StubItf;


public class JavaInvoker extends Invoker {

    public static final String ERROR_CLASS_REFLECTION = "Cannot get class by reflection";
    public static final String ERROR_METHOD_REFLECTION = "Cannot get method by reflection";

    private final String className;
    private final String methodName;
    protected final Method method;

    public JavaInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir, InvocationResources assignedResources)
            throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        // Get method definition properties
        MethodImplementation methodImpl = null;
        try {
            methodImpl = (MethodImplementation) invocation.getMethodImplementation();
        } catch (Exception e) {
            throw new JobExecutionException(ERROR_METHOD_DEFINITION + invocation.getMethodImplementation().getMethodType(), e);
        }
        this.className = methodImpl.getDeclaringClass();
        this.methodName = methodImpl.getAlternativeMethodName();

        // Use reflection to get the requested method
        this.method = getMethod();
    }

    private Method getMethod() throws JobExecutionException {
        Class<?> methodClass = null;
        try {
            methodClass = Class.forName(this.className);
        } catch (ClassNotFoundException e) {
            throw new JobExecutionException(ERROR_CLASS_REFLECTION, e);
        }
        try {
            List<? extends InvocationParam> params = invocation.getParams();
            Class<?>[] types = new Class<?>[params.size()];
            int paramIdx = 0;
            for (InvocationParam param : params) {
                types[paramIdx++] = param.getValueClass();
            }
            return methodClass.getMethod(this.methodName, types);
        } catch (NoSuchMethodException | SecurityException e) {
            throw new JobExecutionException(ERROR_METHOD_REFLECTION, e);
        }
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        Object retValue = runMethod();

        for (InvocationParam np : this.invocation.getParams()) {
            checkSCOPersistence(np);
        }
        if (this.invocation.getTarget() != null) {
            checkSCOPersistence(this.invocation.getTarget());
        }
        for (InvocationParam np : this.invocation.getResults()) {
            np.setValue(retValue);
            np.setValueClass(retValue.getClass());
            checkSCOPersistence(np);
        }
    }

    protected Object runMethod() throws JobExecutionException {
        List<? extends InvocationParam> params = invocation.getParams();
        Object[] values = new Object[params.size()];
        int paramIdx = 0;
        for (InvocationParam param : params) {
            values[paramIdx++] = param.getValue();
        }

        InvocationParam targetParam = invocation.getTarget();
        Object target = null;
        if (targetParam != null) {
            target = targetParam.getValue();
        }

        Object retValue = null;
        try {
            LOGGER.info("Invoked " + method.getName() + (target == null ? "" : " on object " + target) + " in " + context.getHostName());
            retValue = method.invoke(target, values);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new JobExecutionException(ERROR_TASK_EXECUTION, e);
        }
        return retValue;
    }

    private void checkSCOPersistence(InvocationParam np) {
        boolean potentialPSCO = (np.getType().equals(DataType.OBJECT_T)) || (np.getType().equals(DataType.PSCO_T));
        if (np.isWriteFinalValue() && potentialPSCO) {
            Object obj = np.getValue();

            // Check if it is a PSCO and has been persisted in task
            String id = null;
            try {
                StubItf psco = (StubItf) obj;
                id = psco.getID();
            } catch (Exception e) {
                // No need to raise an exception because normal objects are not PSCOs
                id = null;
            }

            // Update to PSCO if needed
            if (id != null) {
                // Object has been persisted, we store the PSCO and change the value to its ID
                np.setType(DataType.PSCO_T);
            }
        }
    }
}
