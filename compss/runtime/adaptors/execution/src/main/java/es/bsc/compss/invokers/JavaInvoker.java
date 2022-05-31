/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.invokers.util.ClassUtils;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.worker.CanceledTask;

import java.io.File;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;

import storage.StubItf;


public class JavaInvoker extends Invoker {

    public static final String ERROR_CLASS_REFLECTION = "Cannot get class by reflection";
    public static final String ERROR_METHOD_REFLECTION = "Cannot get method by reflection";

    protected final String className;
    protected Class<?> methodClass;
    protected final String methodName;
    protected Method method;


    /**
     * Java Invoker constructor.
     *
     * @param context Task execution context
     * @param invocation Task execution description
     * @param taskSandboxWorkingDir Task execution sandbox directory
     * @param assignedResources Assigned resources
     * @throws JobExecutionException Error creating the Java invoker
     */
    public JavaInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources);

        // Get method class and name
        switch (invocation.getMethodImplementation().getMethodType()) {
            case METHOD:
                MethodDefinition methodImpl = (MethodDefinition) invocation.getMethodImplementation().getDefinition();
                this.className = methodImpl.getDeclaringClass();
                this.methodName = methodImpl.getAlternativeMethodName();
                break;
            case MULTI_NODE:
                MultiNodeDefinition multiNodeImpl =
                    (MultiNodeDefinition) invocation.getMethodImplementation().getDefinition();
                this.className = multiNodeImpl.getDeclaringClass();
                this.methodName = multiNodeImpl.getMethodName();
                break;
            default:
                // We have received an incorrect implementation type
                throw new JobExecutionException(
                    ERROR_METHOD_DEFINITION + invocation.getMethodImplementation().getMethodType());
        }

        // Use reflection to get the requested method
        this.method = findMethod();
    }

    protected Method findMethod() throws JobExecutionException {
        try {
            methodClass = Class.forName(this.className);
        } catch (ClassNotFoundException e) {
            throw new JobExecutionException(ERROR_CLASS_REFLECTION, e);
        }
        try {
            return ClassUtils.findMethod(methodClass, methodName, this.invocation.getParams());
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
            if (retValue != null) {
                np.setValueClass(retValue.getClass());
            } else {
                np.setValueClass(null);
            }
            checkSCOPersistence(np);
        }
    }

    protected Object runMethod() throws JobExecutionException {
        List<? extends InvocationParam> params = this.invocation.getParams();
        int paramCount = this.method.getParameterCount();
        Object[] values = new Object[paramCount];

        Object[] paramDest = values;
        int paramIdx = 0;
        for (InvocationParam param : params) {
            if (paramIdx != paramCount - 1) {
                paramDest[paramIdx++] = param.getValue();
            } else {
                Parameter reflectionParam = this.method.getParameters()[paramIdx];
                Class<?> paramClass = this.method.getParameters()[paramIdx].getType();
                // If the method has an arbitrary number of parameters, the last parameter is an array.
                if (reflectionParam.isVarArgs()) {
                    int varArgsCount = params.size() - paramCount + 1;
                    paramDest[paramIdx] = Array.newInstance(paramClass.getComponentType(), varArgsCount);
                    paramDest = (Object[]) paramDest[paramIdx];
                    paramIdx = 0;
                    paramDest[paramIdx++] = param.getValue();
                } else {
                    paramDest[paramIdx++] = param.getValue();
                }
            }
        }

        InvocationParam targetParam = this.invocation.getTarget();
        Object target = null;
        if (targetParam != null) {
            target = targetParam.getValue();
        }

        Object retValue = null;
        try {
            LOGGER.info("Invoked " + this.method.getName() + (target == null ? "" : " on object " + target) + " in "
                + this.context.getHostName());
            retValue = this.method.invoke(target, values);
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

    @Override
    public void cancelMethod() {
        CanceledTask t = new CanceledTask(this.invocation.getTaskId());
        t.cancel();
    }
}
