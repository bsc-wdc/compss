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
package es.bsc.compss.invokers;

import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.invokers.util.ClassUtils;
import es.bsc.compss.loader.LoaderConstants;
import es.bsc.compss.loader.editing.ITAppModifier;
import es.bsc.compss.loader.workflow.JavaWorkflow;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.tracing.TaskExecutionEvent;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class JavaNestedInvoker extends JavaInvoker {

    private String ceiName;
    private Class<?> ceiClass;
    private final COMPSsRuntime runtimeAPI;


    /**
     * Nested Java Invoker constructor.
     *
     * @param context Task execution context
     * @param invocation Task execution description
     * @param sandbox Task execution sandbox directory
     * @param assignedResources Assigned resources
     * @throws JobExecutionException Error creating the Java invoker
     */
    public JavaNestedInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandbox,
        InvocationResources assignedResources) throws JobExecutionException {
        super(context, invocation, sandbox, assignedResources);
        runtimeAPI = context.getRuntimeAPI();
    }

    @Override
    protected Method findMethod() throws JobExecutionException {
        ceiName = invocation.getParallelismSource();
        Class<?> ceiClass;
        if (ceiName != null) {
            try {
                ceiClass = Class.forName(ceiName);
            } catch (ClassNotFoundException ex) {
                LOGGER.warn("Requesting a Nested Invoker with not found CEI " + ceiName + " for Job "
                    + invocation.getJobId() + ". Proxying invoker to a regular Java Invoker.");
                ceiClass = null;
            }
        } else {
            LOGGER.warn("Requesting a Nested Invoker with no CEI for Job " + invocation.getJobId()
                + ". Proxying invoker to a regular Java Invoker.");
            ceiClass = null;
        }
        this.ceiClass = ceiClass;
        Method method;
        if (ceiClass == null) {
            method = super.findMethod();
        } else {
            if (Tracer.isActivated()) {
                Tracer.emitEvent(TaskExecutionEvent.INSTRUMENTING_CLASS);
            }
            try {
                // Call class modifier
                LOGGER.debug("Modifying application " + className);
                methodClass = ITAppModifier.modifyToMemory(className, ceiClass, false, false);
                // Find the corresponding method
                method = ClassUtils.findMethod(methodClass, methodName, this.invocation.getParams());
            } catch (Throwable e) {
                LOGGER.warn("Could not instrument the method to detect nested tasks.", e);
                throw new JobExecutionException("Could not instrument the method to detect nested tasks.", e);
            } finally {

                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(TaskExecutionEvent.INSTRUMENTING_CLASS);
                }
            }
        }
        return method;
    }

    @Override
    protected void runMethod() throws JobExecutionException, COMPSsException {
        if (this.ceiClass == null) {
            super.runMethod();
        } else {
            JavaWorkflow wf = becomesNestedApplication(this.ceiName);
            try {
                super.runMethod();
            } catch (Throwable e) {
                throw new JobExecutionException("Error executing the instrumented method!", e);
            } finally {
                this.completeNestedApplication(wf);
            }
        }
    }

    @Override
    public JavaWorkflow registerWorkflow(String parallelismSource) throws JobExecutionException {
        Method setter;
        JavaWorkflow wf = null;
        try {
            setter = this.methodClass.getDeclaredMethod("setupCOMPSs",
                new Class<?>[] { Class.forName(LoaderConstants.CLASS_COMPSSRUNTIME_API),
                    Class.forName(LoaderConstants.CLASS_APP_RUNNER) });
        } catch (Exception e) {
            throw new JobExecutionException("Class not properly instrumented. Method setCOMPSsVariables not found!", e);
        }
        try {
            Object[] values = new Object[] { this.runtimeAPI,
                this };
            wf = (JavaWorkflow) setter.invoke(null, values);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new JobExecutionException("Error setting Nested COMPSs variables", e);
        }

        return wf;
    }

    @Override
    protected void handleSimpleInputValue(JavaWorkflow wf, InvocationParam p) {
        switch (p.getType()) {
            case OBJECT_T:
            case PSCO_T:
                Object o = p.getValue();
                wf.registerData(p.getType().toByte(), o, p.getSourceDataId());
                break;
            default:
                super.handleSimpleInputValue(wf, p);
        }
    }

    @Override
    protected void handleSimpleOutputValue(JavaWorkflow wf, InvocationParam p) {
        switch (p.getType()) {
            case OBJECT_T:
            case PSCO_T: {
                Object o = p.getValue();
                String dataId = p.getDataMgmtId();
                if (!wf.bindExistingVersionToData(o, dataId)) {
                    Object internal = wf.getObject(p.getValue(), false);
                    p.setValue(internal);
                } else {
                    p.resultIsForwarded();
                }
            }
                break;
            default:
                super.handleSimpleOutputValue(wf, p);
        }
    }
}
