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
package es.bsc.compss.invokers;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.invokers.util.ClassUtils;
import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.loader.LoaderConstants;
import es.bsc.compss.loader.total.ITAppModifier;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.util.parsers.ITFParser;
import es.bsc.compss.worker.COMPSsException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;


public class JavaNestedInvoker extends JavaInvoker {

    private static final String ENGINE_PATH;

    static {
        String compssHome = System.getenv(COMPSsConstants.COMPSS_HOME);
        ENGINE_PATH = "file:" + compssHome + LoaderConstants.ENGINE_JAR_WITH_REL_PATH;
    }

    private String ceiName;
    private Class<?> ceiClass;
    private final COMPSsRuntime runtimeAPI;
    private final LoaderAPI loaderAPI;


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
        loaderAPI = context.getLoaderAPI();
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
                Tracer.emitEvent(TraceEvent.INSTRUMENTING_CLASS);
            }
            try {
                // Add the jars that the custom class loader needs
                ClassLoader myLoader = new URLClassLoader(new URL[] { new URL(ENGINE_PATH) });

                Thread.currentThread().setContextClassLoader(myLoader);

                // Call class modifier
                LOGGER.debug("Modifying application " + className);
                methodClass = ITAppModifier.modifyToMemory(className, className, ceiClass, false, true, true, false);

                // Find the corresponding method
                method = ClassUtils.findMethod(methodClass, methodName, this.invocation.getParams());
            } catch (Exception e) {
                LOGGER.warn("Could not instrument the method to detect nested tasks.", e);
                method = super.findMethod();
            } finally {

                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(TraceEvent.INSTRUMENTING_CLASS);
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
            long appId;
            appId = becomesNestedApplication(this.ceiName);
            // Register Core Elements on Runtime
            List<CoreElementDefinition> ceds = ITFParser.parseITFMethods(this.ceiClass);
            for (CoreElementDefinition ced : ceds) {
                this.runtimeAPI.registerCoreElement(ced);
            }
            Method setter;
            try {
                setter = this.methodClass.getDeclaredMethod("setCOMPSsVariables",
                    new Class<?>[] { Class.forName(LoaderConstants.CLASS_COMPSSRUNTIME_API),
                        Class.forName(LoaderConstants.CLASS_LOADERAPI),
                        Class.forName(LoaderConstants.CLASS_APP_ID) });
            } catch (Exception e) {
                throw new JobExecutionException("Class not properly instrumented. Method setCOMPSsVariables not found!",
                    e);
            }
            try {
                Object[] values = new Object[] { this.runtimeAPI,
                    this.loaderAPI,
                    appId };
                setter.invoke(null, values);
            } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                throw new JobExecutionException("Error setting Nested COMPSs variables", e);
            }
            try {
                super.runMethod();
            } catch (Throwable e) {
                throw new JobExecutionException("Error executing the instrumented method!", e);
            } finally {
                this.completeNestedApplication(appId);
            }
        }
    }
}
