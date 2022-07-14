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

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.invokers.util.ClassUtils;
import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.loader.LoaderConstants;
import es.bsc.compss.loader.LoaderUtils;
import es.bsc.compss.loader.total.ITAppEditor;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.util.parsers.ITFParser;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CodeConverter;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.Modifier;
import javassist.NotFoundException;


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
     * @param taskSandboxWorkingDir Task execution sandbox directory
     * @param assignedResources Assigned resources
     * @throws JobExecutionException Error creating the Java invoker
     */
    public JavaNestedInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
        InvocationResources assignedResources) throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources);
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
            try {
                // Add the jars that the custom class loader needs
                ClassLoader myLoader = new URLClassLoader(new URL[] { new URL(ENGINE_PATH) });

                Thread.currentThread().setContextClassLoader(myLoader);

                ClassPool classPool = getClassPool();
                CtClass appClass = classPool.get(className);
                appClass.defrost();

                String varName = LoaderUtils.randomName(5, LoaderConstants.STR_COMPSS_PREFIX);
                String itApiVar = varName + LoaderConstants.STR_COMPSS_API;
                String itSRVar = varName + LoaderConstants.STR_COMPSS_STREAM_REGISTRY;
                String itORVar = varName + LoaderConstants.STR_COMPSS_OBJECT_REGISTRY;
                String itAppIdVar = varName + LoaderConstants.STR_COMPSS_APP_ID;

                addVariables(classPool, appClass, itApiVar, itSRVar, itORVar, itAppIdVar);
                instrumentClass(classPool, appClass, ceiClass, itApiVar, itSRVar, itORVar, itAppIdVar);
                addModifyVariablesMethod(appClass, itApiVar, itSRVar, itORVar, itAppIdVar);

                methodClass = appClass.toClass();
                appClass.defrost();
                method = ClassUtils.findMethod(methodClass, methodName, this.invocation.getParams());
            } catch (Exception e) {
                LOGGER.warn("Could not instrument the method to detect nested tasks.", e);
                method = super.findMethod();
            }
        }
        return method;
    }

    private static ClassPool getClassPool() {
        ClassPool cp = new ClassPool();
        cp.appendSystemPath();
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_ROOT);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_API);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_API_IMPL);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_LOADER);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_LOADER_TOTAL);
        return cp;
    }

    private static void addVariables(ClassPool cp, CtClass appClass, String itApiVar, String itSRVar, String itORVar,
        String itAppIdVar) throws NotFoundException, CannotCompileException {
        CtClass itApiClass = cp.get(LoaderConstants.CLASS_COMPSSRUNTIME_API);
        CtField itApiField = new CtField(itApiClass, itApiVar, appClass);
        itApiField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        appClass.addField(itApiField);

        CtClass itSRClass = cp.get(LoaderConstants.CLASS_STREAM_REGISTRY);
        CtField itSRField = new CtField(itSRClass, itSRVar, appClass);
        itSRField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        appClass.addField(itSRField);

        CtClass itORClass = cp.get(LoaderConstants.CLASS_OBJECT_REGISTRY);
        CtField itORField = new CtField(itORClass, itORVar, appClass);
        itORField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        appClass.addField(itORField);

        CtClass appIdClass = cp.get(LoaderConstants.CLASS_APP_ID);
        CtField appIdField = new CtField(appIdClass, itAppIdVar, appClass);
        appIdField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        appClass.addField(appIdField);
    }

    private static void instrumentClass(ClassPool cp, CtClass appClass, Class<?> annotItf, String itApiVar,
        String itSRVar, String itORVar, String itAppIdVar)
        throws ClassNotFoundException, NotFoundException, CannotCompileException {
        Method[] remoteMethods = annotItf.getMethods();

        CtMethod[] instrCandidates = appClass.getDeclaredMethods();
        // Candidates to be instrumented if they are not remote
        ITAppEditor itAppEditor;
        itAppEditor = new ITAppEditor(remoteMethods, instrCandidates, itApiVar, itSRVar, itORVar, itAppIdVar, appClass);

        /*
         * Create Code Converter
         */
        CodeConverter converter = new CodeConverter();
        CtClass arrayWatcher = cp.get(LoaderConstants.CLASS_ARRAY_ACCESS_WATCHER);
        CodeConverter.DefaultArrayAccessReplacementMethodNames names;
        names = new CodeConverter.DefaultArrayAccessReplacementMethodNames();
        converter.replaceArrayAccess(arrayWatcher, (CodeConverter.ArrayAccessReplacementMethodNames) names);

        /*
         * Find the methods declared in the application class that will be instrumented - Main - Constructors - Methods
         * that are not in the remote list
         */
        for (CtMethod m : instrCandidates) {
            m.instrument(converter);
            m.instrument(itAppEditor);
        }

        // Instrument constructors
        for (CtConstructor c : appClass.getDeclaredConstructors()) {
            c.instrument(converter);
            c.instrument(itAppEditor);
        }
    }

    private static void addModifyVariablesMethod(CtClass appClass, String itApiVar, String itSRVar, String itORVar,
        String itAppIdVar) throws CannotCompileException, NotFoundException {

        StringBuilder methodBody = new StringBuilder();
        methodBody.append("public static void printCOMPSsVariables() { ");
        methodBody.append("System.out.println(\"Api Var: \" + ").append(itApiVar).append(");");
        methodBody.append("System.out.println(\"SR Var: \" + ").append(itSRVar).append(");");
        methodBody.append("System.out.println(\"OR Var: \" + ").append(itORVar).append(");");
        methodBody.append("System.out.println(\"App Id: \" + ").append(itAppIdVar).append(");");
        methodBody.append("}");
        CtMethod m;
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);

        methodBody = new StringBuilder();
        methodBody.append("public static void setCOMPSsVariables( ").append(LoaderConstants.CLASS_COMPSSRUNTIME_API)
            .append(" runtime" + ", ").append(LoaderConstants.CLASS_LOADERAPI).append(" loader" + ", ")
            .append(LoaderConstants.CLASS_APP_ID).append(" appId" + ") {");
        methodBody.append(itApiVar).append("= runtime;");
        methodBody.append(itSRVar).append("= loader.getStreamRegistry();");
        methodBody.append(itORVar).append("= loader.getObjectRegistry();");
        methodBody.append(itAppIdVar).append("= appId;");
        methodBody.append("}");
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);
    }

    @Override
    protected Object runMethod() throws JobExecutionException {
        Object returnValue;
        if (this.ceiClass == null) {
            returnValue = super.runMethod();
        } else {
            long appId;
            appId = this.runtimeAPI.registerApplication(this.ceiName, this);
            LOGGER.info("Job " + this.invocation.getJobId() + " becomes app " + appId);
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
                returnValue = super.runMethod();
                if (returnValue != null) {
                    Object internal = context.getLoaderAPI().getObjectRegistry().getInternalObject(appId, returnValue);
                    if (internal != null) {
                        returnValue = internal;
                    }
                }
                for (InvocationParam p : this.invocation.getParams()) {
                    if (p.isWriteFinalValue()) {
                        getLastValue(appId, p);
                    }
                }
                for (InvocationParam p : this.invocation.getResults()) {
                    getLastValue(appId, p);
                }
                this.runtimeAPI.noMoreTasks(appId);
                return returnValue;

            } catch (Throwable e) {
                throw new JobExecutionException("Error executing the instrumented method!", e);
            } finally {
                // runtimeAPI.removeApplicationData(appId);
                this.runtimeAPI.deregisterApplication(appId);
            }
        }
        return returnValue;
    }

    private void getLastValue(Long appId, InvocationParam p) {
        switch (p.getType()) {
            case OBJECT_T:
            case PSCO_T:
                ObjectRegistry or = this.context.getLoaderAPI().getObjectRegistry();
                Object internal = or.collectObjectLastValue(appId, p.getValue());
                p.setValue(internal);
                break;
            case FILE_T:
                this.context.getRuntimeAPI().getFile(appId, p.getOriginalName());
                new File((String) p.getValue());
                new File((String) p.getOriginalName());
                break;
            default:
                // Do nothing
        }
    }
}
