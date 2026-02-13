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
package es.bsc.compss.loader.total;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.loader.LoaderConstants;
import es.bsc.compss.loader.LoaderUtils;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public final class ITAppModifier {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Constants
    private static final String COMPSS_APP_CONSTANT = LoaderConstants.CLASS_COMPSS_CONSTANTS + ".APP_NAME";

    // Flag to indicate in class is WS
    private static final boolean IS_WS_CLASS = System.getProperty(COMPSsConstants.COMPSS_IS_WS) != null
        && System.getProperty(COMPSsConstants.COMPSS_IS_WS).equals("true");

    private static final long WALL_CLOCK_LIMIT =
        Long.parseLong(System.getProperty(COMPSsConstants.COMPSS_WALL_CLOCK_LIMIT, "0"));

    private static final Map<String, Map<String, byte[]>> CACHE = new ConcurrentHashMap<>();


    private ITAppModifier() {
    }

    /**
     * Load the modified class into memory and return it. Generally, once a class is loaded into memory no further
     * modifications can be performed on it.
     *
     * @param appName Application name
     * @param annotItf Annotated interface class
     * @param threadIdAsAppId If true, the method provides the current thread ID as the itAppIdVar for instrumentation,
     *            otherwise uses the same "compssAppId"
     * @param isMainClass Whether the calling class is the main application class
     * @return Instrumented class
     */
    public static Class<?> modifyToMemory(String appName, Class<?> annotItf, boolean threadIdAsAppId,
        boolean isMainClass) throws NotFoundException, CannotCompileException, ClassNotFoundException, IOException {
        String cacheKey = appName + "-itf-" + annotItf.getName();
        Map<String, byte[]> bytecodeMap = CACHE.get(cacheKey);
        if (bytecodeMap == null) {
            bytecodeMap = new HashMap<>();
            LOGGER.info("Instrumenting class " + appName + " according to interface " + annotItf.getName());
            CtClass appClass = modify(appName, annotItf, threadIdAsAppId, isMainClass);
            byte[] bytecode = appClass.toBytecode();
            bytecodeMap.put(appClass.getName(), bytecode);
            for (CtClass inner : appClass.getNestedClasses()) {
                bytecodeMap.put(inner.getName(), inner.toBytecode());
            }
            CACHE.put(cacheKey, bytecodeMap);
        } else {
            LOGGER.info("Using cached classes");
        }
        ClassLoader loader = new CustomClassLoader(bytecodeMap);
        Class<?> clazz = loader.loadClass(appName);
        Thread.currentThread().setContextClassLoader(loader);
        return clazz;
    }


    /**
     * Custom class loader for defining the instrumented version of the class.
     */
    private static class CustomClassLoader extends ClassLoader {

        private final Map<String, byte[]> bytecodeMap;


        public CustomClassLoader(Map<String, byte[]> bytecodeMap) {
            super(ITAppModifier.class.getClassLoader());
            this.bytecodeMap = bytecodeMap;
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            byte[] bytecode = bytecodeMap.get(name);
            if (bytecode != null) {
                // define from bytecode without delegating to parent
                return defineClass(name, bytecode, 0, bytecode.length);
            }
            return super.loadClass(name);
        }
    }


    /**
     * Write the modified class to disk.
     *
     * @param appName Application name
     * @param annotItf Annotated interface class
     * @param threadIdAsAppId If true, the method provides the current thread ID as the itAppIdVar for instrumentation,
     *            otherwise uses the same "compssAppId"
     * @param isMainClass Whether the calling class is the main application class
     */
    public static void modifyToFile(String appName, Class<?> annotItf, boolean threadIdAsAppId, boolean isMainClass)
        throws NotFoundException, CannotCompileException, ClassNotFoundException {
        CtClass appClass = modify(appName, annotItf, threadIdAsAppId, isMainClass);
        try {
            appClass.writeFile();
        } catch (Exception e) {
            ErrorManager.fatal("Error writing the instrumented class file");
        }
    }

    /**
     * Modify method.
     */
    private static CtClass modify(String appName, Class<?> annotItf, boolean perThreadWf, boolean isMainClass)
        throws NotFoundException, CannotCompileException, ClassNotFoundException {
        // Use the application editor to include the COMPSs API calls on the application code
        ClassPool classPool = getClassPool();
        CtClass appClass = classPool.get(appName);
        appClass.defrost();

        HashMap<String, CtField> getters = new HashMap<>();

        String varName = "COMPSs_";

        String itApiVar = varName + LoaderConstants.STR_COMPSS_API;
        CtField apiField = buildField(classPool, appClass, LoaderConstants.CLASS_COMPSSRUNTIME_API, itApiVar);
        appClass.addField(apiField);
        getters.put("getRuntime", apiField);

        String itSRVar = varName + LoaderConstants.STR_COMPSS_STREAM_REGISTRY;
        CtField srField = buildField(classPool, appClass, LoaderConstants.CLASS_STREAM_REGISTRY, itSRVar);
        appClass.addField(srField);
        getters.put("getStreamRegistry", srField);

        String itWfVar = varName + LoaderConstants.STR_COMPSS_WORKFLOW;
        String instAppId;
        String instWf;
        String setupWkSupplySrc;
        if (perThreadWf) {
            CtField field = buildField(classPool, appClass, LoaderConstants.CLASS_WORKFLOW_SUPPLIER, itWfVar);
            String fieldInitSrc = "new " + LoaderConstants.CLASS_WORKFLOW_SUPPLIER + "();";
            appClass.addField(field, CtField.Initializer.byExpr(fieldInitSrc));
            instWf = "((" + LoaderConstants.CLASS_WORKFLOW + ")" + itWfVar + ".get())";
            setupWkSupplySrc = LoaderConstants.CLASS_WORKFLOW_SUPPLIER + ".setRuntime(" + itApiVar + ");";
        } else {
            appClass.addField(buildField(classPool, appClass, LoaderConstants.CLASS_WORKFLOW, itWfVar));
            instWf = itWfVar;
            setupWkSupplySrc = ""; // No workflow supply exists. Do nothing
        }
        instAppId = instWf + ".getId()";

        // Instrument class
        instrumentClass(classPool, appClass, annotItf, itApiVar, itSRVar, instWf, instAppId, isMainClass);

        addGetters(appClass, getters);
        StringBuilder methodBody = new StringBuilder();
        methodBody.append("public static ").append(LoaderConstants.CLASS_WORKFLOW).append(" getWorkflow() {");
        methodBody.append("    return ").append(instWf).append(";");
        methodBody.append("}");
        CtMethod m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);

        methodBody = new StringBuilder();
        methodBody.append("private static void setupWorkflowSupplier() {");
        methodBody.append("    ").append(setupWkSupplySrc);
        methodBody.append("}");
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);

        addModifyVariablesMethods(appClass, itApiVar, itSRVar, itWfVar, perThreadWf, isMainClass);
        return appClass;
    }

    private static void addGetters(CtClass appClass, HashMap<String, CtField> getters) throws CannotCompileException {
        for (Map.Entry<String, CtField> entry : getters.entrySet()) {
            CtMethod m = CtNewMethod.getter(entry.getKey(), entry.getValue());
            appClass.addMethod(m);
        }
    }

    /**
     * Create new ClassPool object and load packages into it.
     */
    private static ClassPool getClassPool() {
        ClassPool cp = new ClassPool(true);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_ROOT);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_API);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_API_IMPL);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_LOADER);
        cp.importPackage(LoaderConstants.PACKAGE_COMPSS_LOADER_TOTAL);
        return cp;
    }

    private static CtField buildField(ClassPool cp, CtClass appClass, String fieldClassName, String fieldName)
        throws NotFoundException, CannotCompileException {
        CtClass fieldClass = cp.get(fieldClassName);
        CtField field = new CtField(fieldClass, fieldName, appClass);
        field.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        return field;
    }

    /*
     * Create a Code Converter object and instrument each method based on whether it is the main method, an
     * orchestration method, or a web service method.
     */
    private static void instrumentClass(ClassPool cp, CtClass appClass, Class<?> annotItf, String itApiVar,
        String itSRVar, String itWfVar, String itAppIdVar, boolean isMainClass)
        throws NotFoundException, CannotCompileException {
        // Methods declared in the annotated interface
        Method[] remoteMethods = annotItf.getMethods();

        /*
         * Create Code Converter
         */
        CodeConverter converter = new CodeConverter();
        CtClass arrayWatcher = cp.get(LoaderConstants.CLASS_ARRAY_ACCESS_WATCHER);
        CodeConverter.DefaultArrayAccessReplacementMethodNames names =
            new CodeConverter.DefaultArrayAccessReplacementMethodNames();
        converter.replaceArrayAccess(arrayWatcher, (CodeConverter.ArrayAccessReplacementMethodNames) names);

        /*
         * Find the methods declared in the application class that will be instrumented - Main - Constructors - Methods
         * that are not in the remote list
         */
        if (DEBUG) {
            LOGGER.debug("Flags: isWS: " + IS_WS_CLASS + " isMainClass: " + isMainClass);
        }
        // Candidates to be instrumented if they are not remote
        CtMethod[] instrCandidates = appClass.getDeclaredMethods();

        ITAppEditor itAppEditor =
            new ITAppEditor(remoteMethods, instrCandidates, itApiVar, itSRVar, itWfVar, itAppIdVar, appClass);

        for (CtMethod m : instrCandidates) {
            if (DEBUG) {
                LOGGER.debug("Instrumenting method " + m.getName());
            }
            StringBuilder toInsertAfter = new StringBuilder();

            boolean isMainMethod = LoaderUtils.isMainMethod(m);
            boolean isOrchestration = LoaderUtils.isOrchestration(m);

            if ((isMainMethod && isMainClass) || (isOrchestration && IS_WS_CLASS)) {
                LOGGER.debug("Inserting call noMoreTasks at the end of main");
                toInsertAfter.insert(0, itWfVar + ".noMoreTasks();");

                // Do insertions
                if (IS_WS_CLASS) {
                    m.insertAfter(toInsertAfter.toString()); // executed only if Orchestration finishes properly
                } else { // Main program
                    m.insertAfter(toInsertAfter.toString(), true); // no matter what
                }
            } else {
                if (IS_WS_CLASS) {
                    // If we're instrumenting a service class, only instrument private methods, public might be
                    // non-OE operations
                    if (!Modifier.isPrivate(m.getModifiers())) {
                        continue;
                    }
                }
            }

            /*
             * Instrumenting first the array accesses makes each array access become a call to a black box method of
             * class ArrayAccessWatcher, whose parameters include the array. For the second round of instrumentation,
             * the synchronization by transition to black box automatically synchronizes the arrays accessed. TODO:
             * Change the order of instrumentation, so that we have more control about the synchronization, and we can
             * distinguish between a write access and a read access (now it's read/write access by default, because it
             * goes into the black box).
             */
            m.instrument(converter);
            m.instrument(itAppEditor);
        }

        // Instrument constructors
        for (CtConstructor c : appClass.getDeclaredConstructors()) {
            if (DEBUG) {
                LOGGER.debug("Instrumenting constructor " + c.getLongName());
            }
            c.instrument(converter);
            c.instrument(itAppEditor);
        }
    }

    private static void addModifyVariablesMethods(CtClass appClass, String itApiVar, String itSRVar, String itWfVar,
        boolean perThreadWf, boolean isMainClass) throws CannotCompileException {

        String getWf;
        if (perThreadWf) {
            getWf = "((" + LoaderConstants.CLASS_WORKFLOW + ")" + itWfVar + ".get())";
        } else {
            getWf = itWfVar;
        }

        /*
         * Insert printer method
         */
        StringBuilder methodBody = new StringBuilder();
        methodBody.append("public static void printCOMPSsVariables() { ");
        methodBody.append("System.out.println(\"Api Var: \" + ").append(itApiVar).append(");");
        methodBody.append("System.out.println(\"SR Var: \" + ").append(itSRVar).append(");");
        methodBody.append("System.out.println(\"App Id: \" + ").append(getWf).append(".getId());");
        methodBody.append("}");
        CtMethod m;
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);

        /*
         * Insert method to retrieve the runtime instead of instantiating a new one
         */
        methodBody = new StringBuilder();
        methodBody.append("public static void setCOMPSsVariables( ") //
            .append(LoaderConstants.CLASS_COMPSSRUNTIME_API).append(" runtime, ")//
            .append(LoaderConstants.CLASS_LOADERAPI).append(" loader, ")//
            .append(LoaderConstants.CLASS_WORKFLOW).append(" wf") //
            .append(") {") //
            .append(itApiVar).append("= runtime;") //
            .append("setupWorkflowSupplier();") //
            .append(itSRVar).append("= loader.getStreamRegistry();"); //
        String wfSetInstr;
        if (perThreadWf) {
            wfSetInstr = itWfVar + ".set(wf)";
        } else {
            wfSetInstr = itWfVar + " = wf";
        }
        methodBody.append(wfSetInstr).append(";");
        methodBody.append("}");
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);

        /*
         * Insert method to retrieve the runtime instead of instantiating a new one
         */
        methodBody = new StringBuilder();
        methodBody.append("public static void setCOMPSsVariables( ") //
            .append(LoaderConstants.CLASS_COMPSSRUNTIME_API).append(" runtime, ") //
            .append(LoaderConstants.CLASS_LOADERAPI).append(" loader") //
            .append(") {") //
            .append(itApiVar).append("= runtime;") //
            .append("setupWorkflowSupplier();") //
            .append(itSRVar).append(" = new ").append(LoaderConstants.CLASS_STREAM_REGISTRY).append("(loader);");//

        if (WALL_CLOCK_LIMIT > 0) {
            // Setting wall clock limit with runtime stop.
            methodBody.append(itApiVar).append(".setWallClockLimit(").append(getWf).append(".getId(),")
                .append(WALL_CLOCK_LIMIT).append("L, true);");
        }
        methodBody.append("}");
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);
    }
}
