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
package es.bsc.compss.loader.total;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.loader.LoaderConstants;
import es.bsc.compss.loader.LoaderUtils;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;

import java.lang.reflect.Method;
import java.util.Arrays;

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
        && System.getProperty(COMPSsConstants.COMPSS_IS_WS).equals("true") ? true : false;

    private static final long WALL_CLOCK_LIMIT =
        Long.parseLong(System.getProperty(COMPSsConstants.COMPSS_WALL_CLOCK_LIMIT, "0"));


    private ITAppModifier() {
    }

    /**
     * Modify method.
     */
    private static CtClass modify(String appName, String originalClassName, Class<?> annotItf, boolean threadIdAsAppId,
        boolean useNewAppClassName, boolean isMainClass)
        throws NotFoundException, CannotCompileException, ClassNotFoundException {
        // Use the application editor to include the COMPSs API calls on the application code
        ClassPool classPool = getClassPool();
        CtClass appClass = classPool.get(appName);
        appClass.defrost();
        String varName = LoaderUtils.randomName(5, LoaderConstants.STR_COMPSS_PREFIX);
        if (useNewAppClassName) {
            appClass.setName(appName + "_" + varName);
        }
        String itApiVar = varName + LoaderConstants.STR_COMPSS_API;
        String itSRVar = varName + LoaderConstants.STR_COMPSS_STREAM_REGISTRY;
        String itORVar = varName + LoaderConstants.STR_COMPSS_OBJECT_REGISTRY;
        String itAppIdVar = varName + LoaderConstants.STR_COMPSS_APP_ID;

        // Use thread ID for instrumentation
        String instrumentationAppId;
        if (threadIdAsAppId) {
            instrumentationAppId = "new Long(Thread.currentThread().getId())";
        } else {
            instrumentationAppId = itAppIdVar;
        }

        // Instrument class
        addVariables(classPool, appClass, itApiVar, itSRVar, itORVar, itAppIdVar);
        instrumentClass(classPool, appClass, annotItf, itApiVar, itSRVar, itORVar, instrumentationAppId,
            originalClassName, isMainClass);
        addModifyVariablesMethods(appClass, itApiVar, itSRVar, itORVar, itAppIdVar, instrumentationAppId, isMainClass);

        return appClass;
    }

    /**
     * Load the modified class into memory and return it. Generally, once a class is loaded into memory no further
     * modifications can be performed on it.
     *
     * @param appName Application name
     * @param originalClassName Original class name
     * @param annotItf Annotated interface class
     * @param threadIdAsAppId If true, the method provides the current thread ID as the itAppIdVar for instrumentation,
     *            otherwise uses the same "compssXXXXXAppId"
     * @param useNewAppClassName Use a different name for appClass with additional numbers appended to appName (used by
     *            nested)
     * @param returnOrigClass Whether to return or not the original class (used by nested)
     * @param isMainClass Whether the calling class is the main application class
     * @return Instrumented class
     */
    public static Class<?> modifyToMemory(String appName, String originalClassName, Class<?> annotItf,
        boolean threadIdAsAppId, boolean useNewAppClassName, boolean returnOrigClass, boolean isMainClass)
        throws NotFoundException, CannotCompileException, ClassNotFoundException {
        CtClass appClass =
            modify(appName, originalClassName, annotItf, threadIdAsAppId, useNewAppClassName, isMainClass);

        // Return original method class
        if (returnOrigClass) {
            Class<?> origClass = Class.forName(appName);
            Class<?> methodClass = appClass.toClass(origClass);
            appClass.defrost();
            return methodClass;
        } else {
            return appClass.toClass(annotItf);
        }
    }

    /**
     * Write the modified class to disk.
     *
     * @param appName Application name
     * @param originalClassName Original class name
     * @param annotItf Annotated interface class
     * @param threadIdAsAppId If true, the method provides the current thread ID as the itAppIdVar for instrumentation,
     *            otherwise uses the same "compssXXXXXAppId"
     * @param useNewAppClassName Use a different name for appClass with additional numbers appended to appName (used by
     *            nested)
     * @param isMainClass Whether the calling class is the main application class
     */
    public static void modifyToFile(String appName, String originalClassName, Class<?> annotItf,
        boolean threadIdAsAppId, boolean useNewAppClassName, boolean isMainClass)
        throws NotFoundException, CannotCompileException, ClassNotFoundException {
        CtClass appClass =
            modify(appName, originalClassName, annotItf, threadIdAsAppId, useNewAppClassName, isMainClass);
        try {
            appClass.writeFile();
        } catch (Exception e) {
            ErrorManager.fatal("Error writing the instrumented class file");
        }
    }

    /** Create new ClassPool object and load packages into it. */
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

    /**
     * Add main variables to the instrumented class.
     */
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

    /*
     * Create a Code Converter object and instrument each method based on whether it is the main method, an
     * orchestration method, or a web service method.
     */
    private static void instrumentClass(ClassPool cp, CtClass appClass, Class<?> annotItf, String itApiVar,
        String itSRVar, String itORVar, String itAppIdVar, String originalClassName, boolean isMainClass)
        throws NotFoundException, CannotCompileException {
        // Methods declared in the annotated interface
        Method[] remoteMethods = annotItf.getMethods();

        // Candidates to be instrumented if they are not remote
        CtMethod[] instrCandidates = appClass.getDeclaredMethods();

        ITAppEditor itAppEditor = new ITAppEditor(remoteMethods, instrCandidates, itApiVar, itSRVar, itORVar,
            itAppIdVar, appClass, originalClassName);
        // itAppEditor.setAppId(itAppIdVar);
        // itAppEditor.setAppClass(appClass);

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
        for (CtMethod m : instrCandidates) {
            if (LoaderUtils.checkRemote(m, remoteMethods, null, null) == null) {
                // Not a remote method, we must instrument it
                if (DEBUG) {
                    LOGGER.debug("Instrumenting method " + m.getName());
                }
                StringBuilder toInsertAfter = new StringBuilder();

                boolean isMainProgram = LoaderUtils.isMainMethod(m);
                boolean isOrchestration = LoaderUtils.isOrchestration(m);

                if ((isMainProgram && isMainClass) || (isOrchestration && IS_WS_CLASS)) {
                    LOGGER.debug("Inserting calls at the beginning and at the end of main");
                    if (!IS_WS_CLASS) { // Main program
                        LOGGER.debug("Inserting call stopIT at the end of main");
                        toInsertAfter.insert(0, itApiVar + ".stopIT(true);");
                    }
                    LOGGER.debug("Inserting call noMoreTasks at the end of main");
                    toInsertAfter.insert(0, itApiVar + ".noMoreTasks(" + itAppIdVar + ");");

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
                 * class ArrayAccessWatcher, whose parameters include the array. For the second round of
                 * instrumentation, the synchronization by transition to black box automatically synchronizes the arrays
                 * accessed. TODO: Change the order of instrumentation, so that we have more control about the
                 * synchronization, and we can distinguish between a write access and a read access (now it's read/write
                 * access by default, because it goes into the black box).
                 */
                m.instrument(converter);
                m.instrument(itAppEditor);
            }
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

    private static void addModifyVariablesMethods(CtClass appClass, String itApiVar, String itSRVar, String itORVar,
        String itAppIdVar, String instrumentationAppId, boolean isMainClass) throws CannotCompileException {
        /*
         * Insert printer method
         */
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

        /*
         * Insert method to retrieve the runtime instead of instantiating a new one
         */
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

        /*
         * Overloaded method to retrieve the runtime instead of instantiating a new one passing both SR and OR objects
         * instead of the loader
         */
        methodBody = new StringBuilder();
        methodBody.append("public static void setCOMPSsVariables( ").append(LoaderConstants.CLASS_COMPSSRUNTIME_API)
            .append(" runtime" + ", ").append(LoaderConstants.CLASS_STREAM_REGISTRY).append(" streamRegistry" + ", ")
            .append(LoaderConstants.CLASS_OBJECT_REGISTRY).append(" objectRegistry" + ", ")
            .append(LoaderConstants.CLASS_APP_ID).append(" appId" + ") {");
        methodBody.append(itApiVar).append("= runtime;");
        methodBody.append(itSRVar).append("= streamRegistry;");
        methodBody.append(itORVar).append("= objectRegistry;");
        methodBody.append(itAppIdVar).append("= appId;");
        methodBody.append("}");
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);

        /*
         * Insert method to start runtime - Creation of the COMPSsRuntimeImpl - Creation of the stream registry to keep
         * track of streams (with error handling) - Setting of the COMPSsRuntime interface variable - Start of the
         * COMPSsRuntimeImpl
         */
        methodBody = new StringBuilder();
        methodBody.append("public static void initCOMPSsVariables() {");
        if (isMainClass || IS_WS_CLASS) {
            methodBody.append("System.setProperty(").append(COMPSS_APP_CONSTANT).append(", \"")
                .append(appClass.getName()).append("\");");
        }
        methodBody.append(itApiVar).append(" = new ").append(LoaderConstants.CLASS_COMPSS_API_IMPL).append("();");
        methodBody.append(itApiVar).append(" = (").append(LoaderConstants.CLASS_COMPSSRUNTIME_API).append(")")
            .append(itApiVar).append(";");
        methodBody.append(itSRVar).append(" = new ").append(LoaderConstants.CLASS_STREAM_REGISTRY).append("((")
            .append(LoaderConstants.CLASS_LOADERAPI).append(") ").append(itApiVar).append(" );");
        methodBody.append(itORVar).append(" = new ").append(LoaderConstants.CLASS_OBJECT_REGISTRY).append("((")
            .append(LoaderConstants.CLASS_LOADERAPI).append(") ").append(itApiVar).append(" );");
        methodBody.append(itApiVar).append(".startIT();");
        if (WALL_CLOCK_LIMIT > 0) {
            // Setting wall clock limit with runtime stop.
            methodBody.append(itApiVar).append(".setWallClockLimit(").append(instrumentationAppId).append(",")
                .append(WALL_CLOCK_LIMIT).append("L, true);");
        }
        methodBody.append("}");
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);

        /*
         * Insert getter methods
         */
        methodBody = new StringBuilder();
        methodBody.append("public static ").append(LoaderConstants.CLASS_COMPSSRUNTIME_API).append(" getRuntime() {");
        methodBody.append("return ").append(itApiVar).append(";");
        methodBody.append("}");
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);

        methodBody = new StringBuilder();
        methodBody.append("public static ").append(LoaderConstants.CLASS_STREAM_REGISTRY)
            .append(" getStreamRegistry() {");
        methodBody.append("return ").append(itSRVar).append(";");
        methodBody.append("}");
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);

        methodBody = new StringBuilder();
        methodBody.append("public static ").append(LoaderConstants.CLASS_OBJECT_REGISTRY)
            .append(" getObjectRegistry() {");
        methodBody.append("return ").append(itORVar).append(";");
        methodBody.append("}");
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);

        methodBody = new StringBuilder();
        methodBody.append("public static ").append(LoaderConstants.CLASS_APP_ID).append(" getAppId() {");
        methodBody.append("return ").append(itAppIdVar).append(";");
        methodBody.append("}");
        m = CtNewMethod.make(methodBody.toString(), appClass);
        appClass.addMethod(m);
    }
}
