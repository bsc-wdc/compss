package es.bsc.compss.loader.total;

import java.lang.reflect.Method;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CodeConverter;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtField;
import javassist.CtMethod;
import javassist.Modifier;
import javassist.NotFoundException;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.loader.LoaderConstants;
import es.bsc.compss.loader.LoaderUtils;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;


public class ITAppModifier {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Constants
    private static final String COMPSS_APP_CONSTANT = LoaderConstants.CLASS_COMPSS_CONSTANTS + ".APP_NAME";
    private static final ClassPool CLASS_POOL = ClassPool.getDefault();
    private static final boolean WRITE_TO_FILE = System.getProperty(COMPSsConstants.COMPSS_TO_FILE) != null
            && System.getProperty(COMPSsConstants.COMPSS_TO_FILE).equals("true") ? true : false;

    // Flag to indicate in class is WS
    private static final boolean IS_WS_CLASS = System.getProperty(COMPSsConstants.COMPSS_IS_WS) != null
            && System.getProperty(COMPSsConstants.COMPSS_IS_WS).equals("true") ? true : false;

    // Flag to instrument main method. if COMPSS_IS_MAINCLASS main class not defined (Default case) isMain gets true;
    private static final boolean IS_MAIN_CLASS = System.getProperty(COMPSsConstants.COMPSS_IS_MAINCLASS) != null
            && System.getProperty(COMPSsConstants.COMPSS_IS_MAINCLASS).equals("false") ? false : true;


    /**
     * Modify method
     * 
     * @param appName
     * @return
     * @throws NotFoundException
     * @throws CannotCompileException
     * @throws ClassNotFoundException
     */
    public Class<?> modify(String appName) throws NotFoundException, CannotCompileException, ClassNotFoundException {

        Class<?> annotItf = Class.forName(appName + LoaderConstants.ITF_SUFFIX);
        // Methods declared in the annotated interface
        Method[] remoteMethods = annotItf.getMethods();

        /*
         * Use the application editor to include the COMPSs API calls on the application code
         */
        CLASS_POOL.importPackage(LoaderConstants.PACKAGE_COMPSS_ROOT);
        CLASS_POOL.importPackage(LoaderConstants.PACKAGE_COMPSS_API);
        CLASS_POOL.importPackage(LoaderConstants.PACKAGE_COMPSS_API_IMPL);
        CLASS_POOL.importPackage(LoaderConstants.PACKAGE_COMPSS_LOADER);
        CLASS_POOL.importPackage(LoaderConstants.PACKAGE_COMPSS_LOADER_TOTAL);

        CtClass appClass = CLASS_POOL.get(appName);
        CtClass itApiClass = CLASS_POOL.get(LoaderConstants.CLASS_COMPSSRUNTIME_API);
        CtClass itSRClass = CLASS_POOL.get(LoaderConstants.CLASS_STREAM_REGISTRY);
        CtClass itORClass = CLASS_POOL.get(LoaderConstants.CLASS_OBJECT_REGISTRY);
        CtClass appIdClass = CLASS_POOL.get(LoaderConstants.CLASS_APP_ID);

        String varName = LoaderUtils.randomName(5, LoaderConstants.STR_COMPSS_PREFIX);
        String itApiVar = varName + LoaderConstants.STR_COMPSS_API;
        String itSRVar = varName + LoaderConstants.STR_COMPSS_STREAM_REGISTRY;
        String itORVar = varName + LoaderConstants.STR_COMPSS_OBJECT_REGISTRY;
        String itAppIdVar = varName + LoaderConstants.STR_COMPSS_APP_ID;
        CtField itApiField = new CtField(itApiClass, itApiVar, appClass);
        CtField itSRField = new CtField(itSRClass, itSRVar, appClass);
        CtField itORField = new CtField(itORClass, itORVar, appClass);
        CtField appIdField = new CtField(appIdClass, itAppIdVar, appClass);
        itApiField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        itSRField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        itORField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        appIdField.setModifiers(Modifier.PRIVATE | Modifier.STATIC);
        appClass.addField(itApiField);
        appClass.addField(itSRField);
        appClass.addField(itORField);
        appClass.addField(appIdField);

        /*
         * Create a static constructor to initialize the runtime Create a shutdown hook to stop the runtime before the
         * JVM ends
         */
        manageStartAndStop(appClass, itApiVar, itSRVar, itORVar);

        /*
         * Create IT App Editor
         */
        CtMethod[] instrCandidates = appClass.getDeclaredMethods(); // Candidates to be instrumented if they are not
                                                                    // remote
        ITAppEditor itAppEditor = new ITAppEditor(remoteMethods, instrCandidates, itApiVar, itSRVar, itORVar, itAppIdVar, appClass);
        // itAppEditor.setAppId(itAppIdVar);
        // itAppEditor.setAppClass(appClass);

        /*
         * Create Code Converter
         */
        CodeConverter converter = new CodeConverter();
        CtClass arrayWatcher = CLASS_POOL.get(LoaderConstants.CLASS_ARRAY_ACCESS_WATCHER);
        CodeConverter.DefaultArrayAccessReplacementMethodNames names = new CodeConverter.DefaultArrayAccessReplacementMethodNames();
        converter.replaceArrayAccess(arrayWatcher, (CodeConverter.ArrayAccessReplacementMethodNames) names);

        /*
         * Find the methods declared in the application class that will be instrumented - Main - Constructors - Methods
         * that are not in the remote list
         */
        if (DEBUG) {
            LOGGER.debug("Flags: ToFile: " + WRITE_TO_FILE + " isWS: " + IS_WS_CLASS + " isMainClass: " + IS_MAIN_CLASS);
        }
        for (CtMethod m : instrCandidates) {
            if (LoaderUtils.checkRemote(m, remoteMethods) == null) {
                // Not a remote method, we must instrument it
                if (DEBUG) {
                    LOGGER.debug("Instrumenting method " + m.getName());
                }
                StringBuilder toInsertBefore = new StringBuilder();
                StringBuilder toInsertAfter = new StringBuilder();

                /*
                 * Add local variable to method representing the execution id, which will be the current thread id. Used
                 * for Services, to handle multiple service executions simultaneously with a single runtime For normal
                 * applications, there will be only one execution id.
                 */
                m.addLocalVariable(itAppIdVar, appIdClass);
                toInsertBefore.append(itAppIdVar).append(" = new Long(Thread.currentThread().getId());");

                // TODO remove old code:
                // boolean isMainProgram = writeToFile ? LoaderUtils.isOrchestration(m) : LoaderUtils.isMainMethod(m);
                boolean isMainProgram = LoaderUtils.isMainMethod(m);
                boolean isOrchestration = LoaderUtils.isOrchestration(m);

                if (isMainProgram && IS_MAIN_CLASS) {
                    LOGGER.debug("Inserting calls at the beginning and at the end of main");

                    if (IS_WS_CLASS) { //
                        LOGGER.debug("Inserting calls noMoreTasks at the end of main");
                        toInsertAfter.insert(0, itApiVar + ".noMoreTasks(" + itAppIdVar + ", true);");
                        m.insertBefore(toInsertBefore.toString());
                        m.insertAfter(toInsertAfter.toString()); // executed only if Orchestration finishes properly
                    } else { // Main program
                        LOGGER.debug("Inserting calls noMoreTasks and stopIT at the end of main");
                        // Set global variable for main as well, will be used in code inserted after to be run no matter
                        // what
                        toInsertBefore.append(appName).append('.').append(itAppIdVar)
                                .append(" = new Long(Thread.currentThread().getId());");
                        // toInsertAfter.append("System.exit(0);");
                        toInsertAfter.insert(0, itApiVar + ".stopIT(true);");
                        toInsertAfter.insert(0, itApiVar + ".noMoreTasks(" + appName + '.' + itAppIdVar + ", true);");
                        m.insertBefore(toInsertBefore.toString());
                        m.insertAfter(toInsertAfter.toString(), true); // executed no matter what
                    }

                    /*
                     * Instrumenting first the array accesses makes each array access become a call to a black box
                     * method of class ArrayAccessWatcher, whose parameters include the array. For the second round of
                     * instrumentation, the synchronization by transition to black box automatically synchronizes the
                     * arrays accessed. TODO: Change the order of instrumentation, so that we have more control about
                     * the synchronization, and we can distinguish between a write access and a read access (now it's
                     * read/write access by default, because it goes into the black box).
                     */
                    m.instrument(converter);
                    m.instrument(itAppEditor);
                } else if (isOrchestration) {
                    if (IS_WS_CLASS) { //
                        LOGGER.debug("Inserting calls noMoreTasks and stopIT at the end of orchestration");
                        toInsertAfter.insert(0, itApiVar + ".noMoreTasks(" + itAppIdVar + ", true);");
                        m.insertBefore(toInsertBefore.toString());
                        m.insertAfter(toInsertAfter.toString()); // executed only if Orchestration finishes properly
                    } else {
                        LOGGER.debug("Inserting only before at the beginning of an orchestration");
                        m.insertBefore(toInsertBefore.toString());
                        // TODO remove old code m.insertAfter(toInsertAfter.toString());
                        // executed only if Orchestration finishes properly
                    }
                    m.instrument(converter);
                    m.instrument(itAppEditor);
                } else {
                    LOGGER.debug("Inserting only before");
                    m.insertBefore(toInsertBefore.toString());
                    if (IS_WS_CLASS) {
                        // If we're instrumenting a service class, only instrument private methods, public might be
                        // non-OE operations
                        if (Modifier.isPrivate(m.getModifiers())) {
                            m.instrument(converter);
                            m.instrument(itAppEditor);
                        }
                    } else {
                        // For an application class, instrument all non-remote methods
                        m.instrument(converter);
                        m.instrument(itAppEditor);
                    }
                }
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

        if (WRITE_TO_FILE) {
            // Write the modified class to disk
            try {
                appClass.writeFile();
            } catch (Exception e) {
                ErrorManager.fatal("Error writing the instrumented class file");
            }
            return null;
        } else {
            /*
             * Load the modified class into memory and return it. Generally, once a class is loaded into memory no
             * further modifications can be performed on it.
             */
            return appClass.toClass();
        }
    }

    private void manageStartAndStop(CtClass appClass, String itApiVar, String itSRVar, String itORVar)
            throws CannotCompileException, NotFoundException {

        if (DEBUG) {
            LOGGER.debug("Previous class initializer is " + appClass.getClassInitializer());
        }

        CtConstructor initializer = appClass.makeClassInitializer();

        /*
         * - Creation of the COMPSsRuntimeImpl - Creation of the stream registry to keep track of streams (with error
         * handling) - Setting of the COMPSsRuntime interface variable - Start of the COMPSsRuntimeImpl
         */
        StringBuilder toInsertBefore = new StringBuilder();
        if (IS_MAIN_CLASS || IS_WS_CLASS) {
            toInsertBefore.append("System.setProperty(" + COMPSS_APP_CONSTANT + ", \"" + appClass.getName() + "\");");
        }
        toInsertBefore.append(itApiVar + " = new " + LoaderConstants.CLASS_COMPSS_API_IMPL + "();")
                .append(itApiVar + " = (" + LoaderConstants.CLASS_COMPSSRUNTIME_API + ")" + itApiVar + ";")
                .append(itSRVar + " = new " + LoaderConstants.CLASS_STREAM_REGISTRY + "((" + LoaderConstants.CLASS_LOADERAPI + ") "
                        + itApiVar + " );")
                .append(itORVar + " = new " + LoaderConstants.CLASS_OBJECT_REGISTRY + "((" + LoaderConstants.CLASS_LOADERAPI + ") "
                        + itApiVar + " );")
                .append(itApiVar + ".startIT();");

        initializer.insertBefore(toInsertBefore.toString());
    }

}
