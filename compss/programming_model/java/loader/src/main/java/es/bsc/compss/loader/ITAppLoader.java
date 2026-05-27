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
package es.bsc.compss.loader;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.loader.editing.CustomLoader;
import es.bsc.compss.loader.editing.ITAppModifier;
import es.bsc.compss.loader.workflow.JavaWorkflow;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ITAppLoader {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);
    // Wall clock limit definition
    private static final long WALL_CLOCK_LIMIT =
        Long.parseLong(System.getProperty(LoaderConstants.WALL_CLOCK_LIMIT, "0"));


    /**
     * Factored out loading function so that subclasses of ITAppLoader can re-use this code.
     */
    protected static void load(String appName, String[] appArgs) throws Throwable {
        try (CustomLoader myLoader = new CustomLoader(new URL[] {})) {
            instrumentAndRun(appName, appArgs);
        }
    }

    protected static void instrumentAndRun(String appName, String[] appArgs) throws Throwable {
        // instrumenting class according the CEI named appName + Itf
        Class<?> annotItf = getCEI(appName);
        Class<?> modAppClass = instrumentAppClass(appName, annotItf);

        System.setProperty(COMPSsConstants.APP_NAME, appName);

        // Start runtime and run workflow
        COMPSsRuntime rt = instantiateCOMPSsRuntime();
        LOGGER.debug("Starting runtime");
        rt.startIT();
        try {
            runWorkflow(rt, appName, modAppClass, appArgs);
        } finally {
            rt.stopIT(true);
        }
    }

    private static Class<?> getCEI(String appName) throws ClassNotFoundException {
        String ceiClass = appName + LoaderConstants.ITF_SUFFIX;
        try {
            // Get annotated interface and run main modify method
            return Class.forName(ceiClass);
        } catch (ClassNotFoundException cnfe) {
            LOGGER.error("Could not find CEI class: " + appName);
            throw cnfe;
        }
    }

    private static Class<?> instrumentAppClass(String appName, Class<?> annotItf) throws Exception {
        Class<?> modAppClass;
        try {
            LOGGER.debug("Modifying application " + appName);
            modAppClass = ITAppModifier.modifyToMemory(appName, annotItf, true, true);
            LOGGER.debug("Application " + appName + " instrumented, executing...");
        } catch (Exception e) {
            LOGGER.error("Could not instrument application class: " + appName);
            throw e;
        }
        return modAppClass;
    }

    private static COMPSsRuntime instantiateCOMPSsRuntime() throws Exception {
        try {
            // Start runtime
            LOGGER.debug("Creating runtime");
            Class<?> schedClass = Class.forName(LoaderConstants.CLASS_COMPSS_API_IMPL);
            Constructor<?> rtConstructor = schedClass.getDeclaredConstructor();
            return (COMPSsRuntime) rtConstructor.newInstance();
        } catch (Exception e) {
            LOGGER.error("Could not instantiate COMPSs runtime.");
            throw e;
        }
    }

    private static void runWorkflow(COMPSsRuntime rt, String appName, Class<?> appClass, String[] appArgs)
        throws Throwable {
        JavaWorkflow wf = createWorkflow(rt, appClass);
        Timer timer = null;
        TimerTask wcTask = null;

        if (WALL_CLOCK_LIMIT > 0) {
            timer = new Timer("Application wall clock limit timer");
            wcTask = new TimerTask() {

                @Override
                public void run() {
                    System.err
                        .println("WARNING: Wall clock limit reached for app " + wf.getId() + "! Cancelling tasks...");
                    synchronized (wf) { // When wall clock hits, main app should not be able to continue
                        wf.cancelApplicationTasks();
                        wf.noMoreTasks();
                        wf.deregister(false);
                        rt.stopIT(true);
                        System.exit(122);
                    }
                }
            };
            LOGGER.info("Setting up wall clock limit after " + WALL_CLOCK_LIMIT + " seconds");
            timer.schedule(wcTask, WALL_CLOCK_LIMIT * 1000);
        }

        try {
            executeAsWorkflow(wf, appName, appClass, appArgs);
        } finally {
            if (wcTask != null) {
                wcTask.cancel();
                timer.cancel();
            }
            synchronized (wf) {
                wf.deregister(false);
            }
        }
    }

    private static void executeAsWorkflow(JavaWorkflow wf, String appName, Class<?> appClass, String[] appArgs)
        throws Throwable {
        LOGGER.debug("Executing " + appName);
        // Start main
        Method main = appClass.getDeclaredMethod("main", new Class[] { String[].class });
        try {
            main.invoke(null, new Object[] { appArgs });
        } catch (InvocationTargetException e) {
            wf.cancelApplicationTasks();
            throw e.getCause();
        } finally {
            wf.noMoreTasks();
        }

    }

    private static JavaWorkflow createWorkflow(COMPSsRuntime rt, Class<?> modAppClass)
        throws ReflectiveOperationException {
        Method initializer = modAppClass.getDeclaredMethod("setupCOMPSs",
            new Class<?>[] { Class.forName(LoaderConstants.CLASS_COMPSSRUNTIME_API),
                Class.forName(LoaderConstants.CLASS_APP_RUNNER) });
        Object[] values = new Object[] { rt,
            null };
        return (JavaWorkflow) initializer.invoke(null, values);
    }

    /**
     * Entry point for the instrumentation and start of the COMPSs application.
     */
    public static void main(String[] args) throws Exception {
        // Check args
        if (args.length < 2) {
            ErrorManager.fatal("Error: missing arguments for loader");
        }

        // Prepare the arguments
        String[] appArgs = new String[args.length - 2];
        System.arraycopy(args, 2, appArgs, 0, appArgs.length);

        // Load the application
        try {
            load(args[1], appArgs);
        } catch (Throwable e) {
            LOGGER.fatal("There was an error when loading or executing your application.", e);
            System.exit(1);
        }
    }

}
