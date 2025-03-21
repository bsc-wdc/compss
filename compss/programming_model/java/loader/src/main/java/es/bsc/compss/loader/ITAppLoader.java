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
package es.bsc.compss.loader;

import es.bsc.compss.loader.total.ITAppModifier;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;

import java.lang.reflect.Method;
import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ITAppLoader {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);


    /**
     * Factored out loading function so that subclasses of ITAppLoader can re-use this code.
     */
    protected static void load(String appName, String[] appArgs) throws Exception {
        /*
         * We will have two class loaders: - Custom loader: to load our javassist version classes and the classes that
         * use them. - System loader: parent of the custom loader, it will load the rest of the classes (including the
         * one of the application, once it has been modified).
         */
        CustomLoader myLoader = null;

        try {
            myLoader = new CustomLoader(new URL[] {});

            LOGGER.debug("Modifying application " + appName);
            // Get annotated interface and run main modify method
            Class<?> annotItf = Class.forName(appName + LoaderConstants.ITF_SUFFIX);
            Class<?> modAppClass = ITAppModifier.modifyToMemory(appName, null, annotItf, true, false, false, true);
            if (modAppClass != null) { // if null, the modified app has been written to a file, and thus we're done
                LOGGER.debug("Application " + appName + " instrumented, executing...");
                // Start runtime
                Method initializer = modAppClass.getDeclaredMethod("initCOMPSsVariables");
                initializer.invoke(null);
                // Start main
                Method main = modAppClass.getDeclaredMethod("main", new Class[] { String[].class });
                main.invoke(null, new Object[] { appArgs });
            }
        } catch (Exception e) {
            throw e;
        } finally {
            // Close loader if needed
            if (myLoader != null) {
                myLoader.close();
            }
        }
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
        } catch (Exception e) {
            LOGGER.fatal("There was an error when loading or executing your application.", e);
            System.exit(1);
        }
    }

}
