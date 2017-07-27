package es.bsc.compss.loader;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.ErrorManager;

import java.lang.reflect.Method;
import java.net.URL;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ITAppLoader {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.LOADER);


    /**
     * Factored out loading function so that subclasses of ITAppLoader can re-use this code
     *
     * @param chosenLoader
     * @param appName
     *
     */
    protected static void load(String chosenLoader, String appName, String[] appArgs) throws Exception {
        /*
         * We will have two class loaders: - Custom loader: to load our javassist version classes and the classes that
         * use them. - System loader: parent of the custom loader, it will load the rest of the classes (including the
         * one of the application, once it has been modified).
         */
        CustomLoader myLoader = null;

        try {
            myLoader = new CustomLoader(new URL[] {});

            // Add the jars that the custom class loader needs
            String compssHome = System.getenv(COMPSsConstants.COMPSS_HOME);
            myLoader.addFile(compssHome + LoaderConstants.ENGINE_JAR_WITH_REL_PATH);

            /*
             * The custom class loader must load the class that will modify the application and invoke the modify method
             * on an instance of this class
             */
            String loaderName = LoaderConstants.CUSTOM_LOADER_PREFIX + chosenLoader + LoaderConstants.CUSTOM_LOADER_SUFFIX;
            Class<?> modifierClass = myLoader.loadClass(loaderName);

            Object modifier = modifierClass.newInstance();
            LOGGER.debug("Modifying application " + appName + " with loader " + chosenLoader);

            Method method = modifierClass.getMethod("modify", new Class[] { String.class });
            Class<?> modAppClass = (Class<?>) method.invoke(modifier, new Object[] { appName });
            if (modAppClass != null) { // if null, the modified app has been written to a file, and thus we're done
                LOGGER.debug("Application " + appName + " instrumented, executing...");
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
            load(args[0], args[1], appArgs);
        } catch (Exception e) {
            LOGGER.fatal("There was an error when loading or executing your application.", e);
            System.exit(1);
        }
    }

}
