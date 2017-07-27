package es.bsc.compss.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.logging.log4j.Logger;


/**
 * Support class to load jar files to the classpath
 *
 */
public class Classpath {

    /**
     * Loads all the jars existing in the given path @jarPath
     * 
     * @param jarPath
     * @param logger
     * @throws FileNotFoundException
     */
    public static void loadPath(String jarPath, Logger logger) throws FileNotFoundException {
        File directory = new File(jarPath);
        if (!directory.exists()) {
            throw new FileNotFoundException();
        }

        URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
        Class<?> sysclass = URLClassLoader.class;
        Method method = null;
        try {
            method = sysclass.getDeclaredMethod("addURL", new Class[] { URL.class });
        } catch (NoSuchMethodException | SecurityException e) {
            // Method is always defined.
        }
        if (method == null) {
            throw new FileNotFoundException();
        }
        
        method.setAccessible(true);
        scanFolder(sysloader, method, directory, logger);
    }

    private static void scanFolder(Object callee, Method method, File file, Logger logger) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File child : children) {
                scanFolder(callee, method, child, logger);
            }
        } else {
            try {
                method.invoke(callee, new Object[] { (new File(file.getAbsolutePath())).toURI().toURL() });
                logger.info(file.getAbsolutePath() + " ADDED TO THE CLASSPATH");
            } catch (Exception e) {
                logger.error("COULD NOT LOAD JAR " + file.getAbsolutePath(), e);
            }
        }
    }

}
