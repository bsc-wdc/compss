/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.logging.log4j.Logger;


/**
 * Support class to load jar files to the classpath.
 */
public class Classpath {

    /**
     * Loads all the jars existing in the given path {@code jarPath}.
     * 
     * @param jarPath Path where to look for jar files.
     * @param logger Logger instance to print debug/info/error information.
     * @throws FileNotFoundException Raised when the provided {@code jarPath} does not exist.
     */
    public static void loadPath(String jarPath, Logger logger) throws FileNotFoundException {
        // Check if jarPath exists
        File directory = new File(jarPath);
        if (!directory.exists()) {
            throw new FileNotFoundException();
        }

        // Load the addURL method
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

        // Create the sysloader
        URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();

        // Scan folder for jar files
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
