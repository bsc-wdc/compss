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
package es.bsc.compss.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.instrument.Instrumentation;
import java.util.jar.JarFile;

import org.apache.logging.log4j.Logger;


public class Classpath {

    private static Instrumentation instr;


    private Classpath() {
        // To avoid instantiation
    }

    /**
     * Premain called by the JRE to load the class using -javaagent:/path/to/jar.
     * 
     * @param agentArgs Agent arguments (ignored).
     * @param instrumentation Instrumentation.
     */
    public static void premain(String agentArgs, Instrumentation instrumentation) {
        if (instrumentation == null) {
            throw new NullPointerException("Instrumentation");
        }

        instr = instrumentation;
    }

    /**
     * Returns whether the JarLoader has been enabled by the host JRE or not.
     * 
     * @return {@literal true} if the host JRE has enabled the JarLoader, {@literal false} otherwise.
     */
    public static synchronized boolean isSupported() {
        return instr != null;
    }

    /**
     * Loads all the jars existing in the given path {@code jarPath}.
     * 
     * @param jarsPath Path where to look for jar files.
     * @param logger Logger instance to print debug/info/error information.
     */
    public static void loadJarsInPath(String jarsPath, Logger logger) {
        // Checks
        if (jarsPath == null) {
            logger.error("ERROR: Skipping JAR addition because provided jar is null");
            return;
        }
        File jarsFileOrDirectory = new File(jarsPath);
        if (!jarsFileOrDirectory.exists()) {
            logger.error("ERROR: Skipping JAR addition because jar path was not found at "
                + jarsFileOrDirectory.getAbsolutePath());
            return;
        }

        // Scan for any existing jar file and load it
        scanFolder(jarsFileOrDirectory, logger);
    }

    /**
     * Scans a the given file or folder and loads any valid jar file.
     * 
     * @param file File or folder.
     * @param logger Logger instance to print debug/info/error information.
     */
    private static void scanFolder(File file, Logger logger) {
        // Scan folder
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            for (File child : children) {
                scanFolder(child, logger);
            }
            return;
        }

        // Load file
        try {
            addToClasspath(file);
        } catch (IOException ioe) {
            logger.error("COULD NOT LOAD JAR AT " + file.getAbsolutePath(), ioe);
        }
    }

    /**
     * Adds a JAR file to the list of JAR files searched by the system class loader. This effectively adds a new JAR to
     * the class path.
     *
     * @param jarFile the JAR file to add
     * @throws IOException if there is an error accessing the JAR file
     */
    private static synchronized void addToClasspath(File jarFile) throws IOException {
        // Checks
        if (jarFile == null) {
            throw new NullPointerException("ERROR: Provided jar file is null");
        }
        if (!jarFile.exists()) {
            throw new FileNotFoundException("ERROR: jar file not found at " + jarFile.getAbsolutePath());
        }
        if (!jarFile.canRead()) {
            throw new IOException("ERROR: Can't read jar file at " + jarFile.getAbsolutePath());
        }
        if (!jarFile.isFile()) {
            throw new IOException("ERROR: jar file is not a file at " + jarFile.getAbsolutePath());
        }

        // Add jar
        instr.appendToSystemClassLoaderSearch(new JarFile(jarFile));
    }

}
