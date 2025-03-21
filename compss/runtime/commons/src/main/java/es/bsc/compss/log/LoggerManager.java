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
package es.bsc.compss.log;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.FileOperations;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;


public class LoggerManager {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);

    private static final int MAX_OVERLOAD = 100; // Maximum number of executions of same application

    // Error messages
    private static final String ERROR_COMPSS_LOG_BASE_DIR = "ERROR: Cannot create .COMPSs base log directory";
    private static final String ERROR_APP_OVERLOAD = "ERROR: Cannot erase overloaded directory";
    private static final String ERROR_APP_LOG_DIR = "ERROR: Cannot create application log directory";
    private static final String WARN_FOLDER_OVERLOAD = "WARNING: Reached maximum number of executions for this"
        + " application. To avoid this warning please clean .COMPSs folder";
    private static final String ERROR_JOBS_DIR = "ERROR: Cannot create jobs directory";
    private static final String ERROR_WORKERS_DIR = "ERROR: Cannot create workers directory";

    private static boolean loggerAlreadyLoaded = false;
    private static final String logDir;
    private static final String jobsLogDir;
    private static final String workersLogDir;

    static {
        String log = System.getProperty(COMPSsConstants.LOG_DIR);

        if (log != null && !log.isEmpty()) {

            log = log.endsWith(File.separator) ? log : log + File.separator;
            if (!new File(log).exists()) {
                if (!new File(log).mkdirs()) {
                    ErrorManager.error(ERROR_APP_LOG_DIR + " at " + log);
                }
            }
        } else {
            log = COMPSsDefaults.LOG_DIR;
            if (!new File(log).exists()) {
                if (!new File(log).mkdirs()) {
                    ErrorManager.error(ERROR_COMPSS_LOG_BASE_DIR + " at " + log);
                }
            }

            log = log.endsWith(File.separator) ? log : log + File.separator;

            String appName;
            if (System.getProperty(COMPSsConstants.SERVICE_NAME) != null) {
                appName = System.getProperty(COMPSsConstants.SERVICE_NAME);
            } else {
                appName = System.getProperty(COMPSsConstants.APP_NAME);
            }
            log = log + appName;

            String oldest = null;
            long lastModified = System.currentTimeMillis();
            boolean created = false;
            for (int overloadCode = 1; !created && overloadCode < MAX_OVERLOAD; overloadCode++) {
                String appLog = log + "_" + String.format("%02d", overloadCode) + File.separator;
                if (new File(appLog).exists()) {
                    long modified = new File(appLog).lastModified();
                    if (lastModified > modified) {
                        oldest = appLog;
                    }
                } else {
                    created = new File(appLog).mkdirs();
                    log = appLog;
                }
            }
            if (!created) {
                log = oldest;
                System.err.println(WARN_FOLDER_OVERLOAD);
                System.err.println("Overwriting entry: " + log);
                // Clean previous results to avoid collisions
                try {
                    File f = new File(log);
                    FileOperations.deleteFile(f, null);
                    if (!f.mkdir()) {
                        ErrorManager.error(ERROR_APP_LOG_DIR);
                    }
                } catch (Exception e) {
                    ErrorManager.error(ERROR_APP_OVERLOAD);
                }
            }
        }
        logDir = log;

        jobsLogDir = logDir + "jobs" + File.separator;
        workersLogDir = logDir + "workers" + File.separator;

    }


    // LoggerManager class should be called statically
    private LoggerManager() {
    }

    /**
     * Initializes the logger with the current environment variables.
     */
    public static void init() {

        if (loggerAlreadyLoaded) {
            LOGGER.debug("LoggerManager already initialized, no need for a second initialization");
            return;
        }
        loggerAlreadyLoaded = true;
        System.setProperty(COMPSsConstants.LOG_DIR, logDir);
        ((LoggerContext) LogManager.getContext(false)).reconfigure();

        /*
         * Create a jobs dir where to store: - Jobs output files - Jobs error files
         */
        if (!new File(jobsLogDir).mkdirs()) {
            ErrorManager.error(ERROR_JOBS_DIR);
        }

        /*
         * Create a workers dir where to store: - Worker out files - Worker error files
         */
        if (!new File(workersLogDir).mkdirs()) {
            ErrorManager.error(ERROR_WORKERS_DIR);
        }
    }

    public static String getLogDir() {
        return logDir;
    }

    public static String getJobsLogDir() {
        return jobsLogDir;
    }

    public static String getWorkersLogDir() {
        return workersLogDir;
    }
}
