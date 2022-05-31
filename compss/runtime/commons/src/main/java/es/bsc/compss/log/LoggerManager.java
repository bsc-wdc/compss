/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.util.ErrorManager;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;


public class LoggerManager {

    private static boolean loggerAlreadyLoaded = false;

    private static final String ERROR_COMPSs_LOG_BASE_DIR = "ERROR: Cannot create .COMPSs base log directory";
    private static final String ERROR_APP_OVERLOAD = "ERROR: Cannot erase overloaded directory";
    private static final String ERROR_APP_LOG_DIR = "ERROR: Cannot create application log directory";
    private static final String WARN_FOLDER_OVERLOAD = "WARNING: Reached maximum number of executions for this"
        + " application. To avoid this warning please clean .COMPSs folder";

    private static final int MAX_OVERLOAD = 100; // Maximum number of executions of same application

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);

    private static String userExecutionDirPath;
    private static String compssLogBaseDirPath;

    private static String appLogDirPath;


    // LoggerManager class should be called statically
    private LoggerManager() {
    }

    private static boolean deleteDirectory(File directory) {
        if (!directory.exists()) {
            return false;
        }

        File[] files = directory.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) {
                    deleteDirectory(f);
                } else {
                    if (!f.delete()) {
                        return false;
                    }
                }
            }
        }

        return directory.delete();
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

        // Gets user execution directory
        LoggerManager.userExecutionDirPath = System.getProperty("user.dir");

        /* Creates base Runtime structure directories ************************** */
        boolean mustCreateExecutionSandbox = true;
        // Checks if specific log base dir has been given
        String specificOpt = System.getProperty(COMPSsConstants.SPECIFIC_LOG_DIR);
        if (specificOpt != null && !specificOpt.isEmpty()) {
            LoggerManager.compssLogBaseDirPath =
                specificOpt.endsWith(File.separator) ? specificOpt : specificOpt + File.separator;
            mustCreateExecutionSandbox = false; // This is the only case where
            // the sandbox is provided
        } else {
            // Checks if base log dir has been given
            String baseOpt = System.getProperty(COMPSsConstants.BASE_LOG_DIR);
            if (baseOpt != null && !baseOpt.isEmpty()) {
                baseOpt = baseOpt.endsWith(File.separator) ? baseOpt : baseOpt + File.separator;
                LoggerManager.compssLogBaseDirPath = baseOpt + ".COMPSs" + File.separator;
            } else {
                // No option given - load default (user home)
                LoggerManager.compssLogBaseDirPath =
                    System.getProperty("user.home") + File.separator + ".COMPSs" + File.separator;
            }
        }

        if (!new File(LoggerManager.compssLogBaseDirPath).exists()) {
            if (!new File(LoggerManager.compssLogBaseDirPath).mkdir()) {
                ErrorManager.error(ERROR_COMPSs_LOG_BASE_DIR + " at " + compssLogBaseDirPath);
            }
        }

        // Load working directory. Different for regular applications and
        // services
        if (mustCreateExecutionSandbox) {
            String appName;
            if (System.getProperty(COMPSsConstants.SERVICE_NAME) != null) {
                /*
                 * SERVICE - Gets appName - Overloads the service folder for different executions - MAX_OVERLOAD raises
                 * warning - Changes working directory to serviceName !!!!
                 */
                appName = System.getProperty(COMPSsConstants.SERVICE_NAME);
            } else {
                appName = System.getProperty(COMPSsConstants.APP_NAME);
            }

            int overloadCode = 1;
            String appLog =
                LoggerManager.compssLogBaseDirPath + appName + "_0" + String.valueOf(overloadCode) + File.separator;
            String oldest = appLog;
            while ((new File(appLog).exists()) && (overloadCode <= MAX_OVERLOAD)) {
                // Check oldest file (for overload if needed)
                if (new File(oldest).lastModified() > new File(appLog).lastModified()) {
                    oldest = appLog;
                }
                // Next step
                overloadCode = overloadCode + 1;
                if (overloadCode < 10) {
                    appLog = LoggerManager.compssLogBaseDirPath + appName + "_0" + String.valueOf(overloadCode)
                        + File.separator;
                } else {
                    appLog = LoggerManager.compssLogBaseDirPath + appName + "_" + String.valueOf(overloadCode)
                        + File.separator;
                }
            }
            if (overloadCode > MAX_OVERLOAD) {
                // Select the last modified folder
                appLog = oldest;

                // Overload
                System.err.println(WARN_FOLDER_OVERLOAD);
                System.err.println("Overwriting entry: " + appLog);

                // Clean previous results to avoid collisions
                if (!deleteDirectory(new File(appLog))) {
                    ErrorManager.error(ERROR_APP_OVERLOAD);
                }
            }

            // We have the final appLogDirPath
            LoggerManager.appLogDirPath = appLog;
            if (!new File(LoggerManager.appLogDirPath).mkdir()) {
                ErrorManager.error(ERROR_APP_LOG_DIR);
            }
        } else {
            // The option specific_log_dir has been given. NO sandbox created
            LoggerManager.appLogDirPath = LoggerManager.compssLogBaseDirPath;
        }
        System.setProperty(COMPSsConstants.APP_LOG_DIR, appLogDirPath);
        ((LoggerContext) LogManager.getContext(false)).reconfigure();
    }

    public static void setUserExecutionDirPath(String userExecutionDirPath) {
        LOGGER.debug("User execution dir path: " + userExecutionDirPath);
        LoggerManager.userExecutionDirPath = userExecutionDirPath;
    }

    public static String getUserExecutionDirPath() {
        return userExecutionDirPath;
    }

    public static void setCompssLogBaseDirPath(String compssLogBaseDirPath) {
        LOGGER.debug("Compss log base dir path: " + compssLogBaseDirPath);
        LoggerManager.compssLogBaseDirPath = compssLogBaseDirPath;
    }

    public static String getCompssLogBaseDirPath() {
        return compssLogBaseDirPath;
    }

    public static void setAppLogDirPath(String appLogDirPath) {
        LOGGER.debug("App log dir path: " + appLogDirPath);
        LoggerManager.appLogDirPath = appLogDirPath;
    }

    public static String getAppLogDirPath() {
        return appLogDirPath;
    }
}
