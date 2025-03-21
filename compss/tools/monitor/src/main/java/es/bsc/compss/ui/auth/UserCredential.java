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
package es.bsc.compss.ui.auth;

import es.bsc.compss.commons.Loggers;
import es.bsc.compss.ui.Application;
import es.bsc.compss.ui.Constants;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class UserCredential {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.UI_AUTHENTICATION);
    private static final String RELATIVE_LOG_LOCATION = File.separator + ".COMPSs" + File.separator;

    private String username;
    private String compssBaseLog;
    private Application monitoredApp;
    private boolean authenticated = false;


    /**
     * Creates a new empty user credential.
     */
    public UserCredential() {
        this.username = "";
        this.compssBaseLog = "";
        this.monitoredApp = new Application();
    }

    /**
     * Creates a new user credential for user {@code username}.
     * 
     * @param username Username.
     */
    public UserCredential(String username) {
        LOGGER.debug("Creating user credentials...");
        if (username != null) {
            if (!username.isEmpty()) {
                this.username = username;
            } else {
                loadDefaultOrEnvironmentUser();
                this.compssBaseLog = "";
                this.monitoredApp = new Application();
            }
        } else {
            loadDefaultOrEnvironmentUser();
            this.compssBaseLog = "";
            this.monitoredApp = new Application();
        }
        LOGGER.info("User credentails loaded: " + this.username);
    }

    /**
     * Sets whether the current user has been authenticated or not.
     * 
     * @return Whether the current user has been authenticated or not.
     */
    public boolean setAuthenticated() {
        LOGGER.debug("Verifying user credentials...");
        if (this.username.equals(Constants.USER_DEFAULT)) {
            loadDefaultOrEnvironmentLogFolder();
            this.authenticated = true;
            LOGGER.debug(Constants.USER_DEFAULT + "user credentials loaded");
        } else if (this.username.equals(Constants.USER_ENVIRONMENT)) {
            loadDefaultOrEnvironmentLogFolder();
            this.authenticated = true;
            LOGGER.debug(Constants.USER_ENVIRONMENT + "user credentials loaded");
        } else if (this.username.startsWith(File.separator)) {
            // Loading direct folder without user
            if (this.username.endsWith(".COMPSs") || this.username.endsWith(".COMPSs" + File.separator)) {
                this.compssBaseLog = this.username;
            } else {
                this.compssBaseLog = this.username + RELATIVE_LOG_LOCATION;
            }
            this.username = Constants.USER_DIRECT_PATH;
            this.authenticated = true;
            LOGGER.debug("Direct location detected. Path loaded.");
        } else {
            // Loading username
            String[] cmd = { File.separator + "bin" + File.separator + "sh",
                "-c",
                "echo ~" + username };
            // Execute command
            try {
                String userHome =
                    new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec(cmd).getInputStream()))
                        .readLine();
                if (userHome != null) {
                    if (!userHome.isEmpty()) {
                        if (userHome.startsWith(File.separator)) {
                            this.compssBaseLog = userHome + RELATIVE_LOG_LOCATION;
                            this.authenticated = true;
                            LOGGER.debug(this.username + "user credentials loaded");
                        } else {
                            LOGGER.error("Defined user " + this.username + "is not available.");
                            return false;
                        }
                    } else {
                        LOGGER.error("Defined user " + this.username + "is not available.");
                        return false;
                    }
                } else {
                    LOGGER.error("Defined user " + this.username + "is not available.");
                    return false;
                }
            } catch (IOException e) {
                // The specified user is not available
                LOGGER.error("Defined user " + this.username + "is not available.");
                return false;
            }
        }

        LOGGER.info("User credentails loaded: " + this.username + " " + this.compssBaseLog);
        return true;
    }

    public boolean isAuthenticated() {
        return this.authenticated;
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Returns the COMPSs base log directory.
     * 
     * @return The COMPSs base log directory.
     */
    public String getCompssBaseLog() {
        return this.compssBaseLog;
    }

    /**
     * Sets a new COMPSs base log directory.
     * 
     * @param compssBaseLog The new COMPSs base log directory.
     */
    public void setCompssBaseLog(String compssBaseLog) {
        this.compssBaseLog = compssBaseLog;
    }

    public Application getMonitoredApp() {
        return this.monitoredApp;
    }

    public void setMonitoredApp(Application monitoredApp) {
        this.monitoredApp = monitoredApp;
    }

    private void loadDefaultOrEnvironmentUser() {
        if (System.getenv("COMPSS_MONITOR") == null) {
            LOGGER.debug("Loading default user");
            this.username = Constants.USER_DEFAULT;
        } else {
            LOGGER.debug("Loading environment user");
            this.username = Constants.USER_ENVIRONMENT;
        }
    }

    private void loadDefaultOrEnvironmentLogFolder() {
        if (System.getenv("COMPSS_MONITOR") == null) {
            LOGGER.debug("Loading default user");
            this.compssBaseLog = System.getProperty("user.home") + RELATIVE_LOG_LOCATION;
        } else {
            LOGGER.debug("Loading environment user");
            this.compssBaseLog = System.getenv("COMPSS_MONITOR");
        }
    }

}
