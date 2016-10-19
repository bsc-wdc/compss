package com.bsc.compss.ui.auth;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.bsc.compss.ui.Constants;
import com.bsc.compss.commons.Loggers;
import com.bsc.compss.ui.Application;


public class UserCredential {

    private String username;
    private String COMPSs_BASE_LOG;
    private Application monitoredApp;
    private boolean authenticated = false;

    private static final String RELATIVE_LOG_LOCATION = File.separator + ".COMPSs" + File.separator;
    private static final Logger logger = LogManager.getLogger(Loggers.UI_AUTHENTICATION);


    public UserCredential() {
        this.username = "";
        this.COMPSs_BASE_LOG = "";
        this.monitoredApp = new Application();
    }

    public UserCredential(String username) {
        logger.debug("Creating user credentials...");
        if (username != null) {
            if (!username.isEmpty()) {
                this.username = username;
            } else {
                loadDefaultOrEnvironmentUser();
                this.COMPSs_BASE_LOG = "";
                this.monitoredApp = new Application();
            }
        } else {
            loadDefaultOrEnvironmentUser();
            this.COMPSs_BASE_LOG = "";
            this.monitoredApp = new Application();
        }
        logger.info("User credentails loaded: " + this.username);
    }

    public boolean setAuthenticated() {
        logger.debug("Verifying user credentials...");
        if (this.username.equals(Constants.USER_DEFAULT)) {
            loadDefaultOrEnvironmentLogFolder();
            this.authenticated = true;
            logger.debug(Constants.USER_DEFAULT + "user credentials loaded");
        } else if (this.username.equals(Constants.USER_ENVIRONMENT)) {
            loadDefaultOrEnvironmentLogFolder();
            this.authenticated = true;
            logger.debug(Constants.USER_ENVIRONMENT + "user credentials loaded");
        } else if (this.username.startsWith(File.separator)) {
            // Loading direct folder without user
            if (this.username.endsWith(".COMPSs") || this.username.endsWith(".COMPSs" + File.separator)) {
                this.COMPSs_BASE_LOG = this.username;
            } else {
                this.COMPSs_BASE_LOG = this.username + RELATIVE_LOG_LOCATION;
            }
            this.username = Constants.USER_DIRECT_PATH;
            this.authenticated = true;
            logger.debug("Direct location detected. Path loaded.");
        } else {
            // Loading username
            String[] cmd = { File.separator + "bin" + File.separator + "sh", "-c", "echo ~" + username };
            // Execute command
            try {
                String userHome = new BufferedReader(new InputStreamReader(Runtime.getRuntime().exec(cmd).getInputStream())).readLine();
                if (userHome != null) {
                    if (!userHome.isEmpty()) {
                        if (userHome.startsWith(File.separator)) {
                            this.COMPSs_BASE_LOG = userHome + RELATIVE_LOG_LOCATION;
                            this.authenticated = true;
                            logger.debug(this.username + "user credentials loaded");
                        } else {
                            logger.error("Defined user " + this.username + "is not available.");
                            return false;
                        }
                    } else {
                        logger.error("Defined user " + this.username + "is not available.");
                        return false;
                    }
                } else {
                    logger.error("Defined user " + this.username + "is not available.");
                    return false;
                }
            } catch (IOException e) {
                // The specified user is not available
                logger.error("Defined user " + this.username + "is not available.");
                return false;
            }
        }

        logger.info("User credentails loaded: " + this.username + " " + this.COMPSs_BASE_LOG);
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

    public String getCOMPSs_BASE_LOG() {
        return this.COMPSs_BASE_LOG;
    }

    public void setCOMPSs_BASE_LOG(String compss_base_log) {
        this.COMPSs_BASE_LOG = compss_base_log;
    }

    public Application getMonitoredApp() {
        return this.monitoredApp;
    }

    public void setMonitoredApp(Application monitoredApp) {
        this.monitoredApp = monitoredApp;
    }

    private void loadDefaultOrEnvironmentUser() {
        if (System.getenv("IT_MONITOR") == null) {
            logger.debug("Loading default user");
            this.username = Constants.USER_DEFAULT;
        } else {
            logger.debug("Loading environment user");
            this.username = Constants.USER_ENVIRONMENT;
        }
    }

    private void loadDefaultOrEnvironmentLogFolder() {
        if (System.getenv("IT_MONITOR") == null) {
            logger.debug("Loading default user");
            this.COMPSs_BASE_LOG = System.getProperty("user.home") + RELATIVE_LOG_LOCATION;
        } else {
            logger.debug("Loading environment user");
            this.COMPSs_BASE_LOG = System.getenv("IT_MONITOR");
        }
    }

}
