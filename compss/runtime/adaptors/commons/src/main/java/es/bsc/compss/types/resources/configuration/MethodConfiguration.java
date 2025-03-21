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
package es.bsc.compss.types.resources.configuration;

import es.bsc.compss.COMPSsConstants;

import java.io.File;


public class MethodConfiguration extends Configuration {

    private static final String DEPLOYMENT_ID = System.getProperty(COMPSsConstants.DEPLOYMENT_ID);

    private String host;
    private String user = "";

    private String installDir = "";
    private String workingDir = "";
    private String sandboxWorkingDir;

    private int totalComputingUnits = 0;
    private int totalGPUComputingUnits = 0;
    private int totalFPGAComputingUnits = 0;
    private int totalOTHERComputingUnits = 0;

    private String appDir = "";
    private String classpath = "";
    private String pythonpath = "";
    private String libraryPath = "";
    private String envScript = "";
    private String pythonInterpreter = "";

    // Only used for NIO but declared here for connectors
    private int minPort;
    private int maxPort;

    private int spawnerPort;


    /**
     * Creates a new MethodConfiguration for the given adaptor name.
     * 
     * @param adaptorName Associated adaptor.
     */
    public MethodConfiguration(String adaptorName) {
        super(adaptorName);
    }

    /**
     * Method Configuration constructors cloning an existing configuration.
     * 
     * @param clone Configuration to clone.
     */
    public MethodConfiguration(MethodConfiguration clone) {
        super(clone);
        this.host = clone.host;
        this.user = clone.user;

        this.installDir = clone.installDir;
        this.workingDir = clone.workingDir;
        this.sandboxWorkingDir = clone.sandboxWorkingDir;

        this.totalComputingUnits = clone.totalComputingUnits;
        this.totalGPUComputingUnits = clone.totalGPUComputingUnits;
        this.totalFPGAComputingUnits = clone.totalFPGAComputingUnits;
        this.totalOTHERComputingUnits = clone.totalOTHERComputingUnits;

        this.appDir = clone.appDir;
        this.classpath = clone.classpath;
        this.pythonpath = clone.pythonpath;
        this.libraryPath = clone.libraryPath;
        this.envScript = clone.envScript;
        this.pythonInterpreter = clone.pythonInterpreter;

        this.minPort = clone.minPort;
        this.maxPort = clone.maxPort;

        this.spawnerPort = clone.spawnerPort;
    }

    /**
     * Copies the current MethodConfiguration.
     * 
     * @return A copy of the current MethodConfiguration.
     */
    public MethodConfiguration copy() {
        return new MethodConfiguration(this);
    }

    /**
     * Returns the host name.
     * 
     * @return The host name.
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Sets a new host name.
     * 
     * @param host New host name.
     */
    public void setHost(String host) {
        if (host != null) {
            this.host = host;
        } else {
            this.host = "";
        }
        String newHost = this.getHost().replace("/", "_").replace(":", "_"); // Replace nasty characters
        String sandboxWorkingDir = this.getWorkingDir() + DEPLOYMENT_ID + File.separator + newHost + File.separator;
        this.setSandboxWorkingDir(sandboxWorkingDir);
    }

    /**
     * Returns the user name.
     * 
     * @return The user name.
     */
    public String getUser() {
        return this.user;
    }

    /**
     * Set the user name.
     * 
     * @param user New user name.
     */
    public void setUser(String user) {
        if (user != null) {
            this.user = user;
        } else {
            this.user = "";
        }
    }

    /**
     * Returns the installation directory.
     * 
     * @return The installation directory.
     */
    public String getInstallDir() {
        return this.installDir;
    }

    /**
     * Set the installation directory.
     * 
     * @param installDir Installation directory path
     */
    public void setInstallDir(String installDir) {
        if (installDir == null) {
            this.installDir = "";
        } else if (installDir.isEmpty()) {
            this.installDir = "";
        } else if (!installDir.endsWith(File.separator)) {
            this.installDir = installDir + File.separator;
        } else {
            this.installDir = installDir;
        }
    }

    /**
     * Returns the sandboxed working directory.
     * 
     * @return The sandboxed working directory.
     */
    public String getSandboxWorkingDir() {
        return this.sandboxWorkingDir;
    }

    /**
     * Sets a new sandbox for the working directory.
     * 
     * @param sandboxWorkingDir New sandbox for the working directory.
     */
    public void setSandboxWorkingDir(String sandboxWorkingDir) {
        this.sandboxWorkingDir = sandboxWorkingDir;
    }

    /**
     * Returns the working directory.
     * 
     * @return The working directory.
     */
    public String getWorkingDir() {
        return this.workingDir;
    }

    /**
     * Set the working directory.
     * 
     * @param workingDir Working directory path.
     */
    public void setWorkingDir(String workingDir) {
        if (workingDir == null) {
            // No working dir specified in the project file. Using default tmp
            this.workingDir = File.separator + "tmp" + File.separator;
        } else if (workingDir.isEmpty()) {
            // No working dir specified in the project file. Using default tmp
            this.workingDir = File.separator + "tmp" + File.separator;
        } else if (!workingDir.endsWith(File.separator)) {
            this.workingDir = workingDir + File.separator;
        } else {
            this.workingDir = workingDir;
        }
        String host = this.getHost().replace("/", "_").replace(":", "_"); // Replace nasty characters
        String sandboxWorkingDir = this.workingDir + DEPLOYMENT_ID + File.separator + host + File.separator;
        this.setSandboxWorkingDir(sandboxWorkingDir);
    }

    /**
     * Returns the total number of CPU computing units.
     * 
     * @return The total number of CPU computing units.
     */
    public int getTotalComputingUnits() {
        return this.totalComputingUnits;
    }

    /**
     * Sets a new total of CPU computing units.
     * 
     * @param totalCUs New total of CPU computing units
     */
    public void setTotalComputingUnits(int totalCUs) {
        if (totalCUs > 0) {
            this.totalComputingUnits = totalCUs;
        } else {
            this.totalComputingUnits = 0;
        }
    }

    /**
     * Returns the total number of GPU computing units.
     * 
     * @return The total number of GPU computing units.
     */
    public int getTotalGPUComputingUnits() {
        return this.totalGPUComputingUnits;
    }

    /**
     * Set total GPU computing units.
     * 
     * @param totalGPUs Total GPU computing units.
     */
    public void setTotalGPUComputingUnits(int totalGPUs) {
        if (totalGPUs > 0) {
            this.totalGPUComputingUnits = totalGPUs;
        } else {
            this.totalGPUComputingUnits = 0;
        }
    }

    /**
     * Returns the total number of FPGA computing units.
     * 
     * @return The total number of FPGA computing units.
     */
    public int getTotalFPGAComputingUnits() {
        return this.totalFPGAComputingUnits;
    }

    /**
     * Set total FPGA computing units.
     * 
     * @param totalFPGAs Total FPGA computing units
     */
    public void setTotalFPGAComputingUnits(int totalFPGAs) {
        if (totalFPGAs > 0) {
            this.totalFPGAComputingUnits = totalFPGAs;
        } else {
            this.totalFPGAComputingUnits = 0;
        }
    }

    /**
     * Returns the total number of OTHER computing units.
     * 
     * @return The total number of OTHER computing units.
     */
    public int getTotalOTHERComputingUnits() {
        return this.totalOTHERComputingUnits;
    }

    /**
     * Set total OTHER computing units.
     * 
     * @param totalOTHERs Total OTHER computing units
     */
    public void setTotalOTHERComputingUnits(int totalOTHERs) {
        if (totalOTHERs > 0) {
            this.totalOTHERComputingUnits = totalOTHERs;
        } else {
            this.totalOTHERComputingUnits = 0;
        }
    }

    /**
     * Returns the application directory path.
     * 
     * @return The application directory path.
     */
    public String getAppDir() {
        return this.appDir;
    }

    /**
     * Set the application location directory.
     * 
     * @param appDir Application directory path
     */
    public void setAppDir(String appDir) {
        if (appDir == null || appDir.isEmpty()) {
            this.appDir = "";
        } else if (!appDir.endsWith(File.separator)) {
            this.appDir = appDir + File.separator;
        } else {
            this.appDir = appDir;
        }
    }

    /**
     * Returns the classpath.
     * 
     * @return The classpath.
     */
    public String getClasspath() {
        return this.classpath;
    }

    /**
     * Set the application required classpath.
     * 
     * @param classpath Application classpath
     */
    public void setClasspath(String classpath) {
        if (classpath == null) {
            this.classpath = "";
        } else {
            this.classpath = classpath;
        }
    }

    /**
     * Returns the application's pythonpath.
     * 
     * @return The application's pythonpath.
     */
    public String getPythonpath() {
        return this.pythonpath;
    }

    /**
     * Set the application required pythonpath.
     * 
     * @param pythonpath Application pythonpath
     */
    public void setPythonpath(String pythonpath) {
        if (pythonpath == null) {
            this.pythonpath = "";
        } else {
            this.pythonpath = pythonpath;
        }
    }

    /**
     * Returns the application's library path.
     * 
     * @return The application's library path.
     */
    public String getLibraryPath() {
        return this.libraryPath;
    }

    /**
     * Set the application required library path.
     * 
     * @param libraryPath Application library path
     */
    public void setLibraryPath(String libraryPath) {
        if (libraryPath == null) {
            this.libraryPath = "";
        } else {
            this.libraryPath = libraryPath;
        }
    }

    /**
     * Get the application's environment script path.
     * 
     * @return The application's library path.
     */
    public String getEnvScript() {
        return this.envScript;
    }

    /**
     * Set the application required environment script path.
     * 
     * @param envScriptPath Application environment script path.
     */
    public void setEnvScript(String envScriptPath) {
        if (envScriptPath == null) {
            this.envScript = "";
        } else {
            this.envScript = envScriptPath;
        }
    }

    /**
     * Get the application's Python Interpreter.
     * 
     * @return The application's Python Interpreter.
     */
    public String getPythonInterpreter() {
        return this.pythonInterpreter;
    }

    /**
     * Set the application required Python Interpreter.
     * 
     * @param pythonInterpreter Application Python Interpreter.
     */
    public void setPythonInterpreter(String pythonInterpreter) {
        if (pythonInterpreter == null) {
            this.pythonInterpreter = "";
        } else {
            this.pythonInterpreter = pythonInterpreter;
        }
    }

    /**
     * Returns the minimum port.
     * 
     * @return The minimum port.
     */
    public int getMinPort() {
        return this.minPort;
    }

    /**
     * Sets a new value for the minimum port.
     * 
     * @param minPort New minimum port.
     */
    public void setMinPort(int minPort) {
        this.minPort = minPort;
    }

    /**
     * Returns the maximum port.
     * 
     * @return The maximum port.
     */
    public int getMaxPort() {
        return this.maxPort;
    }

    /**
     * Sets a new value for the maximum port.
     * 
     * @param maxPort New maximum port.
     */
    public void setMaxPort(int maxPort) {
        this.maxPort = maxPort;
    }

    /**
     * Returns the ssh spawner port.
     * 
     * @return The spawner port.
     */
    public int getSpawnerPort() {
        return this.spawnerPort;
    }

    /**
     * Sets a new value for the spawner port.
     * 
     * @param spawnerPort New spawner port.
     */
    public void setSpawnerPort(int spawnerPort) {
        this.spawnerPort = spawnerPort;
    }

}
