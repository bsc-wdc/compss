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


    public MethodConfiguration(String adaptorName) {
        super(adaptorName);
    }

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
    }

    public MethodConfiguration copy() {
        return new MethodConfiguration(this);
    }

    public String getInstallDir() {
        return installDir;
    }

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

    public String getSandboxWorkingDir() {
        return sandboxWorkingDir;
    }

    public void setSandboxWorkingDir(String sandboxWorkingDir) {
        this.sandboxWorkingDir = sandboxWorkingDir;
    }

    public String getWorkingDir() {
        return workingDir;
    }

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

    public int getTotalComputingUnits() {
        return totalComputingUnits;
    }

    public void setTotalComputingUnits(int totalCUs) {
        if (totalCUs > 0) {
            this.totalComputingUnits = totalCUs;
        } else {
            this.totalComputingUnits = 0;
        }
    }

    public int getTotalGPUComputingUnits() {
        return totalGPUComputingUnits;
    }

    public void setTotalGPUComputingUnits(int totalGPUs) {
        if (totalGPUs > 0) {
            this.totalGPUComputingUnits = totalGPUs;
        } else {
            this.totalGPUComputingUnits = 0;
        }
    }

    public int getTotalFPGAComputingUnits() {
        return totalFPGAComputingUnits;
    }

    public void setTotalFPGAComputingUnits(int totalFPGAs) {
        if (totalFPGAs > 0) {
            this.totalFPGAComputingUnits = totalFPGAs;
        } else {
            this.totalFPGAComputingUnits = 0;
        }
    }

    public int getTotalOTHERComputingUnits() {
        return totalOTHERComputingUnits;
    }

    public void setTotalOTHERComputingUnits(int totalOTHERs) {
        if (totalOTHERs > 0) {
            this.totalOTHERComputingUnits = totalOTHERs;
        } else {
            this.totalOTHERComputingUnits = 0;
        }
    }

    public String getAppDir() {
        return appDir;
    }

    public void setAppDir(String appDir) {
        if (appDir == null) {
            this.appDir = "";
        } else if (appDir.isEmpty()) {
            this.appDir = "";
        } else if (!appDir.endsWith(File.separator)) {
            this.appDir = appDir + File.separator;
        } else {
            this.appDir = appDir;
        }
    }

    public String getClasspath() {
        return classpath;
    }

    public void setClasspath(String classpath) {
        if (classpath == null) {
            this.classpath = "";
        } else {
            this.classpath = classpath;
        }
    }

    public String getPythonpath() {
        return pythonpath;
    }

    public void setPythonpath(String pythonpath) {
        if (pythonpath == null) {
            this.pythonpath = "";
        } else {
            this.pythonpath = pythonpath;
        }
    }

    public String getLibraryPath() {
        return libraryPath;
    }

    public void setLibraryPath(String libraryPath) {
        if (libraryPath == null) {
            this.libraryPath = "";
        } else {
            this.libraryPath = libraryPath;
        }
    }

    public String getHost() {
        return host;
    }

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

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        if (user != null) {
            this.user = user;
        } else {
            this.user = "";
        }
    }

    // For JClouds connector
    public int getMaxPort() {
        return -1;
    }

    public int getMinPort() {
        return -1;
    }

}
