package integratedtoolkit.types.resources.configuration;

import java.io.File;

public class MethodConfiguration extends Configuration {

    private String host;
    private String user = "";

    private String installDir = "";
    private String workingDir = "";

    private int totalComputingUnits = 0;

    private String appDir = "";
    private String classpath = "";
    private String pythonpath = "";
    private String libraryPath = "";

    public MethodConfiguration(String adaptorName) {
        super(adaptorName);
    }

    public MethodConfiguration(MethodConfiguration clone) {
        super(clone);
        host = clone.host;
        user = clone.host;

        installDir = clone.installDir;
        workingDir = clone.workingDir;

        totalComputingUnits = clone.totalComputingUnits;

        appDir = clone.appDir;
        classpath = clone.classpath;
        pythonpath = clone.pythonpath;
        libraryPath = clone.libraryPath;
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
