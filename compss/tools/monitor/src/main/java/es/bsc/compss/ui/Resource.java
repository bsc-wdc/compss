package es.bsc.compss.ui;

import java.io.File;


public class Resource {

    private String color;

    private String name;
    private String totalCPUComputingUnits;
    private String totalGPUComputingUnits;
    private String totalFPGAComputingUnits;
    private String totalOTHERComputingUnits;
    private String memorySize;
    private String diskSize;
    private String status;
    private String provider;
    private String image;
    private String runningActions;


    public Resource() {
        this.setName(""); // Any
        this.setTotalCPUComputingUnits("0");
        this.setTotalGPUComputingUnits("0");
        this.setTotalFPGAComputingUnits("0");
        this.setTotalOTHERComputingUnits("0");
        this.setMemorySize("0.0"); // Float MB/GB
        this.setDiskSize("0.0"); // Float MB/GB
        this.setProvider(""); // Any
        this.setImage(""); // Any
        this.setStatus(""); // CONST_VALUES
        this.setRunningActions(""); // Any
    }

    public Resource(String[] data) {
        /*
         * Each entry in the new Resource data is of the form: workerName totalCPUu totalGPUu totalFPGAu totalOTHERu
         * memory disk status provider image actions
         */
        this.setName(data[0]);
        this.setTotalCPUComputingUnits(data[1]);
        this.setTotalGPUComputingUnits(data[2]);
        this.setTotalFPGAComputingUnits(data[3]);
        this.setTotalOTHERComputingUnits(data[4]);
        this.setMemorySize(data[5]);
        this.setDiskSize(data[6]);
        this.setStatus(data[7]);
        this.setProvider(data[8]);
        this.setImage(data[9]);
        this.setRunningActions(data[10]);
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        if (status.equals(Constants.STATUS_RUNNING)) {
            this.status = status;
            this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_RUNNING + ".jpg");
        } else if (status.equals(Constants.STATUS_CREATION)) {
            this.status = status;
            this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_CREATION + ".jpg");
        } else if (status.equals(Constants.STATUS_REMOVING)) {
            this.status = status;
            this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_REMOVING + ".jpg");
        } else {
            // Default value for error
            this.status = Constants.STATUS_REMOVING;
            this.setColor(File.separator + "images" + File.separator + "state" + File.separator + Constants.COLOR_REMOVING + ".jpg");
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTotalCPUComputingUnits() {
        return totalCPUComputingUnits;
    }

    public String getTotalGPUComputingUnits() {
        return totalGPUComputingUnits;
    }

    public String getTotalFPGAComputingUnits() {
        return totalFPGAComputingUnits;
    }

    public String getTotalOTHERComputingUnits() {
        return totalOTHERComputingUnits;
    }

    public void setTotalCPUComputingUnits(String totalCPUComputingUnits) {
        this.totalCPUComputingUnits = totalCPUComputingUnits;
    }

    public void setTotalGPUComputingUnits(String totalGPUComputingUnits) {
        this.totalGPUComputingUnits = totalGPUComputingUnits;
    }

    public void setTotalFPGAComputingUnits(String totalFPGAComputingUnits) {
        this.totalFPGAComputingUnits = totalFPGAComputingUnits;
    }

    public void setTotalOTHERComputingUnits(String totalOTHERComputingUnits) {
        this.totalOTHERComputingUnits = totalOTHERComputingUnits;
    }

    public String getMemorySize() {
        return memorySize;
    }

    public void setMemorySize(String memorySize) {
        this.memorySize = memorySize;
    }

    public String getDiskSize() {
        return diskSize;
    }

    public void setDiskSize(String diskSize) {
        this.diskSize = diskSize;
    }

    public String getProvider() {
        return this.provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public String getImage() {
        return this.image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getRunningActions() {
        return runningActions;
    }

    public void setRunningActions(String runningActions) {
        this.runningActions = runningActions;
    }

    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

}
