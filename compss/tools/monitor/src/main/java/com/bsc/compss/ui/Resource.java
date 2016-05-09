package com.bsc.compss.ui;

import java.io.File;


public class Resource {
	private String color;
	
    private String name;
    private String computingUnits;
    private String memorySize;
    private String diskSize;
    private String provider;
    private String image;
    private String status;
    private String runningActions;
    
    
    public Resource() {   	
    	this.setName("");				//Any
    	this.setComputingUnits("0");	//Int
    	this.setMemorySize("0.0");		//Float MB/GB
    	this.setDiskSize("0.0");		//Float MB/GB
    	this.setProvider("");			//Any
    	this.setImage("");				//Any
    	this.setStatus("");				//CONST_VALUES
    	this.setRunningActions("");		//Any
    }
    
    public Resource(String[] data) {
		/* Each data has the following structure (from parser)
		 *   Position:   0   1    2    3      4     5       6     7    
		 *   Value:    Name CU Memory Disk Provider Image Status Actions
		 */
    	this.setName(data[0]);
    	this.setComputingUnits(data[1]);
    	this.setMemorySize(data[2]);
    	this.setDiskSize(data[3]);
    	this.setProvider(data[4]);
    	this.setImage(data[5]);
    	this.setStatus(data[6]);
    	this.setRunningActions(data[7]);
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
			//Default value for error
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

	public String getComputingUnits() {
		return computingUnits;
	}

	public void setComputingUnits(String computingUnits) {
		this.computingUnits = computingUnits;
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
