package com.bsc.compss.ui;

import java.io.File;


public class Resource {
	private String color;
	
    private String name;
    private String cpuCount;
    private String coreCount;
    private String memorySize;
    private String dataSize;
    private String provider;
    private String image;
    private String status;
    private String runningTasks;
    
    
    public Resource() {   	
    	this.setName("");				//Any
    	this.setCpuCount("0");			//Int
    	this.setCoreCount("0");			//Int
    	this.setMemorySize("0.0");		//Float MB/GB
    	this.setDataSize("0.0");		//Float MB/GB
    	this.setProvider("");			//Any
    	this.setImage("");				//Any
    	this.setStatus("");				//CONST_VALUES
    	this.setRunningTasks("");		//Any
    }
    
    public Resource(String[] data) {
		/* Each data has the following structure (from parser)
		 *   Position:   0   1    2    3      4     5       6     7      8
		 *   Value:    Name CPU Core Memory Disk Provider Image Status Tasks
		 */
    	this.setName(data[0]);
    	this.setCpuCount(data[1]);
    	this.setCoreCount(data[2]);
    	this.setMemorySize(data[3]);
    	this.setDataSize(data[4]);
    	this.setProvider(data[5]);
    	this.setImage(data[6]);
    	this.setStatus(data[7]);
    	this.setRunningTasks(data[8]);
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

	public String getCpuCount() {
		return cpuCount;
	}

	public void setCpuCount(String cpuCount) {
		this.cpuCount = cpuCount;
	}
	
	public String getCoreCount() {
		return coreCount;
	}

	public void setCoreCount(String coreCount) {
		this.coreCount = coreCount;
	}

	public String getMemorySize() {
		return memorySize;
	}

	public void setMemorySize(String memorySize) {
		this.memorySize = memorySize;
	}

	public String getDataSize() {
		return dataSize;
	}

	public void setDataSize(String dataSize) {
		this.dataSize = dataSize;
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

	public String getRunningTasks() {
		return runningTasks;
	}

	public void setRunningTasks(String runningTasks) {
		this.runningTasks = runningTasks;
	}

	public String getColor() {
		return color;
	}

	public void setColor(String color) {
		this.color = color;
	}
}
