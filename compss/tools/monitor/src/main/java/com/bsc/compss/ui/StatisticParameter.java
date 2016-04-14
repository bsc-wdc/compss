package com.bsc.compss.ui;


public class StatisticParameter {
    private String name;
    private String value;
    private String defaultValue;
    
    
    public StatisticParameter() {   	
    	this.setName("");				//Any
    	this.setValue("");				//Any
    	this.setDefaultValue("");		//Any
    }
    
    public StatisticParameter(String name, String value) {
		this.setName(name);
		this.setValue(value);
		this.setDefaultValue(value);
    }
    
    public StatisticParameter(String name, String value, String defaultValue) {
		this.setName(name);
		this.setValue(value);
		this.setDefaultValue(defaultValue);
    }

	public String getName() {
		return this.name;
	}
	
	public void setName (String name) {
		this.name = name;
	}
	
	public String getValue () {
		return this.value;
	}
	
	public void setValue (String value) {
		this.value = value;
	}
	
	public String getDefaultValue() {
		return this.defaultValue;
	}

	public void setDefaultValue (String defaultValue) {
		this.defaultValue = defaultValue;
	}
	
	public void reset() {
		this.value = this.defaultValue;
	}
}
