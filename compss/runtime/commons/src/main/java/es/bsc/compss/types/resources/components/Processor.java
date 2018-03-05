package es.bsc.compss.types.resources.components;

import java.io.Serializable;

import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.resources.MethodResourceDescription;


public class Processor implements Serializable {

    /**
     * Runtime Objects have serialization ID 1L
     */
    private static final long serialVersionUID = 1L;
    
    private String name = MethodResourceDescription.UNASSIGNED_STR;
    private int computingUnits = MethodResourceDescription.ZERO_INT;
    private float speed = MethodResourceDescription.UNASSIGNED_FLOAT;
    private String architecture = MethodResourceDescription.UNASSIGNED_STR;
    private String type = Constants.CPU_TYPE;
    private float internalMemory = MethodResourceDescription.UNASSIGNED_FLOAT;
	private String propName = MethodResourceDescription.UNASSIGNED_STR;
    private String propValue = MethodResourceDescription.UNASSIGNED_STR;
    
    //This flag is to know if a processor has been created by default and not updated
    private boolean modified = false;

    public Processor() {
    	
    }

    public Processor(String name) {
        this.setName(name);
    }

    public Processor(String name, int cu) {
        this.setName(name);
        this.setComputingUnits(cu);
    }

    public Processor(String name, int cu, float speed) {
        this.setName(name);
        this.setComputingUnits(cu);
        this.setSpeed(speed);
    }

    public Processor(String name, int cu, float speed, String arch) {
        this.setName(name);
        this.setComputingUnits(cu);
        this.setSpeed(speed);
        this.setArchitecture(arch);
    }

    public Processor(String name, int cu, float speed, String arch, String type, float internalMem, String propName, String propValue) {
        this.setName(name);
        this.setComputingUnits(cu);
        this.setSpeed(speed);
        this.setArchitecture(arch);
        this.setType(type);
        this.setInternalMemory(internalMem);
        this.setPropName(propName);
        this.setPropValue(propValue);
    }

    public Processor(String name, int cu, String type, float internalMem, String propName, String propValue) {
        this.setName(name);
        this.setComputingUnits(cu);
        this.setType(type);
        this.setInternalMemory(internalMem);
        this.setPropName(propName);
        this.setPropValue(propValue);
    }

    public Processor(Processor p) {
        this.setName(p.getName());
        this.setComputingUnits(p.getComputingUnits());
        this.setSpeed(p.getSpeed());
        this.setArchitecture(p.getArchitecture());
        this.setType(p.getType());
        this.setInternalMemory(p.getInternalMemory());
        this.setPropName(p.getPropName());
        this.setPropValue(p.getPropValue());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        modified = true;
    	this.name = name;
    }

    public int getComputingUnits() {
        return computingUnits;
    }

    public void setComputingUnits(int computingUnits) {
    	modified = true;
    	this.computingUnits = computingUnits;
    }

    public void addComputingUnits(int cu) {
        this.computingUnits = this.computingUnits + cu;
    }

    public void removeComputingUnits(int cu) {
    	modified = true;
    	this.computingUnits = this.computingUnits - cu;
    }

    public void multiply(int amount) {
        this.computingUnits = this.computingUnits * amount;
    }

    public float getSpeed() {
        return speed;
    }

    public void setSpeed(float speed) {
    	modified = true;
    	this.speed = speed;
    }

    public String getArchitecture() {
        return architecture;
    }

    public void setArchitecture(String architecture) {
    	modified = true;
    	this.architecture = architecture;
    }
    
    /**
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		modified = true;
		this.type = type;
	}

	/**
	 * @return the internalMemory
	 */
	public float getInternalMemory() {
		return internalMemory;
	}

	/**
	 * @param internalMemory the internalMemory to set
	 */
	public void setInternalMemory(float internalMemory) {
		modified = true;
		this.internalMemory = internalMemory;
	}

    public String getPropName() {
        return propName;
    }

    public void setPropName(String propName) {
    	modified = true;
    	this.propName = propName;
    }

    public String getPropValue() {
        return propValue;
    }

    public void setPropValue(String propValue) {
        this.propValue = propValue;
    }

	public boolean isCPU() {
		return type.equals(Constants.CPU_TYPE);
	}
	
	public boolean isModified() {
		return modified;
	}
}
