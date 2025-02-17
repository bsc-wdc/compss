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
package es.bsc.compss.types.resources.components;

import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Serializable;


public class Processor implements Serializable {

    /**
     * Enum to match the processor types.
     */
    public static enum ProcessorType {
        CPU, // CPU
        GPU, // GPU
        FPGA, // FPGA
        OTHER // Other
    }


    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    private String name = MethodResourceDescription.UNASSIGNED_STR;
    private int computingUnits = MethodResourceDescription.UNASSIGNED_INT;
    private float speed = MethodResourceDescription.UNASSIGNED_FLOAT;
    private String architecture = MethodResourceDescription.UNASSIGNED_STR;
    private ProcessorType type = ProcessorType.CPU;
    private float internalMemory = MethodResourceDescription.UNASSIGNED_FLOAT;
    private String propName = MethodResourceDescription.UNASSIGNED_STR;
    private String propValue = MethodResourceDescription.UNASSIGNED_STR;

    // This flag is to know if a processor has been created by default and not updated
    private boolean modified = false;


    /**
     * Creates a new Processor instance for serialization.
     */
    public Processor() {
        // For serialization
    }

    /**
     * Creates a new Processor instance with the given information.
     * 
     * @param name Processor name.
     * @param cu Processor computing units.
     * @param speed Processor speed.
     * @param arch Processor architecture.
     * @param type Processor type.
     * @param internalMem Processor memory.
     * @param propName Processor custom property name.
     * @param propValue Processor custom property value.
     */
    public Processor(String name, int cu, float speed, String arch, String type, float internalMem, String propName,
        String propValue) {
        this.setName(name);
        this.setComputingUnits(cu);
        this.setSpeed(speed);
        this.setArchitecture(arch);
        this.setType(type);
        this.setInternalMemory(internalMem);
        this.setPropName(propName);
        this.setPropValue(propValue);
    }

    /**
     * Creates a copy of the given Processor {@code p}.
     * 
     * @param p Processor to copy.
     */
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

    /**
     * Returns the processor name.
     * 
     * @return The processor name.
     */
    public String getName() {
        return this.name;
    }

    /**
     * Sets a new processor name.
     * 
     * @param name The new processor name.
     */
    public void setName(String name) {
        this.modified = true;
        this.name = name;
    }

    /**
     * Returns the number of computing units of the processor.
     * 
     * @return The number of computing units of the processor.
     */
    public int getComputingUnits() {
        return this.computingUnits;
    }

    /**
     * Sets a new number of computing units for the processor.
     * 
     * @param computingUnits The new number of computing units.
     */
    public void setComputingUnits(int computingUnits) {
        this.modified = true;
        this.computingUnits = computingUnits;
    }

    /**
     * Increases the current computing units of the processor by {@code cu} units.
     * 
     * @param cu Additional computing units.
     */
    public void addComputingUnits(int cu) {
        this.computingUnits = this.computingUnits + cu;
    }

    /**
     * Removes {@code cu} units from the current computing units of the processor.
     * 
     * @param cu Number of computing units to remove.
     */
    public void removeComputingUnits(int cu) {
        this.modified = true;
        this.computingUnits = this.computingUnits - cu;
    }

    /**
     * Scales the current computing units by a factor of {@code amount}.
     * 
     * @param amount Computing units scaling factor.
     */
    public void multiply(int amount) {
        this.computingUnits = this.computingUnits * amount;
    }

    /**
     * Returns the processor speed.
     * 
     * @return The processor speed.
     */
    public float getSpeed() {
        return this.speed;
    }

    /**
     * Sets a new speed for the processor.
     * 
     * @param speed New processor speed.
     */
    public void setSpeed(float speed) {
        this.modified = true;
        this.speed = speed;
    }

    /**
     * Returns the processor architecture.
     * 
     * @return The processor architecture.
     */
    public String getArchitecture() {
        return this.architecture;
    }

    /**
     * Sets a new processor architecture.
     * 
     * @param architecture The new processor architecture.
     */
    public void setArchitecture(String architecture) {
        this.modified = true;
        this.architecture = architecture;
    }

    /**
     * Returns the processor type.
     * 
     * @return The processor type.
     */
    public ProcessorType getType() {
        return this.type;
    }

    /**
     * Sets a new processor type.
     * 
     * @param type The name of the new processor type.
     */
    public void setType(String type) {
        this.modified = true;
        this.type = ProcessorType.valueOf(type.toUpperCase());
    }

    /**
     * Sets a new processor type.
     * 
     * @param type The new processor type.
     */
    public void setType(ProcessorType type) {
        this.modified = true;
        this.type = type;
    }

    /**
     * Returns the processor's internal memory.
     * 
     * @return The processor's internal memory.
     */
    public float getInternalMemory() {
        return this.internalMemory;
    }

    /**
     * Sets a new amount of processor's internal memory.
     * 
     * @param internalMemory The new processor's internal memory.
     */
    public void setInternalMemory(float internalMemory) {
        this.modified = true;
        this.internalMemory = internalMemory;
    }

    /**
     * Returns the custom processor's property name.
     * 
     * @return The custom processor's property name.
     */
    public String getPropName() {
        return this.propName;
    }

    /**
     * Sets a new custom processor's property name.
     * 
     * @param propName The new custom processor's property name.
     */
    public void setPropName(String propName) {
        this.modified = true;
        this.propName = propName;
    }

    /**
     * Returns the custom processor's property value.
     * 
     * @return The custom processor's property value.
     */
    public String getPropValue() {
        return this.propValue;
    }

    /**
     * Sets a new custom processor's property value.
     * 
     * @param propValue The new custom processor's property value.
     */
    public void setPropValue(String propValue) {
        this.propValue = propValue;
    }

    /**
     * getComputingUnits() Returns whether the processor is a CPU or not.
     * 
     * @return {@code true} if the processor has type CPU, {@code false} otherwise.
     */
    public boolean isCPU() {
        return this.type.equals(ProcessorType.CPU);
    }

    /**
     * Returns whether the original processor has been modified or not.
     * 
     * @return {@code true} if the original processor instance has been modified, {@code false} otherwise.
     */
    public boolean isModified() {
        return this.modified;
    }

    public boolean hasUnassignedCUs() {
        return this.computingUnits == MethodResourceDescription.UNASSIGNED_INT;
    }

    @Override
    public String toString() {
        return "{" + "\"type\":\"" + this.getType().toString() + "\"," + "\"computing_units\":"
            + this.getComputingUnits() + "," + "\"speed\":" + this.getSpeed() + "," + "\"internal_memory\":"
            + this.getInternalMemory() + "," + "\"architecture\":\"" + this.getArchitecture() + "\","
            + "\"prop_name\":\"" + this.getPropName() + "\"," + "\"prop_value\":\"" + this.getPropValue() + "\"" + "}";
    }
}
