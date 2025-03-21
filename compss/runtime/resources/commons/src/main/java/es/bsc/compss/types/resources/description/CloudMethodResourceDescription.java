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
package es.bsc.compss.types.resources.description;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class CloudMethodResourceDescription extends MethodResourceDescription {

    public static final CloudMethodResourceDescription EMPTY = new CloudMethodResourceDescription();

    // Resource Description
    private String name;
    private final Map<CloudInstanceTypeDescription, int[]> typeComposition;
    private CloudImageDescription image;


    /**
     * Creates a new empty CloudMethodResourceDescription.
     */
    public CloudMethodResourceDescription() {
        super();

        this.name = "";
        this.typeComposition = new HashMap<>();
        this.image = null;
    }

    /**
     * Creates a new CloudMethodResourceDescription from the given constraints.
     * 
     * @param constraints Constraints.
     */
    public CloudMethodResourceDescription(Constraints constraints) {
        super(constraints);

        this.name = "";
        this.typeComposition = new HashMap<>();
        this.image = null;
    }

    /**
     * Creates a new CloudMethodResourceDescription from the given constraints.
     * 
     * @param constraints Constraints.
     */
    public CloudMethodResourceDescription(MethodResourceDescription constraints) {
        super(constraints);

        this.name = "";
        this.typeComposition = new HashMap<>();
        this.image = null;
    }

    /**
     * Clones the given CloudMethodResourceDescription.
     * 
     * @param clone CloudMethodResourceDescription to clone.
     */
    public CloudMethodResourceDescription(CloudMethodResourceDescription clone) {
        super(clone);

        this.name = clone.name;
        this.typeComposition = new HashMap<>();
        for (Entry<CloudInstanceTypeDescription, int[]> entry : clone.typeComposition.entrySet()) {
            this.typeComposition.put(entry.getKey(), new int[] { entry.getValue()[0] });
        }
        this.image = clone.image;
    }

    /**
     * Creates a new CloudMethodResourceDescription from the given cloud instance type and image.
     * 
     * @param type Instance type.
     * @param image Image.
     */
    public CloudMethodResourceDescription(CloudInstanceTypeDescription type, CloudImageDescription image) {
        super(type.getResourceDescription());

        this.name = "";
        this.typeComposition = new HashMap<>();
        this.typeComposition.put(type, new int[] { 1 });
        this.image = image;
    }

    @Override
    public CloudMethodResourceDescription copy() {
        return new CloudMethodResourceDescription(this);
    }

    /**
     * Returns the CloudMethodResourceDescription name.
     * 
     * @return
     */
    public String getName() {
        return this.name;
    }

    /**
     * Sets a new name for the current CloudMethodResourceDescription.
     * 
     * @param name New name.
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Returns the associated image.
     * 
     * @return The associated cloud image.
     */
    public CloudImageDescription getImage() {
        return this.image;
    }

    /**
     * Sets a new associated cloud image.
     * 
     * @param image New cloud image.
     */
    public void setImage(CloudImageDescription image) {
        this.image = image;
    }

    /**
     * Returns the types composition.
     * 
     * @return The types composition.
     */
    public Map<CloudInstanceTypeDescription, int[]> getTypeComposition() {
        return this.typeComposition;
    }

    /**
     * Adds {@code count} slots of the given type {@code type} to the type composition.
     * 
     * @param type Instance type.
     * @param count Number of count slots to add.
     */
    public void addInstances(CloudInstanceTypeDescription type, int count) {
        int[] counts = this.typeComposition.get(type);
        if (counts != null) {
            counts[0] += count;
        } else {
            this.typeComposition.put(type, new int[] { count });
        }
    }

    /**
     * Adds a new instance type to the type composition with a single slot.
     * 
     * @param type Instance type.
     */
    public void addInstance(CloudInstanceTypeDescription type) {
        int[] counts = this.typeComposition.get(type);
        if (counts != null) {
            counts[0]++;
        } else {
            this.typeComposition.put(type, new int[] { 1 });
        }
    }

    @Override
    public void increase(ResourceDescription cmrd) {
        super.increase(cmrd);
        for (Entry<CloudInstanceTypeDescription, int[]> typeCount : ((CloudMethodResourceDescription) cmrd)
            .getTypeComposition().entrySet()) {
            CloudInstanceTypeDescription type = typeCount.getKey();
            int[] count = typeCount.getValue();
            addInstances(type, count[0]);
        }
    }

    /**
     * Returns a list of the possible reductions.
     * 
     * @return A list fo the possible reductions.
     */
    public List<CloudInstanceTypeDescription> getPossibleReductions() {
        List<CloudInstanceTypeDescription> reductions = new LinkedList<>();
        for (CloudInstanceTypeDescription type : this.typeComposition.keySet()) {
            reductions.add(type);
        }
        return reductions;
    }

    /**
     * Removes the given instance type.
     * 
     * @param type Instance type to remove.
     */
    public void removeInstance(CloudInstanceTypeDescription type) {
        int[] counts = this.typeComposition.get(type);
        if (counts != null) {
            counts[0]--;
            if (counts[0] == 0) {
                this.typeComposition.remove(type);
            }
        }
    }

    /**
     * Remove the {@code count} slots from the given instance type {@code type}.
     * 
     * @param type Instance type.
     * @param count Number of slots to remove.
     */
    public void removeInstances(CloudInstanceTypeDescription type, int count) {
        int[] counts = this.typeComposition.get(type);
        if (counts != null) {
            counts[0] -= count;
            if (counts[0] < 1) {
                this.typeComposition.remove(type);
            }
        }
    }

    @Override
    public void reduce(ResourceDescription cmrd) {
        super.reduce(cmrd);
        for (Entry<CloudInstanceTypeDescription, int[]> typeCount : ((CloudMethodResourceDescription) cmrd)
            .getTypeComposition().entrySet()) {
            CloudInstanceTypeDescription type = typeCount.getKey();
            int[] count = typeCount.getValue();
            removeInstances(type, count[0]);
        }
    }

    @Override
    protected void dumpContent(StringBuilder sb) {
        super.dumpContent(sb);
        sb.append("\"cloud\":{");
        sb.append("\"image\":").append((this.image == null) ? "NULL" : "\"" + this.image.getImageName() + "\"")
            .append(",");
        sb.append("\"type_composition\":{");
        Iterator<Entry<CloudInstanceTypeDescription, int[]>> entries = this.typeComposition.entrySet().iterator();
        Entry<CloudInstanceTypeDescription, int[]> entry;
        if (entries.hasNext()) {
            entry = entries.next();
            sb.append("\"").append(entry.getKey().getName()).append("\":").append(entry.getValue()[0]);
        }
        while (entries.hasNext()) {
            sb.append(",");
            entry = entries.next();
            sb.append("\"").append(entry.getKey().getName()).append("\":").append(entry.getValue()[0]);
        }
        sb.append("}}");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        this.dumpContent(sb);
        sb.append("}");

        return sb.toString();
    }

    /**
     * Dumps the current state to a string leaded by the given prefix.
     * 
     * @param prefix Indentation prefix.
     * @return String containing a dump of the current state.
     */
    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("\t").append("VIRTUAL_INSTANCE = [").append("\n");
        sb.append(prefix).append("\t").append("\t").append("NAME = ").append(this.name).append("\n");
        sb.append(prefix).append("\t").append("\t").append("COMPONENTS = [").append("\n");
        for (Entry<CloudInstanceTypeDescription, int[]> component : this.typeComposition.entrySet()) {
            String componentName = component.getKey().getName();
            int[] amount = component.getValue();
            sb.append(prefix).append("\t").append("\t").append("\t").append("COMPONENT = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("NAME = ")
                .append(componentName).append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("AMOUNT = ").append(amount[0])
                .append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("\t").append("\t").append("]").append("\n");
        sb.append(prefix).append("\t").append("]").append("\n");
        return sb.toString();
    }

}
