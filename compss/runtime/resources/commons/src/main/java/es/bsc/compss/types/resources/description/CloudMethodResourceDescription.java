package es.bsc.compss.types.resources.description;

import es.bsc.compss.types.annotations.Constraints;
import es.bsc.compss.types.resources.MethodResourceDescription;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


public class CloudMethodResourceDescription extends MethodResourceDescription {

    public static final CloudMethodResourceDescription EMPTY = new CloudMethodResourceDescription();

    // Resource Description
    private String name = "";
    private final Map<CloudInstanceTypeDescription, int[]> typeComposition = new HashMap<>();
    private CloudImageDescription image = null;


    public CloudMethodResourceDescription() {
        super();
    }

    public CloudMethodResourceDescription(Constraints constraints) {
        super(constraints);
    }

    public CloudMethodResourceDescription(MethodResourceDescription constraints) {
        super(constraints);
    }

    public CloudMethodResourceDescription(CloudMethodResourceDescription clone) {
        super(clone);
        name = clone.name;
        for (Entry<CloudInstanceTypeDescription, int[]> entry : clone.typeComposition.entrySet()) {
            typeComposition.put(entry.getKey(), new int[] { entry.getValue()[0] });
        }
        image = clone.image;
    }

    public CloudMethodResourceDescription(CloudInstanceTypeDescription type, CloudImageDescription image) {
        super(type.getResourceDescription());
        typeComposition.put(type, new int[] { 1 });
        this.image = image;
    }

    @Override
    public CloudMethodResourceDescription copy() {
        return new CloudMethodResourceDescription(this);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Map<CloudInstanceTypeDescription, int[]> getTypeComposition() {
        return typeComposition;
    }

    public void addInstances(CloudInstanceTypeDescription type, int count) {
        int[] counts = typeComposition.get(type);
        if (counts != null) {
            counts[0] += count;
        } else {
            typeComposition.put(type, new int[] { count });
        }
    }

    public void addInstance(CloudInstanceTypeDescription type) {
        int[] counts = typeComposition.get(type);
        if (counts != null) {
            counts[0]++;
        } else {
            typeComposition.put(type, new int[] { 1 });
        }
    }

    public void increase(CloudMethodResourceDescription cmrd) {
        super.increase(cmrd);
        for (Entry<CloudInstanceTypeDescription, int[]> typeCount : cmrd.getTypeComposition().entrySet()) {
            CloudInstanceTypeDescription type = typeCount.getKey();
            int[] count = typeCount.getValue();
            addInstances(type, count[0]);
        }
    }

    public List<CloudInstanceTypeDescription> getPossibleReductions() {
        List<CloudInstanceTypeDescription> reductions = new LinkedList<>();
        for (CloudInstanceTypeDescription type : typeComposition.keySet()) {
            reductions.add(type);
        }
        return reductions;
    }

    public void removeInstance(CloudInstanceTypeDescription type) {
        int[] counts = typeComposition.get(type);
        if (counts != null) {
            counts[0]--;
            if (counts[0] == 0) {
                typeComposition.remove(type);
            }
        }
    }

    public void removeInstances(CloudInstanceTypeDescription type, int count) {
        int[] counts = typeComposition.get(type);
        if (counts != null) {
            counts[0] -= count;
            if (counts[0] < 1) {
                typeComposition.remove(type);
            }
        }
    }

    public void reduce(CloudMethodResourceDescription cmrd) {
        super.reduce(cmrd);
        for (Entry<CloudInstanceTypeDescription, int[]> typeCount : cmrd.getTypeComposition().entrySet()) {
            CloudInstanceTypeDescription type = typeCount.getKey();
            int[] count = typeCount.getValue();
            removeInstances(type, count[0]);
        }
    }

    public CloudImageDescription getImage() {
        return image;
    }

    public void setImage(CloudImageDescription image) {
        this.image = image;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("[CLOUD");
        sb.append(" IMAGE=").append((this.image == null) ? "NULL" : this.image.getImageName());
        sb.append(" TYPE_COMPOSITION=[");
        for (Entry<CloudInstanceTypeDescription, int[]> entry : typeComposition.entrySet()) {
            sb.append(" ").append(entry.getKey().getName()).append("=").append(entry.getValue()[0]);
        }
        sb.append("]]");

        return sb.toString();
    }

    public String getCurrentState(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("\t").append("VIRTUAL_INSTANCE = [").append("\n");
        sb.append(prefix).append("\t").append("\t").append("NAME = ").append(name).append("\n");
        sb.append(prefix).append("\t").append("\t").append("COMPONENTS = [").append("\n");
        for (Entry<CloudInstanceTypeDescription, int[]> component : typeComposition.entrySet()) {
            String componentName = component.getKey().getName();
            int[] amount = component.getValue();
            sb.append(prefix).append("\t").append("\t").append("\t").append("COMPONENT = [").append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("NAME = ").append(componentName).append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("\t").append("AMOUNT = ").append(amount[0]).append("\n");
            sb.append(prefix).append("\t").append("\t").append("\t").append("]").append("\n");
        }
        sb.append(prefix).append("\t").append("\t").append("]").append("\n");
        sb.append(prefix).append("\t").append("]").append("\n");
        return sb.toString();
    }

}
