package integratedtoolkit.types.resources;

import integratedtoolkit.types.Implementation;


public abstract class ResourceDescription {

    public ResourceDescription() {
    }

    public ResourceDescription(ResourceDescription rd) {
    }

    public abstract ResourceDescription copy();

    
    public abstract boolean canHost(Implementation<?> impl);

    public abstract void increase(ResourceDescription rd);

    public abstract void reduce(ResourceDescription rd);

    public abstract void increaseDynamic(ResourceDescription rd);

    public abstract void reduceDynamic(ResourceDescription rd);
    
    public abstract ResourceDescription getDynamicCommons(ResourceDescription constraints);

    public abstract boolean isDynamicUseless();

    public static ResourceDescription reduceCommonDynamics(ResourceDescription gapResource, ResourceDescription constraints) {
        ResourceDescription commons = gapResource.getDynamicCommons(constraints);
        gapResource.reduceDynamic(commons);
        constraints.reduceDynamic(commons);
        return commons;
    }

}
