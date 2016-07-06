package integratedtoolkit.types.resources;

import integratedtoolkit.types.Implementation;

public abstract class ResourceDescription {

    public ResourceDescription() {
    }

    public ResourceDescription(ResourceDescription rd) {
    }

    public abstract ResourceDescription copy();

    /**
     * Checks the static constraints for an implementation to be run on a
     * resource
     *
     * @param impl
     * @return
     */
    public abstract boolean canHost(Implementation<?> impl);

    /**
     * Increases the static and dynamic capabilities
     *
     * @param rd
     */
    public abstract void increase(ResourceDescription rd);

    /**
     * Decreases the static and dynamic capabilities
     *
     * @param rd
     */
    public abstract void reduce(ResourceDescription rd);

    /**
     * Checks the dynamic capabilities for an implementation to be run on a
     * resource
     *
     * @param impl
     * @return
     */
    public abstract boolean canHostDynamic(Implementation<?> impl);

    /**
     * Increases the dynamic capabilities
     *
     * @param rd
     */
    public abstract void increaseDynamic(ResourceDescription rd);

    /**
     * Reduces the dynamic capabilities returning the description of the real
     * capabilities that have been reduced
     *
     * @param rd
     * @return
     */
    public abstract ResourceDescription reduceDynamic(ResourceDescription rd);

    /**
     * Return the dynamic common capabilities
     *
     * @param constraints
     * @return
     */
    public abstract ResourceDescription getDynamicCommons(ResourceDescription constraints);

    /**
     * Returns if the resource is fully used or not
     *
     * @return
     */
    public abstract boolean isDynamicUseless();

    /**
     * Reduce only the common dynamic capabilities
     *
     * @param gapResource
     * @param constraints
     * @return
     */
    public static ResourceDescription reduceCommonDynamics(ResourceDescription gapResource, ResourceDescription constraints) {
        ResourceDescription commons = gapResource.getDynamicCommons(constraints);
        gapResource.reduceDynamic(commons);
        constraints.reduceDynamic(commons);
        return commons;
    }

}
