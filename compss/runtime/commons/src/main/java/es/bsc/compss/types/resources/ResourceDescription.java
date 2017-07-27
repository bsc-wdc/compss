package es.bsc.compss.types.resources;

import java.io.Externalizable;

import es.bsc.compss.types.implementations.Implementation;

/**
 * Abstract representation of a Resource
 *
 */
public abstract class ResourceDescription implements Externalizable {

    /**
     * Empty resource Description
     */
    public ResourceDescription() {
        // Only for externalization
    }

    /**
     * New copy of a resource description
     *
     * @param rd
     */
    public ResourceDescription(ResourceDescription rd) {
        // Nothing to copy since there are no attributes
    }

    /**
     * Copy method
     *
     * @return
     */
    public abstract ResourceDescription copy();

    /**
     * Checks the static constraints for an implementation to be run on a
     * resource
     *
     * @param impl
     * @return
     */
    public abstract boolean canHost(Implementation impl);

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
    public abstract boolean canHostDynamic(Implementation impl);

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
     * Returns if the resource is defines some consumption
     *
     * @return
     */
    public abstract boolean isDynamicConsuming();

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

    public abstract String getDynamicDescription();
}
