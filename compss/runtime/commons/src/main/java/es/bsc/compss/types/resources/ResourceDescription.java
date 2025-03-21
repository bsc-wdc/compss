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
package es.bsc.compss.types.resources;

import es.bsc.compss.types.implementations.Implementation;

import java.io.Externalizable;


/**
 * Abstract representation of a Resource.
 */
public abstract class ResourceDescription implements Externalizable {

    /**
     * Empty resource Description for serialization.
     */
    public ResourceDescription() {
        // Only for externalization
    }

    /**
     * New copy of a resource description.
     *
     * @param rd ResourceDescription to be copied.
     */
    public ResourceDescription(ResourceDescription rd) {
        // Nothing to copy since there are no attributes
    }

    /**
     * Copies the current ResourceDescription.
     *
     * @return A copy of the current ResourceDescription.
     */
    public abstract ResourceDescription copy();

    /**
     * Checks the static constraints for an implementation to be run on a resource.
     *
     * @param impl Implementation to be run
     * @return {@code true} if the resource can run the given implementation, {@code false} otherwise.
     */
    public abstract boolean canHost(Implementation impl);

    /**
     * Modifies the current description to copy the values from the one passed in as a parameter.
     *
     * @param rd Resource Description to be copied.
     */
    public abstract void mimic(ResourceDescription rd);

    /**
     * Increases the static and dynamic capabilities.
     *
     * @param rd ResourceDescription containing the increasing capabilities.
     */
    public abstract void increase(ResourceDescription rd);

    /**
     * Decreases the static and dynamic capabilities.
     *
     * @param rd ResourceDescription containing the decreasing capabilities.
     */
    public abstract void reduce(ResourceDescription rd);

    /**
     * Checks the dynamic capabilities for an implementation to be run on a resource.
     *
     * @param impl Implementation to be run.
     * @return {@code true} if the resource can dynamically run the given implementation, {@code false} otherwise.
     */
    public abstract boolean canHostDynamic(Implementation impl);

    /**
     * Increases the dynamic capabilities.
     *
     * @param rd ResourceDescription containing the dynamic increasing capabilities.
     */
    public abstract void increaseDynamic(ResourceDescription rd);

    /**
     * Reduces the dynamic capabilities returning the description of the real capabilities that have been reduced.
     *
     * @param rd ResourceDescription containing the dynamic decreasing capabilities.
     * @return A description of the real capabilities that have been reduced.
     */
    public abstract ResourceDescription reduceDynamic(ResourceDescription rd);

    /**
     * Return the dynamic common capabilities.
     *
     * @param constraints ResourceDescription to compare with.
     * @return A ResourceDescription containing the common dynamic capabilities.
     */
    public abstract ResourceDescription getDynamicCommons(ResourceDescription constraints);

    /**
     * Returns whether the resource is fully used or not.
     *
     * @return {@code true} if the dynamic capabilities are fully used, {@code false} otherwise.
     */
    public abstract boolean isDynamicUseless();

    /**
     * Returns if the resource defines some consumption.
     *
     * @return {@code true} if the resource is consuming, {@code false} otherwise.
     */
    public abstract boolean isDynamicConsuming();

    /**
     * Reduce only the common dynamic capabilities from the current ResourceDescription and the gap ResourceDescription.
     *
     * @param gapResource ResourceDescription defining the gap
     * @param constraints ResourceDescription defining the common dynamic capabilities to reduce.
     * @return
     */
    public static ResourceDescription reduceCommonDynamics(ResourceDescription gapResource,
        ResourceDescription constraints) {
        ResourceDescription commons = gapResource.getDynamicCommons(constraints);
        gapResource.reduceDynamic(commons);
        constraints.reduceDynamic(commons);
        return commons;
    }

    /**
     * Returns a string containing the dynamic description of the resource.
     * 
     * @return A string containing the dynamic description of the resource.
     */
    public abstract String getDynamicDescription();

    /**
     * Returns whether a method uses CPU or not.
     * 
     * @return true if the method uses CPUs, false otherwise.
     */
    public abstract boolean usesCPUs();

}
