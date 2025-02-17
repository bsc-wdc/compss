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

package es.bsc.compss.types.implementations.definition;

import java.io.Serializable;


/**
 * Container representation.
 */
public class ContainerDescription implements Serializable {

    private static final long serialVersionUID = 1L;


    public static enum ContainerEngine {
        DOCKER, // Docker container engine
        SINGULARITY, // Singularity container engine
        UDOCKER; // uDocker container engine
    }


    private ContainerEngine engine;
    private String image;
    private String options;


    /**
     * Create an empty container description for serialisation.
     */
    public ContainerDescription() {
        // Only for serialisation
    }

    /**
     * Create a Container Description.
     *
     * @param engine Container engine.
     * @param image Container image.
     */
    public ContainerDescription(ContainerEngine engine, String image, String options) {
        this.engine = engine;
        this.image = image;
        this.options = options;
    }

    /**
     * Get the container options.
     *
     * @return the container options
     */
    public String getOptions() {
        return options;
    }

    /**
     * Set the container options.
     *
     * @param options Container options.
     */
    public void setOptions(String options) {
        this.options = options;
    }

    /**
     * Returns the associated container engine.
     *
     * @return The associated container engine.
     */
    public ContainerEngine getEngine() {
        return this.engine;
    }

    /**
     * Sets a new container engine.
     *
     * @param engine New container engine.
     */
    public void setEngine(ContainerEngine engine) {
        this.engine = engine;
    }

    /**
     * Returns the associated container image.
     *
     * @return The associated container image.
     */
    public String getImage() {
        return this.image;
    }

    /**
     * Sets a new container image.
     *
     * @param image New container image.
     */
    public void setImage(String image) {
        this.image = image;
    }

    /**
     * Describes the container description as a JSON.
     * 
     * @return JSON representing the state of the Container Description.
     */
    public String toJSON() {
        return "{" + "\"engine\":\"" + this.engine.toString() + "\"," + "\"image\":\"" + this.image + "\"" + "}";
    }

    @Override
    public String toString() {
        return "ContainerDescription [engine=" + this.engine.toString() + ", image=" + this.image + "]";
    }

}
