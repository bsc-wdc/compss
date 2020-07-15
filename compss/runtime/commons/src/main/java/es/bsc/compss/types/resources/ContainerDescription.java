/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import java.io.Serializable;


/**
 * Container representation.
 */
public class ContainerDescription implements Serializable {

    private static final long serialVersionUID = 1L;

    private String engine;
    private String image;


    public ContainerDescription() {
    }

    /**
     * Create a Container Description.
     * 
     * @param engine Container engine.
     * @param image Container imagen.
     */
    public ContainerDescription(String engine, String image) {
        super();
        this.engine = engine;
        this.image = image;
    }

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Override
    public String toString() {
        return "ContainerDescription [engine=" + engine + ", image=" + image + "]";
    }

}
