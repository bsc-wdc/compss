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

/**
 * Container representation.
 */
public class ContainerDescription {

    private String engine;
    private String image;
    private String binary;
    private String hostDir;


    public ContainerDescription() {
    }

    /**
     * Create a Container Description.
     * 
     * @param engine Container engine.
     * @param image Container imagen.
     * @param binary COntainer binary.
     * @param hostDir Container directory.
     */
    public ContainerDescription(String engine, String image, String binary, String hostDir) {
        super();
        this.engine = engine;
        this.image = image;
        this.binary = binary;
        this.hostDir = hostDir;
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

    public String getBinary() {
        return binary;
    }

    public void setBinary(String binary) {
        this.binary = binary;
    }

    public void setImage(String image) {
        this.image = image;
    }

    public String getHostDir() {
        return hostDir;
    }

    public void setHostDir(String hostDir) {
        this.hostDir = hostDir;
    }

    @Override
    public String toString() {
        return "ContainerDescription [engine=" + engine + ", image=" + image + ", binary=" + binary + ", hostDir="
            + hostDir + "]";
    }

}
