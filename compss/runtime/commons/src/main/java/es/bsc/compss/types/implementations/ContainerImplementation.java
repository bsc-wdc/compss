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

package es.bsc.compss.types.implementations;

import es.bsc.compss.types.resources.MethodResourceDescription;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class ContainerImplementation extends AbstractMethodImplementation implements Externalizable {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 3;
    public static final String SIGNATURE = "container.CONTAINER";
    private String engine;
    private String image;
    private String binary;


    /**
     * Creates a new ContainerImplementation instance for serialization.
     */
    public ContainerImplementation() {
        // For externalizable
        super();
    }

    /**
     * Creates a new ContainerImplementation from the given parameters.
     * 
     * @param engine Engine[docker or singularity].
     * @param image Image from the container.
     * @param binary Binary to execute.
     * @param coreId Core Id.
     * @param implementationId Implementation Id.
     * @param signature Container signature.
     * @param annot Container requirements.
     */
    public ContainerImplementation(String engine, String image, String binary, Integer coreId, Integer implementationId,
        String signature, MethodResourceDescription annot) {

        super(coreId, implementationId, signature, annot);
        this.engine = engine;
        this.image = image;
    }

    public String getEngine() {
        return engine;
    }

    public String getImage() {
        return image;
    }

    public String getBinary() {
        return binary;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.CONTAINER;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[CONTAINER=").append(engine);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return "ContainerImplementation [engine=" + engine + ", image=" + image + ", binary=" + binary + "]";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.engine = (String) in.readObject();
        this.image = (String) in.readObject();
        this.binary = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.engine);
        out.writeObject(this.image);
        out.writeObject(this.binary);
    }

}
