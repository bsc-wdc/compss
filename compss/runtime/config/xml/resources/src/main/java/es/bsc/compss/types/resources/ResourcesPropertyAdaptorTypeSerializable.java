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

import es.bsc.compss.types.resources.jaxb.ResourcesPropertyAdaptorType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * This class exists only to make ResourcesPropertyAdaptorType Serializable.
 */
public class ResourcesPropertyAdaptorTypeSerializable extends ResourcesPropertyAdaptorType implements Externalizable {

    /**
     * Only for externalization.
     */
    public ResourcesPropertyAdaptorTypeSerializable() {
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        try {
            oo.writeUTF(this.name);
            oo.writeUTF(this.value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        try {
            this.name = oi.readUTF();
            this.value = oi.readUTF();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "{\"name\":\"" + this.getName() + "\", \"value\":\"" + this.getValue() + "\"}";
    }

}
