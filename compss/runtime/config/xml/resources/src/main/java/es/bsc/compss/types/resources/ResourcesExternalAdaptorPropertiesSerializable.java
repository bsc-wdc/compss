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

import es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ResourcesPropertyAdaptorType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
import java.util.List;


/**
 * This class exists only to make ResourcesExternalAdaptorProperties Serializable.
 */
public class ResourcesExternalAdaptorPropertiesSerializable extends ResourcesExternalAdaptorProperties
    implements Externalizable {

    /**
     * Only for externalization.
     */
    public ResourcesExternalAdaptorPropertiesSerializable() {
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        try {
            oo.writeObject(this.getProperty());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        try {
            this.property = (List<ResourcesPropertyAdaptorType>) oi.readObject();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"properties\":[");
        Iterator<ResourcesPropertyAdaptorType> itr = this.getProperty().iterator();
        if (itr.hasNext()) {
            ResourcesPropertyAdaptorType p = itr.next();
            sb.append(p);
            while (itr.hasNext()) {
                p = itr.next();
                sb.append("," + p);
            }
        }
        sb.append("]}");
        return sb.toString();
    }
}
