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

package es.bsc.compss.agent.comm.messages.types;

import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourcesExternalAdaptorPropertiesSerializable;
import es.bsc.compss.types.resources.ResourcesPropertyAdaptorTypeSerializable;
import es.bsc.compss.types.resources.jaxb.ResourcesExternalAdaptorProperties;
import es.bsc.compss.types.resources.jaxb.ResourcesPropertyAdaptorType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Resource description for the Comm Agent.
 */
public class CommResource extends Resource<Void, ResourcesExternalAdaptorProperties> implements Externalizable {

    private static final String ADAPTOR_NAME = "es.bsc.compss.agent.comm.CommAgentAdaptor";
    private int port;


    public CommResource() {
    }

    public CommResource(String name, int port) {
        super(name, null, ADAPTOR_NAME, null, null);
        this.port = port;
    }

    public CommResource(String name, MethodResourceDescription desc, int port) {
        super(name, desc, ADAPTOR_NAME, null, null);
        this.port = port;
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        super.writeExternal(oo);
        oo.writeInt(port);
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        super.readExternal(oi);
        this.port = oi.readInt();
    }

    @Override
    public ResourcesExternalAdaptorProperties getResourceConf() {
        ResourcesExternalAdaptorProperties properties = new ResourcesExternalAdaptorPropertiesSerializable();
        ResourcesPropertyAdaptorType property = new ResourcesPropertyAdaptorTypeSerializable();
        property.setName("Port");
        property.setValue(Integer.toString(port));
        properties.getProperty().add(property);
        return properties;
    }

    public String getAdaptor() {
        return CommResource.ADAPTOR_NAME;
    }

    public int getPort() {
        return port;
    }

    @Override
    protected void dumpContent(StringBuilder sb) {
        super.dumpContent(sb);
        sb.append(",\"port\":").append(port);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        this.dumpContent(sb);
        sb.append("}");
        return sb.toString();
    }

}
