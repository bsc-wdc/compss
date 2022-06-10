/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.agent.types;

import es.bsc.compss.types.resources.MethodResourceDescription;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Description of a resource to be used by the agent.
 *
 * @param <P> Description of the resource corresponding to the project configuration file
 * @param <R> Description of the resource corresponding to the resources configuration file
 */
public class Resource<P, R> implements Externalizable {

    private static final long serialVersionUID = 1L;

    private String name;
    private MethodResourceDescription description;
    private String adaptor;
    private P projectConf;
    private R resourceConf;


    public Resource() {
    }

    /**
     * Constructs a new resource description.
     *
     * @param name Name of the node
     * @param description Description of the resources available
     * @param adaptor Name of the adaptor to use to interact with the node
     * @param projectConf Project configuration file content related to the node
     * @param resourceConf Resources configuration file content related to the node
     */
    public Resource(String name, MethodResourceDescription description, String adaptor, P projectConf, R resourceConf) {
        this.name = name;
        this.description = description;
        this.adaptor = adaptor;
        this.projectConf = projectConf;
        this.resourceConf = resourceConf;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAdaptor() {
        return adaptor;
    }

    public void setAdaptor(String adaptor) {
        this.adaptor = adaptor;
    }

    public MethodResourceDescription getDescription() {
        return description;
    }

    public void setDescription(MethodResourceDescription description) {
        this.description = description;
    }

    public P getProjectConf() {
        return projectConf;
    }

    public void setProjectConf(P projectConf) {
        this.projectConf = projectConf;
    }

    public R getResourceConf() {
        return resourceConf;
    }

    public void setResourceConf(R resourceConf) {
        this.resourceConf = resourceConf;
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        try {
            oo.writeUTF(name);
            oo.writeUTF(adaptor);
            oo.writeObject(description);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        try {
            this.name = oi.readUTF();
            this.adaptor = oi.readUTF();
            this.description = (MethodResourceDescription) oi.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public String toString() {
        return "NAME = " + name + ", ADAPTOR = " + adaptor;
    }

}
