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
package es.bsc.compss.agent.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Data value location on an Agent.
 */
public class PrivateRemoteDataLocation implements RemoteDataLocation {

    private Resource<?, ?> resource;
    private String path;


    public PrivateRemoteDataLocation() {
        this.resource = null;
    }

    public PrivateRemoteDataLocation(Resource<?, ?> agent, String path) {
        this.resource = agent;
        this.path = path;
    }

    @Override
    public Type getType() {
        return Type.PRIVATE;
    }

    /**
     * Resource owning the data.
     *
     * @return Resource owning the data.
     */
    public Resource<?, ?> getResource() {
        return this.resource;
    }

    /**
     * Returns the path on the remote resource where to fetch the data.
     *
     * @return data path within the resource.
     */
    public String getPath() {
        return this.path;
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeObject(this.resource);
        oo.writeUTF(this.path);
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        this.resource = (Resource<?, ?>) oi.readObject();
        this.path = oi.readUTF();
    }

    public String toString() {
        return "{\"resource\":" + (this.resource == null ? "null" : this.resource) + "," + "\"path\":\"" + this.path
            + "\"}";
    }
}
