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
package es.bsc.compss.gos.master;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.resources.Resource;


public class GOSUri {

    private final String user;
    private final String host;
    private final String path;

    private final ProtocolType protocol;


    /**
     * Creates a new GOSUri instance with the given information.
     *
     * @param host Resource hosting the URI.
     * @param path Path.
     * @param schema Schema.
     */
    public GOSUri(String user, Resource host, String path, ProtocolType schema) {
        this.user = user;
        this.host = host.getName();
        this.path = path;
        this.protocol = schema;
    }

    public String getHost() {
        return host;
    }

    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "GOSURI: " + protocol.getSchema() + user + "@" + getHost() + "/" + path;
    }

    public String getUser() {
        return user;
    }

    public boolean isLocal() {
        return host.equals(Comm.getAppHost().getName());
    }
}
