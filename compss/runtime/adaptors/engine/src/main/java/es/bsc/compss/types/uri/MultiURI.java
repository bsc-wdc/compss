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
package es.bsc.compss.types.uri;

import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.resources.Resource;

import java.io.File;
import java.util.HashMap;
import java.util.Map.Entry;


/**
 * Represents the different URIs associated to the same path in the same host.
 */
public class MultiURI implements Comparable<MultiURI> {

    private final ProtocolType protocol;
    private final Resource host;
    private final String path;
    private final HashMap<String, Object> internal;


    /**
     * Constructor.
     * 
     * @param protocol Protocol
     * @param host Resource
     * @param path Path in the resource
     */
    public MultiURI(ProtocolType protocol, Resource host, String path) {
        this.protocol = protocol;
        this.host = host;
        this.path = path;

        this.internal = new HashMap<>();

        // Try to register the URI now
        try {
            host.setInternalURI(this);
        } catch (UnstartedNodeException e) {
            // We do not throw the exception because we do not want to block the application
            // We will try to recover the URI on the getInternalURI method
        }
    }

    public void setInternalURI(String adaptor, Object uri) {
        this.internal.put(adaptor, uri);
    }

    /**
     * Get the internal URI for a given adaptor.
     * 
     * @param adaptor Adaptor name
     * @return Returns the URI in the adaptors format
     * @throws UnstartedNodeException Error because host of the MultiURI not started
     */
    public Object getInternalURI(String adaptor) throws UnstartedNodeException {
        Object o = this.internal.get(adaptor);

        if (o == null) {
            // Try to register the URI now
            this.host.setInternalURI(this);
            o = this.internal.get(adaptor);
        }
        return o;
    }

    public ProtocolType getProtocol() {
        return this.protocol;
    }

    public Resource getHost() {
        return this.host;
    }

    public String getPath() {
        return this.path;
    }

    @Override
    public String toString() {
        // File.separator not needed, even if hostname isEmpty
        return this.protocol.getSchema() + this.host.getName() + this.path;
    }

    /**
     * Extended toString version for debugging purposes.
     * 
     * @return
     */
    public String debugString() {
        StringBuilder sb = new StringBuilder();

        sb.append(this.protocol.getSchema());
        sb.append(this.host.getName()).append(File.separator).append(this.path);
        sb.append("\n");

        for (Entry<String, Object> e : this.internal.entrySet()) {
            sb.append("\t * ").append(e.getKey()).append(" -> ").append(e.getValue()).append("\n");
        }

        return sb.toString();
    }

    @Override
    public int compareTo(MultiURI o) {
        if (o == null) {
            throw new NullPointerException();
        }
        int compare = this.host.getName().compareTo(o.host.getName());
        if (compare == 0) {
            compare = this.path.compareTo(o.path);
        }
        return compare;
    }
}
