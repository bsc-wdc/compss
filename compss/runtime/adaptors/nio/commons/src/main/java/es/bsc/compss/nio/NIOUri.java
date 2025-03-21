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
package es.bsc.compss.nio;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.execution.InvocationParamURI;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Class to represent internal URIs in NIO.
 */
public class NIOUri implements Externalizable, InvocationParamURI {

    private NIONode host;
    private String path;
    private ProtocolType protocol;


    public NIOUri() {
        // Only for externalization
    }

    /**
     * Creates a new NIOUri instance with the given information.
     * 
     * @param host NIONode hosting the URI.
     * @param path Path.
     * @param schema Schema.
     */
    public NIOUri(NIONode host, String path, ProtocolType schema) {
        this.host = host;
        this.path = path;
        this.protocol = schema;
    }

    /**
     * Returns the internal URI.
     * 
     * @return The internal URI.
     */
    public String getInternalURI() {
        return this.toString();
    }

    @Override
    public ProtocolType getProtocol() {
        return this.protocol;
    }

    /**
     * Returns the URI host.
     * 
     * @return The URI host.
     */
    public NIONode getHost() {
        return this.host;
    }

    @Override
    public boolean isHost(String hostname) {
        String hostAndPort = this.host.toString();
        String hostName = hostAndPort.substring(0, hostAndPort.indexOf(":"));
        return hostName.equals(hostname);
    }

    @Override
    public String getPath() {
        return this.path;
    }

    /**
     * Returns the URI scheme.
     * 
     * @return The URI scheme.
     */
    public String getScheme() {
        return this.protocol.getSchema();
    }

    @Override
    public String toString() {
        return this.protocol.getSchema() + this.host + File.separator + this.path;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.host);
        out.writeUTF(this.path);
        out.writeUTF(this.protocol.getSchema());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.host = (NIONode) in.readObject();
        this.path = in.readUTF();
        this.protocol = ProtocolType.getBySchema(in.readUTF());
    }

}
