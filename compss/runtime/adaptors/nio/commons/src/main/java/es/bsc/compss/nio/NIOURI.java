/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.types.execution.InvocationParamURI;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class NIOURI implements Externalizable, InvocationParamURI {

    private NIONode host;
    private String path;
    private Protocol protocol;

    public NIOURI() {
    }

    public NIOURI(NIONode host, String path, Protocol schema) {
        this.host = host;
        this.path = path;
        this.protocol = schema;
    }

    public String getInternalURI() {
        return toString();
    }

    @Override
    public Protocol getProtocol() {
        return protocol;
    }

    public NIONode getHost() {
        return host;
    }

    @Override
    public boolean isHost(String hostname) {
        String hostAndPort = host.toString();
        String hostName = hostAndPort.substring(0, hostAndPort.indexOf(":"));
        return hostName.equals(hostname);
    }

    @Override
    public String getPath() {
        return path;
    }

    public String getScheme() {
        return protocol.getSchema();
    }

    @Override
    public String toString() {
        return protocol.getSchema() + host + File.separator + path;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(host);
        out.writeUTF(path);
        out.writeUTF(protocol.getSchema());
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        host = (NIONode) in.readObject();
        path = in.readUTF();
        protocol = Protocol.getBySchema(in.readUTF());
    }

}
