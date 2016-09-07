package integratedtoolkit.nio;

import es.bsc.comm.nio.NIONode;
import integratedtoolkit.types.data.location.DataLocation.Protocol;

import java.io.Externalizable;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class NIOURI implements Externalizable {

    private NIONode host;
    private String path;


    public NIOURI() {
    }

    public NIOURI(NIONode host, String path) {
        this.host = host;
        this.path = path;
    }

    public String getInternalURI() {
        return toString();
    }

    public NIONode getHost() {
        return host;
    }

    public String getPath() {
        return path;
    }

    public String getScheme() {
        return Protocol.ANY_URI.getSchema();
    }

    @Override
    public String toString() {
        return Protocol.ANY_URI.getSchema() + host + File.separator + path;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(host);
        out.writeUTF(path);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        host = (NIONode) in.readObject();
        path = in.readUTF();
    }

}
