package integratedtoolkit.types.uri;

import integratedtoolkit.exceptions.UnstartedNodeException;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.types.resources.Resource;

import java.io.File;
import java.util.HashMap;
import java.util.Map.Entry;


public class MultiURI implements Comparable<MultiURI> {

    private final Protocol protocol;
    private final Resource host;
    private final String path;
    private final HashMap<String, Object> internal;


    public MultiURI(Protocol protocol, Resource host, String path) {
        this.protocol = protocol;
        this.host = host;
        this.path = path;
        this.internal = new HashMap<>();
    }

    public void setInternalURI(String adaptor, Object uri) {
        internal.put(adaptor, uri);
    }

    public Object getInternalURI(String adaptor) throws UnstartedNodeException {
        Object o = internal.get(adaptor);
        if (o == null) {
            host.setInternalURI(this);
            o = internal.get(adaptor);
        }
        return o;
    }

    public Resource getHost() {
        return this.host;
    }

    public String getPath() {
        return this.path;
    }

    public Protocol getProtocol() {
        return this.protocol;
    }

    public String getScheme() {
        return this.protocol.getSchema();
    }

    @Override
    public String toString() {
        return this.protocol.getSchema() + this.host.getName() + File.separator + this.path;
    }

    public String debugString() {
        StringBuilder sb = new StringBuilder(this.protocol.getSchema() + this.host.toString() + File.separator + this.path + "\n");
        for (Entry<String, Object> e : internal.entrySet()) {
            sb.append("\t * ").append(e.getKey()).append(" -> ").append(e.getValue()).append("\n");
        }
        return sb.toString();
    }

    @Override
    public int compareTo(MultiURI o) {
        if (o == null) {
            throw new NullPointerException();
        }
        int compare = host.getName().compareTo(o.host.getName());
        if (compare == 0) {
            compare = path.compareTo(o.path);
        }
        return compare;
    }
}
