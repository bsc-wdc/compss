package integratedtoolkit.types.data.location;

import integratedtoolkit.types.resources.Resource;
import java.util.LinkedList;


public class PrivateLocation extends DataLocation{

    final URI uri;

    public PrivateLocation(Resource host, String path) {
        super();
        this.uri = new URI(host, path);
    }

    @Override
    public DataLocation.Type getType() {
        return DataLocation.Type.PRIVATE;
    }

    @Override
    public LinkedList<URI> getURIs() {
        LinkedList<URI> list = new LinkedList<URI>();
        list.add(this.uri);
        return list;
    }

    @Override
    public LinkedList<Resource> getHosts() {
        LinkedList<Resource> list = new LinkedList<Resource>();
        list.add(this.uri.getHost());
        return list;
    }

    @Override
    public URI getURIInHost(Resource targetHost) {
        if (uri.getHost() == targetHost) {
            return uri;
        } else {
            return null;
        }
    }

    @Override
    public boolean isTarget(DataLocation target) {
        if (target.getType() != DataLocation.Type.PRIVATE) {
            return false;
        }
        URI targetURI = ((PrivateLocation) target).uri;
        return (targetURI.getHost() == uri.getHost()
                && targetURI.getPath().contentEquals(uri.getPath()));
    }

    public String toString() {
        return this.uri.toString();
    }

    @Override
    public String getSharedDisk() {
        return null;
    }

    @Override
    public String getPath() {
        return this.uri.getPath();
    }

    @Override
    public String getLocationKey() {
        return uri.getPath() + ":" + uri.getHost().getName();
    }

    @Override
    public int compareTo(DataLocation o) {
        if (o == null) {
            throw new NullPointerException();
        }
        if (o.getClass() != PrivateLocation.class) {
            return (this.getClass().getName()).compareTo("integratedtoolkit.types.data.location.PrivateLocation");
        } else {
            return uri.compareTo(((PrivateLocation) o).uri);
        }
    }

}
