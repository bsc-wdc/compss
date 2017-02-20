package integratedtoolkit.types.data.location;

import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.uri.MultiURI;

import java.util.LinkedList;


public class PrivateLocation extends DataLocation {

    private final MultiURI uri;


    public PrivateLocation(Protocol protocol, Resource host, String path) {
        super();
        this.uri = new MultiURI(protocol, host, path);
    }

    @Override
    public DataLocation.Type getType() {
        return DataLocation.Type.PRIVATE;
    }

    @Override
    public Protocol getProtocol() {
        return this.uri.getProtocol();
    }

    @Override
    public LinkedList<MultiURI> getURIs() {
        LinkedList<MultiURI> list = new LinkedList<>();
        list.add(this.uri);

        return list;
    }

    @Override
    public LinkedList<Resource> getHosts() {
        LinkedList<Resource> list = new LinkedList<>();
        list.add(this.uri.getHost());
        return list;
    }

    @Override
    public MultiURI getURIInHost(Resource targetHost) {
        if (uri.getHost().equals(targetHost)) {
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
        MultiURI targetURI = ((PrivateLocation) target).uri;
        return (targetURI.getHost() == uri.getHost() && targetURI.getPath().contentEquals(uri.getPath()));
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
            return (this.getClass().getName()).compareTo(PrivateLocation.class.toString());
        } else {
            return uri.compareTo(((PrivateLocation) o).uri);
        }
    }

    @Override
    public String toString() {
        return this.uri.toString();
    }

}
