package integratedtoolkit.types.data.location;

import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.util.ErrorManager;

import java.util.LinkedList;

import storage.StorageException;
import storage.StorageItf;


public class PersistentLocation extends DataLocation {

    private final String id;


    public PersistentLocation(String id) {
        super();
        this.id = id;
    }

    @Override
    public DataLocation.Type getType() {
        return DataLocation.Type.PERSISTENT;
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.PERSISTENT_URI;
    }

    public String getId() {
        return this.id;
    }

    @Override
    public LinkedList<MultiURI> getURIs() {
        LinkedList<MultiURI> uris = new LinkedList<>();

        // Retrieve URIs from Storage Back-end
        try {
            for (String hostName : StorageItf.getLocations(this.id)) {
                Resource host = Resource.getResource(hostName);
                if (host != null) {
                    uris.add(new MultiURI(Protocol.PERSISTENT_URI, host, this.id));
                } else {
                    logger.warn("Storage Back-End returned non-registered host " + hostName);
                }
            }
        } catch (StorageException e) {
            ErrorManager.error("ERROR: Cannot retrieve locations of " + this.id + " from Storage Back-end");
        }

        return uris;
    }

    @Override
    public LinkedList<Resource> getHosts() {
        LinkedList<Resource> hosts = new LinkedList<>();

        // Retrieve URIs from Storage Back-end
        logger.debug("Get PSCO locations for " + this.id);
        try {
            for (String hostName : StorageItf.getLocations(this.id)) {
                Resource host = Resource.getResource(hostName);
                if (host != null) {
                    hosts.add(host);
                } else {
                    logger.warn("Storage Back-End returned non-registered host " + hostName);
                }
            }
        } catch (StorageException e) {
            ErrorManager.error("ERROR: Cannot retrieve locations of " + this.id + " from Storage Back-end");
        }

        return hosts;
    }

    @Override
    public MultiURI getURIInHost(Resource targetHost) {
        // Retrieve URIs from Storage Back-end
        try {
            for (String hostName : StorageItf.getLocations(this.id)) {
                if (hostName.equals(targetHost.getName())) {
                    return new MultiURI(Protocol.PERSISTENT_URI, targetHost, this.id);
                }
            }
        } catch (StorageException e) {
            ErrorManager.error("ERROR: Cannot retrieve locations of " + this.id + " from Storage Back-end");
        }

        return null;
    }

    @Override
    public boolean isTarget(DataLocation target) {
        if (target.getType() != DataLocation.Type.PERSISTENT) {
            return false;
        }

        return this.id.equals(((PersistentLocation) target).id);
    }

    @Override
    public String toString() {
        return Protocol.PERSISTENT_URI.getSchema() + this.id;
    }

    @Override
    public String getSharedDisk() {
        return null;
    }

    @Override
    public String getPath() {
        return this.id;
    }

    @Override
    public String getLocationKey() {
        return this.id + ":persistent:" + "allHosts";
    }

    @Override
    public int compareTo(DataLocation o) {
        if (o == null) {
            throw new NullPointerException();
        }
        if (o.getClass() != PersistentLocation.class) {
            return (this.getClass().getName()).compareTo(PersistentLocation.class.toString());
        } else {
            return this.id.compareTo(((PersistentLocation) o).id);
        }
    }

}
