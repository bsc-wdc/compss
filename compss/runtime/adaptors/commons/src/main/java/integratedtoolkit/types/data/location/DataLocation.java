package integratedtoolkit.types.data.location;

import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.util.SharedDiskManager;
import java.util.LinkedList;


public abstract class DataLocation implements Comparable<DataLocation>{

    public enum Type {
        PRIVATE,
        SHARED
    }

    public static DataLocation getSharedLocation(String sharedDisk, String path) {
        return new SharedLocation(sharedDisk, path);
    }

    public static DataLocation getPrivateLocation(Resource host, String path) {
        return new PrivateLocation(host, path);
    }

    public static DataLocation getLocation(Resource host, String path) {
        String diskName = SharedDiskManager.getSharedName(host, path);
        if (diskName != null) {
            String mountpoint = SharedDiskManager.getMounpoint(host, diskName);
            return new SharedLocation(diskName, path.substring(mountpoint.length()));
        } else {
            return new PrivateLocation(host, path);
        }
    }

    public abstract Type getType();

    public abstract LinkedList<URI> getURIs();

    public abstract String getSharedDisk();

    public abstract LinkedList<Resource> getHosts();

    public abstract String getPath();

    public abstract URI getURIInHost(Resource targetHost);

    public abstract boolean isTarget(DataLocation target);

    public abstract String getLocationKey();

}
