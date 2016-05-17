package integratedtoolkit.types.data.location;

import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.util.SharedDiskManager;

import java.io.File;
import java.util.LinkedList;


public class SharedLocation extends DataLocation {

    private final String diskName;
    private final String path;

    SharedLocation(String sharedDisk, String path) {
    	this.diskName = sharedDisk;
        this.path = path;
    }

    @Override
    public URI getURIInHost(Resource host) {
        String diskPath = SharedDiskManager.getMounpoint(host, diskName);
        if (diskPath == null) {
            return null;
        }
        return new URI(host, diskPath + path);
    }

    @Override
    public DataLocation.Type getType() {
        return DataLocation.Type.SHARED;
    }

    @Override
    public LinkedList<URI> getURIs() {
        LinkedList<URI> uris = new LinkedList<URI>();
        for (Resource host : SharedDiskManager.getAllMachinesfromDisk(diskName)) {
            String diskPath = SharedDiskManager.getMounpoint(host, diskName);
            uris.add(new URI(host, diskPath + path));
        }
        return uris;
    }

    @Override
    public LinkedList<Resource> getHosts() {
        return SharedDiskManager.getAllMachinesfromDisk(diskName);
    }

    @Override
    public boolean isTarget(DataLocation target) {
        String targetDisk;
        String targetPath;
        if (target.getType() == DataLocation.Type.PRIVATE) {
            PrivateLocation privateLoc = (PrivateLocation) target;
            targetDisk = null;//TODO: extract from URI
            targetPath = privateLoc.uri.getPath();
        } else {
            SharedLocation sharedloc = (SharedLocation) target;
            targetDisk = sharedloc.diskName;
            targetPath = sharedloc.path;
        }
        return (targetDisk != null && targetDisk.contentEquals(diskName) && targetPath.contentEquals(targetPath));
    }

    public String toString() {
        return "shared:" + diskName + File.separator + path;
    }

    @Override
    public String getSharedDisk() {
        return diskName;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public String getLocationKey() {
        return path + ":shared:" + diskName;
    }

    @Override
    public int compareTo(DataLocation o) {
        if (o == null) {
            throw new NullPointerException();
        }
        if (o.getClass() != SharedLocation.class) {
            return (this.getClass().getName()).compareTo("integratedtoolkit.types.data.location.SharedLocation");
        } else {
            SharedLocation sl = (SharedLocation) o;
            int compare = diskName.compareTo(sl.diskName);
            if (compare == 0) {
                compare = path.compareTo(sl.path);
            }
            return compare;
        }
    }
}
