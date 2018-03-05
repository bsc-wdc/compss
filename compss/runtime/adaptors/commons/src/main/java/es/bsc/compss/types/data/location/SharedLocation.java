package es.bsc.compss.types.data.location;

import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.SharedDiskManager;

import java.io.File;
import java.util.LinkedList;
import java.util.List;


public class SharedLocation extends DataLocation {

    private final String diskName;
    private final String path;
    private final Protocol protocol;


    public SharedLocation(Protocol protocol, String sharedDisk, String path) {
        this.diskName = sharedDisk;
        this.path = path;
        this.protocol = protocol;
    }

    @Override
    public MultiURI getURIInHost(Resource host) {
        String diskPath = SharedDiskManager.getMounpoint(host, this.diskName);
        if (diskPath == null) {
            return null;
        }
        if (!diskPath.endsWith(File.separator)) {
            diskPath = diskPath + File.separator;
        }

        return new MultiURI(this.protocol, host, diskPath + this.path);
    }

    @Override
    public DataLocation.Type getType() {
        return DataLocation.Type.SHARED;
    }

    @Override
    public Protocol getProtocol() {
        return this.protocol;
    }

    @Override
    public List<MultiURI> getURIs() {
        List<MultiURI> uris = new LinkedList<>();
        List<Resource> resList = SharedDiskManager.getAllMachinesfromDisk(this.diskName);
        Resource[] resources;
        synchronized (resList) {
            resources = resList.toArray(new Resource[resList.size()]);
        }
        for (Resource host : resources) {
            String diskPath = SharedDiskManager.getMounpoint(host, this.diskName);
            if (!diskPath.endsWith(File.separator)) {
                diskPath = diskPath + File.separator;
            }

            uris.add(new MultiURI(this.protocol, host, diskPath + this.path));
        }

        return uris;
    }

    @Override
    public List<Resource> getHosts() {
        return SharedDiskManager.getAllMachinesfromDisk(this.diskName);
    }

    @Override
    public boolean isTarget(DataLocation target) {
        String targetDisk;
        String targetPath;
        if (target.getType().equals(DataLocation.Type.PRIVATE)) {
            PrivateLocation privateLoc = (PrivateLocation) target;
            targetDisk = null; // TODO: extract from URI
            targetPath = privateLoc.getPath();
        } else {
            SharedLocation sharedloc = (SharedLocation) target;
            targetDisk = sharedloc.diskName;
            targetPath = sharedloc.path;
        }
        return (targetDisk != null && targetDisk.contentEquals(this.diskName) && targetPath.contentEquals(targetPath));
    }

    @Override
    public String getSharedDisk() {
        return this.diskName;
    }

    @Override
    public String getPath() {
        return this.path;
    }

    @Override
    public String getLocationKey() {
        return this.path + ":shared:" + this.diskName;
    }

    @Override
    public int compareTo(DataLocation o) {
        if (o == null) {
            throw new NullPointerException();
        }
        if (o.getClass() != SharedLocation.class) {
            return (this.getClass().getName()).compareTo(SharedLocation.class.toString());
        } else {
            SharedLocation sl = (SharedLocation) o;
            int compare = this.diskName.compareTo(sl.diskName);
            if (compare == 0) {
                compare = this.path.compareTo(sl.path);
            }
            return compare;
        }
    }

    @Override
    public String toString() {
        return "shared:" + this.diskName + File.separator + this.path;
    }

}
