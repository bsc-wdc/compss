package integratedtoolkit.types.data;

import integratedtoolkit.comm.Comm;
import integratedtoolkit.exceptions.CannotLoadException;
import integratedtoolkit.exceptions.UnstartedNodeException;
import integratedtoolkit.log.Loggers;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;

import integratedtoolkit.types.data.listener.SafeCopyListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.types.data.location.PersistentLocation;
import integratedtoolkit.types.data.operation.copy.Copy;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;
import integratedtoolkit.util.SharedDiskManager;
import integratedtoolkit.util.Tracer;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;


public class LogicalData {

    // Logger
    private static final Logger logger = LogManager.getLogger(Loggers.COMM);

    // Logical data name
    private final String name;
    // Value in memory, null if value in disk
    private Object value;
    // Id if PSCO, null otherwise
    private String id;

    // List of existing copies
    private final Set<DataLocation> locations = new TreeSet<>();
    // In progress
    private final List<CopyInProgress> inProgress = new LinkedList<>();

    // Indicates if LogicalData has been ordered to save before
    private boolean isBeingSaved;
    // Locks the host while LogicalData is being copied
    private final Semaphore lockHostRemoval = new Semaphore(1);


    /*
     * Constructors
     */
    /**
     * Constructs a LogicalData for a given data version
     * 
     * @param name
     */
    public LogicalData(String name) {
        this.name = name;
        this.value = null;
        this.id = null;

        this.isBeingSaved = false;
    }

    /*
     * Getters
     */
    /**
     * Returns the data version name
     * 
     * @return
     */
    public String getName() {
        // No need to sync because it cannot be modified
        return this.name;
    }

    /**
     * Returns the PSCO id. Null if its not a PSCO
     * 
     * @return
     */
    public String getId() {
        return this.id;
    }

    /**
     * Returns all the hosts that contain a data location
     * 
     * @return
     */
    public synchronized Set<Resource> getAllHosts() {
        Set<Resource> list = new HashSet<>();
        for (DataLocation loc : this.locations) {
            list.addAll(loc.getHosts());
        }

        return list;
    }

    /**
     * Obtain the all the URIs
     * 
     * @return
     */
    public synchronized List<MultiURI> getURIs() {
        List<MultiURI> list = new LinkedList<>();
        for (DataLocation loc : this.locations) {
            List<MultiURI> locationURIs = loc.getURIs();
            // Adds all the valid locations
            if (locationURIs != null) {
                list.addAll(locationURIs);
            }
        }

        return list;
    }

    public synchronized Set<DataLocation> getLocations() {
        return this.locations;
    }

    /**
     * Returns if the data value is stored in memory or not
     * 
     * @return
     */
    public synchronized boolean isInMemory() {
        return (this.value != null);
    }

    /**
     * Returns the value stored in memory
     * 
     * @return
     */
    public synchronized Object getValue() {
        return this.value;
    }

    /*
     * Setters
     */
    /**
     * Adds a new location
     * 
     * @param loc
     */
    public synchronized void addLocation(DataLocation loc) {
        this.isBeingSaved = false;
        this.locations.add(loc);
        for (Resource r : loc.getHosts()) {
            switch (loc.getType()) {
                case PRIVATE:
                    r.addLogicalData(this);
                    break;
                case SHARED:
                    SharedDiskManager.addLogicalData(loc.getSharedDisk(), this);
                    break;
                case PERSISTENT:
                    this.id = ((PersistentLocation) loc).getId();
                    break;
            }
        }
    }

    /**
     * Removes the object from master main memory and removes its location
     * 
     * @return
     */
    public synchronized Object removeValue() {
        DataLocation loc = null;
        String targetPath = Protocol.OBJECT_URI.getSchema() + this.name;
        try {
            SimpleURI uri = new SimpleURI(targetPath);
            loc = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
        }

        Object val = this.value;
        this.value = null;
        // Removes only the memory location (no need to check private, shared,
        // persistent)
        this.locations.remove(loc);

        return val;
    }

    /**
     * Sets the memory value
     * 
     * @param o
     */
    public synchronized void setValue(Object o) {
        this.value = o;
    }

    /**
     * Sets the LD id
     * 
     * @param id
     */
    public synchronized void setId(String id) {
        this.id = id;
    }

    /**
     * Writes memory value to file
     * 
     * @throws Exception
     */
    public synchronized void writeToStorage() throws IOException {
        if (this.id != null) {
            // It is a persistent object that is already persisted
            // Nothing to do
            // If the PSCO is not persisted we treat it as a normal object
        } else {
            // The object must be written to file
            String targetPath = Comm.getAppHost().getWorkingDirectory() + this.name;
            Serializer.serialize(value, targetPath);

            String targetPathWithSchema = Protocol.FILE_URI.getSchema() + targetPath;
            SimpleURI targetURI = new SimpleURI(targetPathWithSchema);
            DataLocation loc = DataLocation.createLocation(Comm.getAppHost(), targetURI);

            this.isBeingSaved = false;
            this.locations.add(loc);
            for (Resource r : loc.getHosts()) {
                switch (loc.getType()) {
                    case PRIVATE:
                        r.addLogicalData(this);
                        break;
                    case SHARED:
                        SharedDiskManager.addLogicalData(loc.getSharedDisk(), this);
                        break;
                    case PERSISTENT:
                        // Nothing to do
                        break;
                }
            }
        }
    }

    /**
     * Loads the value of the LogicalData from a file
     * 
     * @throws CannotLoadException
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws StorageException
     * @throws UnstartedNodeException
     * 
     * @throws Exception
     */
    public synchronized void loadFromStorage() throws CannotLoadException {
        if (value != null) {
            // Value is already loaded in memory
            return;
        }

        for (DataLocation loc : this.locations) {
            switch (loc.getType()) {
                case PRIVATE:
                case SHARED:
                    // Get URI and deserialize object if possible
                    MultiURI u = loc.getURIInHost(Comm.getAppHost());
                    if (u == null) {
                        continue;
                    }

                    String path = u.getPath();
                    if (path.startsWith(File.separator)) {
                        try {
                            this.value = Serializer.deserialize(path);
                        } catch (ClassNotFoundException | IOException e) {
                            // Check next location since deserialization was invalid
                            this.value = null;
                            continue;
                        }

                        String targetPath = Protocol.OBJECT_URI.getSchema() + this.name;
                        SimpleURI uri = new SimpleURI(targetPath);
                        try {
                            DataLocation tgtLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
                            addLocation(tgtLoc);
                        } catch (IOException e) {
                            // Check next location since location was invalid
                            this.value = null;
                            continue;
                        }
                    }

                    return;
                case PERSISTENT:
                    PersistentLocation pLoc = (PersistentLocation) loc;

                    if (Tracer.isActivated()) {
                        Tracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType());
                    }
                    try {
                        this.value = StorageItf.getByID(pLoc.getId());
                        this.id = pLoc.getId();
                    } catch (StorageException se) {
                        // Check next location since cannot retrieve the object from the storage Back-end
                        continue;
                    } finally {
                        if (Tracer.isActivated()) {
                            Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_GETBYID.getType());
                        }
                    }

                    String targetPath = Protocol.OBJECT_URI.getSchema() + this.name;
                    SimpleURI uri = new SimpleURI(targetPath);
                    try {
                        DataLocation tgtLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
                        addLocation(tgtLoc);
                    } catch (IOException e) {
                        // Check next location since location was invalid
                        this.value = null;
                        continue;
                    }

                    return;
            }
        }

        // Any location has been able to load the value
        throw new CannotLoadException("Object has not any valid location available in the master");
    }

    /**
     * Removes all the locations assigned to a given host and returns a valid location if the file is unique
     * 
     * @param host
     * @param sharedMountPoints
     * @return a valid location if the file is unique
     */
    public synchronized DataLocation removeHostAndCheckLocationToSave(Resource host, Map<String, String> sharedMountPoints) {
        // If the file is being saved means that this function has already been
        // executed
        // for the same LogicalData. Thus, all the host locations are already
        // removed
        // and there is no unique file to save
        if (isBeingSaved) {
            return null;
        }

        // Otherwise, we must remove all the host locations and store a unique
        // location if needed. We only store the "best" location if any (by
        // choosing
        // any private location found or the first shared location)
        lockHostRemoval_private();
        DataLocation uniqueHostLocation = null;
        Iterator<DataLocation> it = this.locations.iterator();
        while (it.hasNext()) {
            DataLocation loc = it.next();
            switch (loc.getType()) {
                case PRIVATE:
                    if (loc.getURIInHost(host) != null) {
                        this.isBeingSaved = true;
                        uniqueHostLocation = loc;
                        it.remove();
                    }
                    break;
                case SHARED:
                    // When calling this function the host inside the
                    // SharedDiskManager has been removed
                    // If there are no remaining hosts it means it was the last
                    // host thus, the location
                    // is unique and must be saved
                    if (loc.getHosts().isEmpty()) {
                        String sharedDisk = loc.getSharedDisk();
                        if (sharedDisk != null) {
                            String mountPoint = sharedMountPoints.get(sharedDisk);
                            if (mountPoint != null) {
                                if (uniqueHostLocation == null) {
                                    this.isBeingSaved = true;

                                    String targetPath = Protocol.FILE_URI.getSchema() + loc.getPath();
                                    try {
                                        SimpleURI uri = new SimpleURI(targetPath);
                                        uniqueHostLocation = DataLocation.createLocation(host, uri);
                                    } catch (Exception e) {
                                        ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
                                    }
                                }
                            }
                        }
                    }
                    break;
                case PERSISTENT:
                    // Persistent location must never be saved
                    break;
            }
        }

        releaseHostRemoval_private();
        return uniqueHostLocation;
    }

    /**
     * Returns the copies in progress
     * 
     * @return
     */
    public synchronized Collection<Copy> getCopiesInProgress() {
        List<Copy> copies = new LinkedList<>();
        for (CopyInProgress cp : this.inProgress) {
            copies.add(cp.getCopy());
        }

        return copies;
    }

    /**
     * Returns if the data is already available in a given targetHost
     * 
     * @param targetHost
     * @return
     */
    public synchronized MultiURI alreadyAvailable(Resource targetHost) {
        for (DataLocation loc : locations) {
            MultiURI u = loc.getURIInHost(targetHost);
            // If we have found a valid location, return it
            if (u != null) {
                return u;
            }
        }

        // All locations are invalid
        return null;
    }

    /**
     * Returns if a copy of the LogicalData is being performed to a target host
     * 
     * @param target
     * @return the copy in progress or null if none
     */
    public synchronized Copy alreadyCopying(DataLocation target) {
        for (CopyInProgress cip : this.inProgress) {
            if (cip.hasTarget(target)) {
                return cip.getCopy();
            }
        }

        return null;
    }

    /**
     * Begins a copy of the LogicalData to a target host
     * 
     * @param c
     * @param target
     */
    public synchronized void startCopy(Copy c, DataLocation target) {
        this.inProgress.add(new CopyInProgress(c, target));
    }

    /**
     * Marks the end of a copy. Returns the location of the finished copy or null if not found
     * 
     * @param c
     * @return
     */
    public synchronized DataLocation finishedCopy(Copy c) {
        DataLocation loc = null;

        Iterator<CopyInProgress> it = this.inProgress.iterator();
        while (it.hasNext()) {
            CopyInProgress cip = it.next();
            if (cip.c == c) {
                it.remove();
                loc = cip.loc;
                break;
            }
        }

        return loc;
    }

    /**
     * Adds a listener to the inProgress copies
     * 
     * @param listener
     */
    public synchronized void notifyToInProgressCopiesEnd(SafeCopyListener listener) {
        for (CopyInProgress cip : this.inProgress) {
            listener.addOperation();
            cip.c.addEventListener(listener);
        }
    }

    /**
     * Sets the LogicalData as obsolete
     * 
     */
    public synchronized void isObsolete() {
        for (Resource res : this.getAllHosts()) {
            res.addObsolete(this);
        }
    }

    public synchronized void lockHostRemoval() {
        lockHostRemoval_private();
    }

    public synchronized void releaseHostRemoval() {
        releaseHostRemoval_private();
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Logical Data name: ").append(this.name).append("\n");
        sb.append("\t Value: ").append(value).append("\n");
        sb.append("\t Id: ").append(id).append("\n");
        sb.append("\t Locations:\n");
        synchronized (locations) {
            for (DataLocation dl : locations) {
                sb.append("\t\t * ").append(dl).append("\n");
            }
        }
        return sb.toString();
    }

    /*
     * PRIVATE HELPER METHODS
     */
    private void lockHostRemoval_private() {
        try {
            lockHostRemoval.acquire();
        } catch (InterruptedException e) {
            logger.error("Exception", e);
        }
    }

    private void releaseHostRemoval_private() {
        lockHostRemoval.release();
    }


    /*
     * Copy in progress class to extend external copy
     */
    private static class CopyInProgress {

        private final Copy c;
        private final DataLocation loc;


        public CopyInProgress(Copy c, DataLocation loc) {
            this.c = c;
            this.loc = loc;
        }

        public Copy getCopy() {
            return this.c;
        }

        private boolean hasTarget(DataLocation target) {
            return loc.isTarget(target);
        }

        @Override
        public String toString() {
            return c.getName() + " to " + loc.toString();
        }

    }

}
