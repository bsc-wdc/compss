/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.data;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.data.BindingDataManager;
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.CommException;
import es.bsc.compss.types.data.listener.SafeCopyListener;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.PersistentLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Serializer;
import es.bsc.compss.util.SharedDiskManager;
import es.bsc.compss.util.TraceEvent;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;


public class LogicalData {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String DBG_PREFIX = "[LogicalData] ";
    // Logical data name
    private final String name;
    // Value in memory, null if value in disk
    private Object value;
    // Id if PSCO, null otherwise
    private String pscoId;
    // Id if Binding object, null otherwise
    private String bindingId;

    // List of names the identify the value
    private Set<String> knownAlias = new TreeSet<>();
    // List of existing copies
    private Set<DataLocation> locations = new TreeSet<>();
    // In progress
    private List<CopyInProgress> inProgress = new LinkedList<>();
    // File's size.
    private float size;

    // Indicates if LogicalData has been ordered to save before
    private boolean isBeingSaved;
    private boolean isBindingData;


    /*
     * Constructors
     */
    /**
     * Constructs a LogicalData for a given data version.
     *
     * @param name Data name
     */
    public LogicalData(String name) {
        this.name = name;
        this.knownAlias.add(name);
        this.value = null;
        this.pscoId = null;
        this.bindingId = null;
        this.isBeingSaved = false;
        this.isBindingData = false;
        this.size = 0;
    }

    /**
     * Merges two logicalDataValues and makes it look like the same one.
     *
     * @param ld first LogicalData
     * @param ld2 second LogicalData
     * @throws CommException the values within the logicalData instances are inconsistent.
     */
    public static void link(LogicalData ld, LogicalData ld2) throws CommException {
        Object value = null;
        String pscoId = null;
        String bindingId = null;
        if (ld.value != null) {
            if (ld2.value != null) {
                if (ld2.value != ld.value) {
                    throw new CommException("Linking two LogicalData with different value in memory");
                }
            } else {
                value = ld.value;
            }
        } else {
            value = ld2.value;
        }
        if (ld.pscoId != null) {
            if (ld2.pscoId != null) {
                if (ld2.pscoId.compareTo(ld.pscoId) != 0) {
                    throw new CommException("Linking two LogicalData with different pscoId in memory");
                }
            } else {
                pscoId = ld.pscoId;
            }
        } else {
            pscoId = ld2.pscoId;
        }
        if (ld.bindingId != null) {
            if (ld2.bindingId != null) {
                if (ld2.bindingId.compareTo(ld.bindingId) != 0) {
                    throw new CommException("Linking two LogicalData with different value in memory");
                }
            } else {
                bindingId = ld.bindingId;
            }
        } else {
            bindingId = ld2.bindingId;
        }

        if (ld.isInMemory()) {
            if (!ld2.isInMemory()) {
                for (String alias : ld2.knownAlias) {
                    ld.addKnownAlias(alias);
                }
                ld2.knownAlias = ld.knownAlias;
            } // If they both have the same value, it will be locations will be added later on
        } else {
            if (ld2.isInMemory()) {
                for (String alias : ld.knownAlias) {
                    ld2.addKnownAlias(alias);
                }
                ld.knownAlias = ld2.knownAlias;
            } else {
                ld.knownAlias.addAll(ld2.knownAlias);
                ld2.knownAlias = ld.knownAlias;
            }
        }

        ld.value = value;
        ld2.value = value;
        ld.pscoId = pscoId;
        ld2.pscoId = pscoId;
        ld.bindingId = bindingId;
        ld2.bindingId = bindingId;
        ld.locations.addAll(ld2.locations);
        ld2.locations = ld.locations;
        ld.inProgress.addAll(ld2.inProgress);
        ld2.inProgress = ld.inProgress;
    }

    /**
     * Returns the data version name.
     *
     * @return
     */
    public String getName() {
        // No need to sync because it cannot be modified
        return this.name;
    }

    /**
     * Returns the set with all the known alias for the data.
     *
     * @return set containing all the known alias for the data.
     */
    public Set<String> getKnownAlias() {
        return knownAlias;
    }

    /**
     * Adds a new alias to the data, and includes the corresponding new locations if the object is in memory.
     *
     * @param name The new alias that the data is known as
     */
    public void addKnownAlias(String name) {
        if (this.knownAlias.add(name)) {
            if (this.isInMemory()) {
                String targetPath = ProtocolType.OBJECT_URI.getSchema() + name;
                try {
                    DataLocation loc;
                    SimpleURI uri = new SimpleURI(targetPath);
                    loc = DataLocation.createLocation(Comm.getAppHost(), uri);
                    this.locations.add(loc);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
                }
            }
        }
    }

    /**
     * Removes an alias from the data. If no more aliases are known for that data, all the values stored locally on the
     * devices, either on memory or disk, are removed.
     *
     * @param alias Alias to remove from the list of known aliases.
     */
    public void removeKnownAlias(String alias) {
        if (this.knownAlias.remove(alias)) {
            if (this.knownAlias.isEmpty()) {
                for (Resource res : this.getAllHosts()) {
                    res.addObsolete(this);
                }
                for (DataLocation dl : this.locations) {
                    MultiURI uri = dl.getURIInHost(Comm.getAppHost());
                    if (uri != null) {
                        File f = new File(uri.getPath());
                        if (f.exists()) {
                            LOGGER.info("Deleting file " + f.getAbsolutePath());
                            if (!f.delete()) {
                                LOGGER.error("Cannot delete file " + f.getAbsolutePath());
                            } else {
                                if (f.isDirectory()) {
                                    // directories must be removed recursively
                                    Path directory = Paths.get(uri.getPath());
                                    try {
                                        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {

                                            @Override
                                            public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
                                                throws IOException {
                                                Files.delete(file);
                                                return FileVisitResult.CONTINUE;
                                            }

                                            @Override
                                            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                                                throws IOException {
                                                Files.delete(dir);
                                                return FileVisitResult.CONTINUE;
                                            }
                                        });
                                    } catch (IOException e) {
                                        LOGGER.error("Cannot delete directory " + f.getAbsolutePath());
                                    }
                                }
                            }
                        }
                    }
                }
                value = null;
            } else {
                String targetPath = ProtocolType.OBJECT_URI.getSchema() + alias;
                SimpleURI uri = new SimpleURI(targetPath);
                for (Resource res : this.getAllHosts()) {
                    try {
                        LogicalData ld = new LogicalData(alias);
                        DataLocation loc = DataLocation.createLocation(res, uri);
                        ld.addLocation(loc);
                        this.locations.remove(loc);
                        res.addObsolete(ld);
                    } catch (Exception e) {
                        ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
                    }
                }
                // There are other alias pointing to the data. Remove only
                if (this.isInMemory()) {
                    try {
                        DataLocation loc;
                        loc = DataLocation.createLocation(Comm.getAppHost(), uri);
                        this.locations.remove(loc);
                    } catch (Exception e) {
                        ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
                    }
                }
            }
        }
    }

    /**
     * Returns the PSCO id. Null if its not a PSCO
     *
     * @return
     */
    public String getPscoId() {
        return this.pscoId;
    }

    /**
     * Returns all the hosts that contain a data location.
     *
     * @return
     */
    public synchronized Set<Resource> getAllHosts() {
        Set<Resource> list = new HashSet<>();
        for (DataLocation loc : this.locations) {
            List<Resource> hosts = loc.getHosts();
            synchronized (hosts) {
                list.addAll(hosts);
            }
        }

        return list;
    }

    /**
     * Adds a new location.
     *
     * @param loc New location
     */
    public synchronized void addLocation(DataLocation loc) {
        if (loc.getProtocol() == ProtocolType.OBJECT_URI && loc.getHosts().contains(Comm.getAppHost())) {
            // Is registering the location of the object on main memory.
            try {
                addLocationsForInMemoryObject();
                return;
            } catch (Exception e) {
                ErrorManager.error("ERROR generating a new location for the object in memory for data " + this.name, e);
            }
        }
        this.isBeingSaved = false;
        this.locations.add(loc);
        switch (loc.getType()) {
            case PRIVATE:
                for (Resource r : loc.getHosts()) {
                    r.addLogicalData(this);
                }
                break;
            case BINDING:
                for (Resource r : loc.getHosts()) {
                    this.isBindingData = true;
                    if (this.bindingId == null) {
                        this.bindingId = ((BindingObjectLocation) loc).getId();
                    }
                    r.addLogicalData(this);
                }
                break;
            case SHARED:
                SharedDiskManager.addLogicalData(loc.getSharedDisk(), this);
                break;
            case PERSISTENT:
                this.pscoId = ((PersistentLocation) loc).getId();
                break;
        }
    }

    /**
     * Obtain the all the URIs.
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

    /**
     * Obtain all URIs in a resource.
     *
     * @param targetHost Resource
     * @return list of uri where data is located in the node
     */
    public synchronized List<MultiURI> getURIsInHost(Resource targetHost) {
        List<MultiURI> list = new LinkedList<>();
        for (DataLocation loc : this.locations) {
            MultiURI locationURI = loc.getURIInHost(targetHost);
            if (locationURI != null) {
                list.add(locationURI);
            }
        }
        return list;
    }

    public synchronized Set<DataLocation> getLocations() {
        return this.locations;
    }

    public synchronized void setSize(float size) {
        this.size = size;
    }

    public float getSize() {
        return this.size;
    }

    /**
     * Returns if the data value is stored in memory or not.
     *
     * @return
     */
    public synchronized boolean isInMemory() {
        return (this.value != null);
    }

    /**
     * Returns if the data is binding data.
     *
     * @return
     */
    public synchronized boolean isBindingData() {
        return isBindingData;
    }

    /**
     * Returns the value stored in memory.
     *
     * @return
     */
    public synchronized Object getValue() {
        return this.value;
    }

    /**
     * Removes the object from master main memory and removes its location.
     *
     * @return
     */
    public synchronized Object removeValue() {
        DataLocation loc = null;
        String targetPath = ProtocolType.OBJECT_URI.getSchema() + this.name;
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
     * Sets the memory value.
     *
     * @param o Object value
     */
    public synchronized void setValue(Object o) {
        this.value = o;
    }

    /**
     * Sets the PSCO id of a logical data.
     *
     * @param id PSCO Identifier
     */
    public synchronized void setPscoId(String id) {
        this.pscoId = id;
    }

    /**
     * Writes memory value to file.
     *
     * @throws Exception Error writting to storage
     */
    public synchronized void writeToStorage() throws Exception {
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Writting object " + this.name + " to storage");
        }
        if (isBindingData) {
            String targetPath = Comm.getAppHost().getWorkingDirectory() + this.name;
            String id;
            // decide the id where the object is stored in the binding
            if (this.bindingId != null) {
                id = this.bindingId;
            } else {
                if (this.value != null) {
                    id = (String) this.value;
                } else {
                    id = this.name;
                }
            }
            if (id.contains("#")) {
                id = BindingObject.generate(id).getName();
            }
            if (BindingDataManager.isInBinding(id)) {
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Writting binding object " + id + " to file " + targetPath);
                }
                BindingDataManager.storeInFile(id, targetPath);
                addWrittenObjectLocation(targetPath);
            } else {
                LOGGER.error(DBG_PREFIX + " Error " + id + " not found in binding");
                throw (new Exception(" Error " + id + " not found in binding"));
            }
        } else {
            if (this.pscoId != null) {
                // It is a persistent object that is already persisted
                // Nothing to do
                // If the PSCO is not persisted we treat it as a normal object
            } else {

                // The object must be written to file
                String targetPath = Comm.getAppHost().getWorkingDirectory() + this.name;
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Writting object " + this.name + " to file " + targetPath);
                }
                Serializer.serialize(value, targetPath);
                addWrittenObjectLocation(targetPath);
            }
        }
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Object " + this.name + " written to storage");
        }
    }

    private void addWrittenObjectLocation(String targetPath) throws IOException {
        String targetPathWithSchema = ProtocolType.FILE_URI.getSchema() + targetPath;
        SimpleURI targetURI = new SimpleURI(targetPathWithSchema);
        DataLocation loc = DataLocation.createLocation(Comm.getAppHost(), targetURI);
        this.isBeingSaved = false;
        this.locations.add(loc);
        for (Resource r : loc.getHosts()) {
            switch (loc.getType()) {
                case BINDING:
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

    /**
     * Loads the value of the LogicalData from a file.
     *
     * @throws CannotLoadException Error loading from storage
     */
    public synchronized void loadFromStorage() throws CannotLoadException {
        // TODO: Check if we have to do something in binding data??
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
                        try {
                            addLocationsForInMemoryObject();
                        } catch (IOException e) {
                            // Check next location since location was invalid
                            this.value = null;
                            continue;
                        }
                    }

                    return;
                case PERSISTENT:
                    PersistentLocation pLoc = (PersistentLocation) loc;

                    if (Tracer.extraeEnabled()) {
                        Tracer.emitEvent(TraceEvent.STORAGE_GETBYID.getId(), TraceEvent.STORAGE_GETBYID.getType());
                    }
                    try {
                        this.value = StorageItf.getByID(pLoc.getId());
                        this.pscoId = pLoc.getId();
                    } catch (StorageException se) {
                        // Check next location since cannot retrieve the object from the storage Back-end
                        continue;
                    } finally {
                        if (Tracer.extraeEnabled()) {
                            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.STORAGE_GETBYID.getType());
                        }
                    }

                    try {
                        addLocationsForInMemoryObject();
                    } catch (IOException e) {
                        // Check next location since location was invalid
                        this.value = null;
                        continue;
                    }

                    return;
                case BINDING:
                    // We should never reach this
                    throw new CannotLoadException("ERROR: Trying to load from storage a BINDING location");
            }
        }

        // Any location has been able to load the value
        throw new CannotLoadException("Object has not any valid location available in the master");
    }

    private void addLocationsForInMemoryObject() throws IOException {
        LinkedList<DataLocation> locations = new LinkedList();
        for (String alias : this.knownAlias) {
            String targetPath = ProtocolType.OBJECT_URI.getSchema() + alias;
            SimpleURI uri = new SimpleURI(targetPath);
            DataLocation tgtLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
            locations.add(tgtLoc);
        }
        // Loop splitted just in case that 1 location cannot be created. It raises an exception and adds no new location
        for (DataLocation loc : locations) {
            this.isBeingSaved = false;
            this.locations.add(loc);
            Comm.getAppHost().addLogicalData(this);
        }
    }

    /**
     * Removes all the locations assigned to a given host and returns a valid location if the file is unique.
     *
     * @param host Resource
     * @param sharedMountPoints Shared mount point
     * @return a valid location if the file is unique
     */
    public synchronized DataLocation removeHostAndCheckLocationToSave(Resource host,
        Map<String, String> sharedMountPoints) {
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
        DataLocation uniqueHostLocation = null;
        Iterator<DataLocation> it = this.locations.iterator();
        while (it.hasNext()) {
            DataLocation loc = it.next();
            switch (loc.getType()) {
                case BINDING:
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

                                    String targetPath = ProtocolType.FILE_URI.getSchema() + loc.getPath();
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
        return uniqueHostLocation;
    }

    /**
     * Returns the copies in progress.
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
     * Returns if the data is already available in a given targetHost.
     *
     * @param targetHost target resource
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
     * Returns if a copy of the LogicalData is being performed to a target host.
     *
     * @param target Target location
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
     * Begins a copy of the LogicalData to a target host.
     *
     * @param c Copy
     * @param target Target data location
     */
    public synchronized void startCopy(Copy c, DataLocation target) {
        this.inProgress.add(new CopyInProgress(c, target));
    }

    /**
     * Marks the end of a copy. Returns the location of the finished copy or null if not found.
     *
     * @param c Copy
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
     * Adds a listener to the inProgress copies.
     *
     * @param listener Copy listener
     */
    public synchronized void notifyToInProgressCopiesEnd(SafeCopyListener listener) {
        for (CopyInProgress cip : this.inProgress) {
            listener.addOperation();
            cip.c.addEventListener(listener);
        }
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Logical Data name: ").append(this.name).append("\n");
        sb.append("\t Value: ").append(value).append("\n");
        sb.append("\t Id: ").append(pscoId).append("\n");
        sb.append("\t Locations:\n");
        synchronized (locations) {
            for (DataLocation dl : locations) {
                sb.append("\t\t * ").append(dl).append("\n");
            }
        }
        return sb.toString();

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
