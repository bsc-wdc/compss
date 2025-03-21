/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.data.listener.SafeCopyListener;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.PersistentLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.location.SharedDisk;
import es.bsc.compss.types.data.operation.copy.Copy;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.FileOpsManager;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.util.serializers.Serializer;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.ConcurrentModificationException;
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
    // 1 position array acting as a pointer to the object. Thus, all linked LogicalData share the same pointer and
    // setValue assigns the value to all Linked Data at a time
    private Object[] value;
    // Id if PSCO, null otherwise
    private String[] pscoId;
    // Id if Binding object, null otherwise
    private String[] bindingId;

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

    // Elements monitoring changes on this data's locations.
    private LinkedList<LocationMonitor> locMonitors;

    private boolean accessedByMain;

    // The data has been removed
    private boolean isDeleted;

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
        this.value = new Object[] { null };
        this.pscoId = new String[] { null };
        this.bindingId = new String[] { null };
        this.isBeingSaved = false;
        this.isBindingData = false;
        this.size = 0;
        this.locMonitors = new LinkedList<>();
        this.accessedByMain = false;
        this.isDeleted = false;
    }

    /**
     * Merges two logicalDataValues and makes it look like the same one.
     *
     * @param ld first LogicalData
     * @param ld2 second LogicalData
     * @throws CommException the values within the logicalData instances are inconsistent.
     */
    public static void link(LogicalData ld, LogicalData ld2) throws CommException {
        synchronized (ld) {
            synchronized (ld2) {
                Object valueContent = null;
                if (ld.isInMemory()) {
                    if (ld2.isInMemory()) {
                        if (ld2.getValue() != ld.getValue()) {
                            throw new CommException("Linking two LogicalData with different value in memory");
                        }
                    } else {
                        valueContent = ld.value[0];
                    }
                } else {
                    valueContent = ld2.value[0];
                }

                String[] pscoId = null;
                if (ld.getPscoId() != null) {
                    if (ld2.getPscoId() != null) {
                        if (ld2.getPscoId().compareTo(ld.getPscoId()) != 0) {
                            throw new CommException("Linking two LogicalData with different pscoId in memory");
                        }
                    } else {
                        pscoId = ld.pscoId;
                    }
                } else {
                    pscoId = ld2.pscoId;
                }

                String[] bindingId = null;
                if (ld.bindingId[0] != null) {
                    if (ld2.bindingId[0] != null) {
                        if (ld2.bindingId[0].compareTo(ld.bindingId[0]) != 0) {
                            throw new CommException("Linking two LogicalData with different value in memory");
                        }
                    } else {
                        bindingId = ld.bindingId;
                    }
                } else {
                    bindingId = ld2.bindingId;
                }

                LogicalData slave;
                LogicalData master;
                if (ld.knownAlias.size() == 1) {
                    // LD becomes LD2
                    slave = ld;
                    master = ld2;
                } else {
                    if (ld2.knownAlias.size() == 1) {
                        // LD2 becomes LD
                        slave = ld;
                        master = ld2;
                    } else {
                        throw new CommException("Linking two LogicalData with multiple links");
                    }
                }

                // Include locations for in memory values of the slave LD.
                // If the value is on slave, locations will be added later.
                if (master.isInMemory() && !slave.isInMemory()) {
                    synchronized (slave.knownAlias) {
                        for (String alias : slave.getKnownAlias()) {
                            String targetPath = ProtocolType.OBJECT_URI.getSchema() + alias;
                            try {
                                DataLocation loc;
                                SimpleURI uri = new SimpleURI(targetPath);
                                loc = DataLocation.createLocation(Comm.getAppHost(), uri);
                                synchronized (master.locations) {
                                    master.locations.add(loc);
                                }
                            } catch (Exception e) {
                                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
                            }
                        }
                    }
                }
                master.value[0] = valueContent;
                slave.value = master.value;
                master.pscoId = pscoId;
                slave.pscoId = master.pscoId;
                master.bindingId = bindingId;
                slave.bindingId = master.bindingId;

                synchronized (master.locMonitors) {
                    master.locMonitors.addAll(slave.locMonitors);
                }
                slave.locMonitors = master.locMonitors;

                synchronized (master.inProgress) {
                    master.inProgress.addAll(slave.inProgress);
                }
                slave.inProgress = master.inProgress;

                synchronized (master.locations) {
                    master.locations.addAll(slave.locations);
                }
                slave.locations = master.locations;

                synchronized (master.knownAlias) {
                    master.knownAlias.addAll(slave.knownAlias);
                }
                slave.knownAlias = master.knownAlias;
            }
        }
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
     * Checks whether a logical data is a known aliases of the same data.
     *
     * @param data data whose aliasing is to be checked
     * @return {@literal true}, if they both represent the same data; {@literal false} otherwise
     */
    public synchronized boolean isAlias(LogicalData data) {
        return data.knownAlias == this.knownAlias;
    }

    /**
     * Counts the number of alias known for the data.
     *
     * @return number of known alias for the data
     */
    public synchronized int countKnownAlias() {
        return this.knownAlias.size();
    }

    /**
     * Adds a new alias to the data, and includes the corresponding new locations if the object is in memory.
     *
     * @param alias The new alias that the data is known as
     */
    public synchronized void addKnownAlias(String alias) {
        isDeleted = false;
        if (this.knownAlias.add(alias)) {
            if (this.isInMemory()) {
                String targetPath = ProtocolType.OBJECT_URI.getSchema() + alias;
                try {
                    DataLocation loc;
                    SimpleURI uri = new SimpleURI(targetPath);
                    loc = DataLocation.createLocation(Comm.getAppHost(), uri);
                    synchronized (this.locations) {
                        this.locations.add(loc);
                    }
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
     * @param asynch Whether to perform local deletions in a synchronous or asynchronous way. {@literal true} if the
     *            deletion is asynchronous; {@literal false} otherwise.
     */
    public synchronized void removeKnownAlias(String alias, boolean asynch) {
        if (this.knownAlias.remove(alias)) {
            if (this.knownAlias.isEmpty()) {
                this.isDeleted = true;
                for (Resource res : this.getAllHosts()) {
                    res.addObsolete(this);
                }
                synchronized (this.locations) {
                    LinkedList<DataLocation> removedLocations = new LinkedList<>();
                    for (DataLocation dl : this.locations) {
                        if (deleteIfLocal(dl, asynch)) {
                            removedLocations.add(dl);
                        }
                    }
                    this.locations.removeAll(removedLocations);
                }
                value[0] = null;
            } else {
                String targetPath = ProtocolType.OBJECT_URI.getSchema() + alias;
                SimpleURI uri = new SimpleURI(targetPath);
                // There are other alias pointing to the data. Remove only
                if (this.isInMemory()) {
                    try {
                        DataLocation loc;
                        loc = DataLocation.createLocation(Comm.getAppHost(), uri);
                        synchronized (this.locations) {
                            this.locations.remove(loc);
                        }
                    } catch (Exception e) {
                        ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
                    }
                }
            }
        }
    }

    private boolean deleteIfLocal(DataLocation dl, boolean asynch) {
        MultiURI uri = dl.getURIInHost(Comm.getAppHost());
        if (uri != null && (uri.getProtocol() == ProtocolType.ANY_URI || uri.getProtocol() == ProtocolType.FILE_URI
            || uri.getProtocol() == ProtocolType.DIR_URI)) {
            if (!(dl.isCheckpointing() && this.accessedByMain)) {
                File f = new File(uri.getPath());
                if (asynch) {
                    FileOpsManager.deleteAsync(f);
                } else {
                    try {
                        FileOpsManager.deleteSync(f);
                    } catch (IOException ioe) {
                        // LOG message already printed by FileOpsManager
                    }
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Returns the PSCO id. Null if its not a PSCO
     *
     * @return
     */
    public final String getPscoId() {
        return this.pscoId[0];
    }

    /**
     * Returns all the hosts that contain a data location.
     *
     * @return
     */
    public synchronized Set<Resource> getAllHosts() {
        Set<Resource> list = new HashSet<>();
        synchronized (this.locations) {
            for (DataLocation loc : this.locations) {
                List<Resource> hosts = loc.getHosts();
                synchronized (hosts) {
                    list.addAll(hosts);
                }
            }
        }
        return list;
    }

    /**
     * Registers a new element monitoring data's locations changes.
     *
     * @param monitor element to notify location updates
     */
    public void registerLocationMonitor(LocationMonitor monitor) {
        synchronized (this.locMonitors) {
            this.locMonitors.add(monitor);
        }
    }

    /**
     * Unregisters a new element monitoring data's locations changes.
     *
     * @param monitor element to stop notifying location updates
     */
    public void unregisterLocationMonitor(LocationMonitor monitor) {
        synchronized (this.locMonitors) {
            this.locMonitors.remove(monitor);
        }
    }

    /**
     * Adds a new location and notifies all the registered monitors.
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

        List<Resource> resources = loc.getHosts();
        synchronized (this.locMonitors) {
            for (LocationMonitor readerData : this.locMonitors) {
                readerData.addedLocation(resources);
            }
        }

        this.isBeingSaved = false;
        synchronized (this.locations) {
            this.locations.add(loc);
        }
        switch (loc.getType()) {
            case PRIVATE:
                for (Resource r : loc.getHosts()) {
                    r.addLogicalData(this);
                }
                break;
            case BINDING:
                for (Resource r : loc.getHosts()) {
                    this.isBindingData = true;
                    if (this.bindingId[0] == null) {
                        this.bindingId[0] = ((BindingObjectLocation) loc).getId();
                    }
                    r.addLogicalData(this);
                }
                break;
            case SHARED:
                SharedDisk disk = loc.getSharedDisk();
                disk.addLogicalData(this);
                break;
            case PERSISTENT:
                this.pscoId[0] = ((PersistentLocation) loc).getId();
                break;
        }
        if (isDeleted) {
            deleteLocation(loc);
        }
    }

    private void deleteLocation(DataLocation loc) {
        List<Resource> resources = loc.getHosts();
        for (Resource res : resources) {
            res.addObsolete(this);
        }
        deleteIfLocal(loc, true);
    }

    /**
     * Obtain the all the URIs.
     *
     * @return
     */
    public synchronized List<MultiURI> getURIs() {
        List<MultiURI> list = new LinkedList<>();
        synchronized (this.locations) {
            for (DataLocation loc : this.locations) {
                List<MultiURI> locationURIs = loc.getURIs();
                // Adds all the valid locations
                if (locationURIs != null) {
                    list.addAll(locationURIs);
                }
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
        synchronized (this.locations) {
            for (DataLocation loc : this.locations) {
                MultiURI locationURI = loc.getURIInHost(targetHost);
                if (locationURI != null) {
                    list.add(locationURI);
                }
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
    public final synchronized boolean isInMemory() {
        return (this.value[0] != null);
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
    public final synchronized Object getValue() {
        return this.value[0];
    }

    /**
     * Removes the location as a source for the data.
     *
     * @param loc Location to remove
     */
    public synchronized void removeLocation(DataLocation loc) {
        synchronized (this.locations) {
            this.locations.remove(loc);
        }
    }

    /**
     * Removes the object from master main memory and removes its location.
     *
     * @return
     */
    public synchronized Object removeValue() {
        for (String alias : getKnownAlias()) {
            DataLocation loc = null;

            String targetPath = ProtocolType.OBJECT_URI.getSchema() + alias;
            try {
                SimpleURI uri = new SimpleURI(targetPath);
                loc = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
            }
            // Removes only the memory location (no need to check private, shared, persistent)
            synchronized (this.locations) {
                this.locations.remove(loc);
            }
        }

        Object val = this.value[0];
        this.value[0] = null;

        return val;
    }

    /**
     * Sets the memory value.
     *
     * @param o Object value
     */
    public synchronized void setValue(Object o) {
        this.value[0] = o;
    }

    /**
     * Sets the PSCO id of a logical data.
     *
     * @param id PSCO Identifier
     */
    public synchronized void setPscoId(String id) {
        this.pscoId[0] = id;
    }

    /**
     * Writes memory value to file.
     *
     * @throws Exception Error writing to storage
     */
    public synchronized void writeToStorage() throws Exception {
        if (DEBUG) {
            LOGGER.debug(DBG_PREFIX + "Writting object " + this.name + " to storage");
        }
        if (isBindingData) {
            String targetPath = Comm.getAppHost().getWorkingDirectory() + this.name;
            String id;
            // decide the id where the object is stored in the binding
            if (this.bindingId[0] != null) {
                id = this.bindingId[0];
            } else {
                if (this.value[0] != null) {
                    id = (String) this.value[0];
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
            if (this.pscoId[0] != null) {
                // It is a persistent object that is already persisted
                // Nothing to do
                // If the PSCO is not persisted we treat it as a normal object
            } else {

                // The object must be written to file
                String targetPath = Comm.getAppHost().getWorkingDirectory() + this.name;
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Writting object " + this.name + " to file " + targetPath);
                }
                Serializer.serialize(value[0], targetPath);
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
        synchronized (this.locations) {
            this.locations.add(loc);
        }
        for (Resource r : loc.getHosts()) {
            switch (loc.getType()) {
                case BINDING:
                case PRIVATE:
                    r.addLogicalData(this);
                    break;
                case SHARED:
                    SharedDisk disk = loc.getSharedDisk();
                    disk.addLogicalData(this);
                    break;
                case PERSISTENT:
                    // Nothing to do
                    break;
            }
        }
    }

    /**
     * Reads the value of the LogicalData from a file but does not keep it as the value.
     *
     * @throws CannotLoadException Error loading from storage
     * @returns the value of the LogicalData
     */
    public synchronized Object readFromStorage() throws CannotLoadException {
        synchronized (this.locations) {
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
                                return Serializer.deserialize(path);
                            } catch (ClassNotFoundException | IOException e) {
                                // Check next location since deserialization was invalid
                                this.value[0] = null;
                                continue;
                            }
                        }
                        break;
                    case PERSISTENT:
                        PersistentLocation pLoc = (PersistentLocation) loc;
                        if (Tracer.isActivated()) {
                            Tracer.emitEvent(TraceEvent.STORAGE_GETBYID);
                        }
                        try {
                            this.pscoId[0] = pLoc.getId();
                            return StorageItf.getByID(this.pscoId[0]);
                        } catch (StorageException se) {
                            // Check next location since cannot retrieve the object from the storage Back-end
                            continue;
                        } finally {
                            if (Tracer.isActivated()) {
                                Tracer.emitEventEnd(TraceEvent.STORAGE_GETBYID);
                            }
                        }
                    case BINDING:
                        // We should never reach this
                        throw new CannotLoadException("ERROR: Trying to load from storage a BINDING location");
                }
            }
        }
        // Any location has been able to load the value
        throw new CannotLoadException("Object has not any valid location available in the master");
    }

    /**
     * Loads the value of the LogicalData from a file.
     *
     * @throws CannotLoadException Error loading from storage
     */
    public synchronized void loadFromStorage() throws CannotLoadException {
        // TODO: Check if we have to do something in binding data??
        if (value[0] != null) {
            // Value is already loaded in memory
            return;
        }
        synchronized (this.locations) {
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
                                this.value[0] = Serializer.deserialize(path);
                            } catch (ClassNotFoundException | IOException e) {
                                // Check next location since deserialization was invalid
                                this.value[0] = null;
                                continue;
                            }
                            try {
                                addLocationsForInMemoryObject();
                            } catch (IOException e) {
                                // Check next location since location was invalid
                                this.value[0] = null;
                                continue;
                            }
                        }

                        return;
                    case PERSISTENT:
                        PersistentLocation pLoc = (PersistentLocation) loc;

                        if (Tracer.isActivated()) {
                            Tracer.emitEvent(TraceEvent.STORAGE_GETBYID);
                        }
                        try {
                            this.value[0] = StorageItf.getByID(pLoc.getId());
                            this.pscoId[0] = pLoc.getId();
                        } catch (StorageException se) {
                            // Check next location since cannot retrieve the object from the storage Back-end
                            continue;
                        } finally {
                            if (Tracer.isActivated()) {
                                Tracer.emitEventEnd(TraceEvent.STORAGE_GETBYID);
                            }
                        }

                        try {
                            addLocationsForInMemoryObject();
                        } catch (IOException e) {
                            // Check next location since location was invalid
                            this.value[0] = null;
                            continue;
                        }

                        return;
                    case BINDING:
                        // We should never reach this
                        throw new CannotLoadException("ERROR: Trying to load from storage a BINDING location");
                }
            }
        }
        // Any location has been able to load the value
        throw new CannotLoadException("Object has not any valid location available in the master");
    }

    private void addLocationsForInMemoryObject() throws IOException {
        synchronized (this.locations) {
            LinkedList<DataLocation> locations = new LinkedList<>();
            for (String alias : this.knownAlias) {
                String targetPath = ProtocolType.OBJECT_URI.getSchema() + alias;
                SimpleURI uri = new SimpleURI(targetPath);
                DataLocation tgtLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
                locations.add(tgtLoc);
            }
            // Loop splitted just in case that 1 location cannot be created. It raises an exception and adds no new
            // location
            for (DataLocation loc : locations) {
                this.isBeingSaved = false;
                this.locations.add(loc);
                Comm.getAppHost().addLogicalData(this);
            }
        }
    }

    /**
     * Removes all the locations assigned to a given host and returns a valid location if the file is unique.
     *
     * @param host Resource
     * @param sharedMountPoints map relating all the shared disks in the resource and their corresponding mountpoints
     * @return a valid location if the file is unique
     */
    public synchronized DataLocation removeHostAndCheckLocationToSave(Resource host,
        Map<SharedDisk, String> sharedMountPoints) {
        // If the file is being saved means that this function has already been executed
        // for the same LogicalData. Thus, all the host locations are already removed
        // and there is no unique file to save
        if (isBeingSaved) {
            return null;
        }
        // Otherwise, we must remove all the host locations and store a unique
        // location if needed. We only store the "best" location if any (by
        // choosing
        // any private location found or the first shared location)
        DataLocation uniqueHostLocation = null;
        synchronized (this.locations) {
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
                        // When calling this function, the host inside the
                        // SharedDiskManager has been removed
                        // If there are no remaining hosts it means it was the last
                        // host thus, the location
                        // is unique and must be saved
                        if (loc.getHosts().isEmpty()) {
                            SharedDisk disk = loc.getSharedDisk();
                            if (disk != null) {
                                String mountPoint = sharedMountPoints.get(disk);
                                if (mountPoint != null) {
                                    if (uniqueHostLocation == null) {
                                        this.isBeingSaved = true;

                                        String targetPath = ProtocolType.FILE_URI.getSchema() + loc.getPath();
                                        try {
                                            SimpleURI uri = new SimpleURI(targetPath);
                                            uniqueHostLocation = DataLocation.createLocation(host, uri);
                                        } catch (Exception e) {
                                            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath,
                                                e);
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
    }

    /**
     * Returns the copies in progress.
     *
     * @return
     */
    public synchronized Collection<Copy> getCopiesInProgress() {
        List<Copy> copies = new LinkedList<>();
        synchronized (this.inProgress) {
            for (CopyInProgress cp : this.inProgress) {
                copies.add(cp.getCopy());
            }
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
        synchronized (this.locations) {
            for (DataLocation loc : locations) {
                MultiURI u = loc.getURIInHost(targetHost);
                // If we have found a valid location, return it
                if (u != null) {
                    return u;
                }
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
        synchronized (this.inProgress) {
            for (CopyInProgress cip : this.inProgress) {
                if (cip.hasTarget(target)) {
                    return cip.getCopy();
                }
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
        synchronized (this.inProgress) {
            this.inProgress.add(new CopyInProgress(c, target));
        }
    }

    /**
     * Marks the end of a copy. Returns the location of the finished copy or null if not found.
     *
     * @param c Copy
     * @return
     */
    public synchronized DataLocation finishedCopy(Copy c) {
        DataLocation loc = null;
        synchronized (this.inProgress) {
            Iterator<CopyInProgress> it = this.inProgress.iterator();
            while (it.hasNext()) {
                CopyInProgress cip = it.next();
                if (cip.c == c) {
                    it.remove();
                    loc = cip.loc;
                    break;
                }
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
        synchronized (this.inProgress) {
            for (CopyInProgress cip : this.inProgress) {
                listener.addOperation();
                cip.c.addEventListener(listener);
            }
        }
    }

    public void setAccessedByMain(boolean accessedByMain) {
        this.accessedByMain = accessedByMain;
    }

    public boolean isAccessedByMain() {
        return accessedByMain;
    }

    @Override
    public synchronized String toString() {
        while (true) {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("Logical Data name: ").append(this.name).append("\n");
                sb.append("Aliases:");
                for (String alias : this.knownAlias) {
                    sb.append(" ").append(alias);
                }
                sb.append("\n");
                sb.append("\t Value: ").append(value[0]).append("\n");
                sb.append("\t Id: ").append(pscoId[0]).append("\n");
                sb.append("\t Locations:\n");
                for (DataLocation dl : locations) {
                    sb.append("\t\t * ").append(dl).append("\n");
                }
                return sb.toString();
            } catch (ConcurrentModificationException cme) {
                // repeat
            }
        }
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
