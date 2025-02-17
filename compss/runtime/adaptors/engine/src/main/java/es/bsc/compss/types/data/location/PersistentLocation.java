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
package es.bsc.compss.types.data.location;

import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourcesPool;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;

import java.util.LinkedList;
import java.util.List;

import storage.StorageException;
import storage.StorageItf;


public class PersistentLocation extends DataLocation {

    private final String id;


    public PersistentLocation(String id) {
        super();
        this.id = id;
    }

    @Override
    public LocationType getType() {
        return LocationType.PERSISTENT;
    }

    @Override
    public ProtocolType getProtocol() {
        return ProtocolType.PERSISTENT_URI;
    }

    public String getId() {
        return this.id;
    }

    @Override
    public List<MultiURI> getURIs() {
        // Retrieve locations from Back-end
        List<String> locations = getLocations();
        if (locations == null) {
            ErrorManager.error("ERROR: Cannot retrieve locations of " + this.id + " from Storage Back-end");
        }

        // Retrieve URIs from hosts
        LinkedList<MultiURI> uris = new LinkedList<>();
        for (String hostName : locations) {
            Resource host = ResourcesPool.getResource(hostName);
            if (host != null) {
                uris.add(new MultiURI(ProtocolType.PERSISTENT_URI, host, this.id));
            } else {
                LOGGER.warn("Storage Back-End returned non-registered host " + hostName + ". Skipping URI in host");
            }
        }

        return uris;
    }

    @Override
    public List<Resource> getHosts() {
        LOGGER.debug("Get PSCO locations for " + this.id);

        // Retrieve locations from Back-end
        List<String> locations = getLocations();

        // Get hosts
        List<Resource> hosts = new LinkedList<>();
        for (String hostName : locations) {
            Resource host = ResourcesPool.getResource(hostName);
            if (host != null) {
                hosts.add(host);
            } else {
                LOGGER.warn("Storage Back-End returned non-registered host " + hostName);
            }
        }

        return hosts;
    }

    @Override
    public MultiURI getURIInHost(Resource targetHost) {
        // Retrieve locations from Back-end
        List<String> locations = getLocations();
        if (locations == null) {
            ErrorManager.error("ERROR: Cannot retrieve locations of " + this.id + " from Storage Back-end");
        }

        // Get URIs in host
        for (String hostName : locations) {
            if (hostName.equals(targetHost.getName())) {
                return new MultiURI(ProtocolType.PERSISTENT_URI, targetHost, this.id);
            }
        }

        return null;
    }

    private List<String> getLocations() {
        List<String> locations = null;
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.STORAGE_GETLOCATIONS);
        }
        try {
            locations = StorageItf.getLocations(this.id);
        } catch (StorageException e) {
            ErrorManager.error("ERROR: Cannot retrieve locations of " + this.id + " from Storage Back-end");
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(TraceEvent.STORAGE_GETLOCATIONS);
            }
        }
        return locations;
    }

    @Override
    public boolean isTarget(DataLocation target) {
        if (target.getType() != LocationType.PERSISTENT) {
            return false;
        }

        return this.id.equals(((PersistentLocation) target).id);
    }

    @Override
    public String toString() {
        return ProtocolType.PERSISTENT_URI.getSchema() + this.id;
    }

    @Override
    public SharedDisk getSharedDisk() {
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

    @Override
    public void modifyPath(String path) {
        // Nothing to do

    }

}
