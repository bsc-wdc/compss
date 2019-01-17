/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.SharedDiskManager;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class DataLocation implements Comparable<DataLocation> {

    /**
     * DataLocation Types
     *
     */
    public enum Type {
        PRIVATE, // For private objects and files
        SHARED, // For shared locations
        PERSISTENT, // For persistent storages
        BINDING // For binding objects
    }

    /**
     * Supported Protocols
     * 
     */
    public enum Protocol {
        FILE_URI("file://"), // File protocol
        SHARED_URI("shared://"), // Shared protocol
        OBJECT_URI("object://"), // Object protocol
        PERSISTENT_URI("storage://"), // Persistent protocol
        BINDING_URI("binding://"), //Binding protocol
        ANY_URI("any://"); // Other

        private final String schema;


        private Protocol(String schema) {
            this.schema = schema;
        }

        public String getSchema() {
            return this.schema;
        }

        public static Protocol getBySchema(String schema) {
            for (Protocol p : Protocol.values()) {
                if (p.schema.equals(schema)) {
                    return p;
                }
            }

            return null;
        }

    }


    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    public static final String ERROR_INVALID_LOCATION = "ERROR: Invalid location URI";
    public static final String ERROR_UNSTARTED_NODE = "ERROR: Cannot retrieve URIs from an unstarted node";


    /**
     * Creates a new location in the host @host with path @uri. The URI must: - Contain a valid schema (file://,
     * shared://, object://, storage://) - Contain a valid path - Any hostname (ignored since host is received from the
     * other parameter)
     * 
     * @param host
     * @param uri
     * @return
     * @throws Exception
     */
    public static DataLocation createLocation(Resource host, SimpleURI uri) throws IOException {
        Protocol protocol = Protocol.getBySchema(uri.getSchema());
        if (protocol == null) {
            LOGGER.warn("WARN: Unrecognised protocol [ " + uri.getSchema() + " ] for createLocation. Switching to "
                    + Protocol.ANY_URI.getSchema());
            protocol = Protocol.ANY_URI;
        }
        DataLocation loc = null;
        switch (protocol) {
            case FILE_URI:
                // Local file
                String canonicalPath = null;
                try {
                    canonicalPath = new URI(uri.getPath()).normalize().getPath();
                    if ('/' != canonicalPath.charAt(0)) {
                        canonicalPath = new File(uri.getPath()).getCanonicalPath();
                    }
                } catch (URISyntaxException e) {
                    canonicalPath = new File(uri.getPath()).getCanonicalPath();
                }
                LOGGER.debug("Creating new FileLocation: " + protocol.getSchema() + host.getName() + "@" + canonicalPath);
                loc = createLocation(Protocol.FILE_URI, host, canonicalPath);
                break;
            case SHARED_URI:
                // Shared file of the form: shared://sharedDisk/path/file
                int splitIndex = uri.getPath().indexOf(File.separator); // First slash occurrence
                String diskName = uri.getPath().substring(0, splitIndex);
                String path = uri.getPath().substring(splitIndex + 1);
                LOGGER.debug("Creating new SharedLocation: " + protocol.getSchema() + "@" + diskName + path);
                loc = new SharedLocation(Protocol.SHARED_URI, diskName, path);
                break;
            case OBJECT_URI:
                // Object
                String objectName = uri.getPath(); // The Object name is stored as path in the URI
                LOGGER.debug("Creating new ObjectLocation: " + protocol.getSchema() + host.getName() + "@" + objectName);
                loc = createLocation(Protocol.OBJECT_URI, host, objectName);
                break;
            case PERSISTENT_URI:
                String id = uri.getPath(); // The PSCO Id is stored as path in the URI
                LOGGER.debug("Creating new PersistentLocation: " + id);
                loc = new PersistentLocation(id);
                break;
            case BINDING_URI:
                // Object
                BindingObject bo = BindingObject.generate(uri.getPath()); // The Object name is stored as path in the URI
                LOGGER.debug("Creating new BindingObjectLocation: " + protocol.getSchema() + host.getName() + "@" + bo);
                loc = new BindingObjectLocation( host, bo);
                break;
            case ANY_URI:
                LOGGER.debug("Creating new AnyLocation: " + Protocol.ANY_URI.getSchema() + host.getName() + "@" + uri.getPath());
                loc = createLocation(Protocol.ANY_URI, host, uri.getPath());
                break;
        }
        return loc;
    }

    /**
     * Private Helper method for createLocation
     * 
     * @param host
     * @param path
     * @param protocol
     * @return
     */
    private static DataLocation createLocation(Protocol protocol, Resource host, String path) {
        String diskName = SharedDiskManager.getSharedName(host, path);
        if (diskName != null) {
            String mountpoint = SharedDiskManager.getMounpoint(host, diskName);
            SharedLocation sharLoc = new SharedLocation(protocol, diskName, path.substring(mountpoint.length()));
            return sharLoc;
        } else {
            PrivateLocation privLoc = new PrivateLocation(protocol, host, path);
            return privLoc;
        }
    }

    public abstract Type getType();

    public abstract Protocol getProtocol();

    public abstract List<MultiURI> getURIs();

    public abstract String getSharedDisk();

    public abstract List<Resource> getHosts();

    public abstract String getPath();
    
    public abstract void modifyPath(String path);
    
    public abstract MultiURI getURIInHost(Resource targetHost);

    public abstract boolean isTarget(DataLocation target);

    public abstract String getLocationKey();

}
