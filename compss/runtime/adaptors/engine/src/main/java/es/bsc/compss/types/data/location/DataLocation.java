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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class DataLocation implements Comparable<DataLocation> {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    public static final String ERROR_INVALID_LOCATION = "ERROR: Invalid location URI";
    public static final String ERROR_UNSTARTED_NODE = "ERROR: Cannot retrieve URIs from an unstarted node";

    private boolean isCheckpoint = false;


    /**
     * Creates a new location in the host @host with path @uri. The URI must: - Contain a valid schema (file://,
     * shared://, object://, storage://) - Contain a valid path - Any hostname (ignored since host is received from the
     * other parameter)
     * 
     * @param host Location Resource
     * @param uri Location URI
     * @return Data location object
     * @throws IOException Error creating location
     */
    public static DataLocation createLocation(Resource host, SimpleURI uri) throws IOException {
        ProtocolType protocol = ProtocolType.getBySchema(uri.getSchema());
        if (protocol == null) {
            LOGGER.warn("WARN: Unrecognised protocol [ " + uri.getSchema() + " ] for createLocation. Switching to "
                + ProtocolType.ANY_URI.getSchema());
            protocol = ProtocolType.ANY_URI;
        }
        DataLocation loc = null;
        switch (protocol) {
            case DIR_URI:
            case FILE_URI:
                // Local file or dir
                String canonicalPath = null;
                try {
                    canonicalPath = new URI(uri.getPath()).normalize().getPath();
                    if ('/' != canonicalPath.charAt(0)) {
                        canonicalPath = new File(uri.getPath()).getCanonicalPath();
                    }
                } catch (URISyntaxException e) {
                    canonicalPath = new File(uri.getPath()).getCanonicalPath();
                }
                LOGGER
                    .debug("Creating new FileLocation: " + protocol.getSchema() + host.getName() + "@" + canonicalPath);
                loc = createLocation(protocol, host, canonicalPath);
                break;
            case SHARED_URI:
                // Shared file of the form: shared://sharedDisk/path/file
                int splitIndex = uri.getPath().indexOf(File.separator); // First slash occurrence
                String diskName = uri.getPath().substring(0, splitIndex);
                String path = uri.getPath().substring(splitIndex + 1);
                LOGGER.debug("Creating new SharedLocation: " + protocol.getSchema() + "@" + diskName + path);
                loc = new SharedLocation(ProtocolType.SHARED_URI, diskName, path);
                break;
            case OBJECT_URI:
                // Object
                String objectName = uri.getPath(); // The Object name is stored as path in the URI
                LOGGER
                    .debug("Creating new ObjectLocation: " + protocol.getSchema() + host.getName() + "@" + objectName);
                loc = createLocation(ProtocolType.OBJECT_URI, host, objectName);
                break;
            case STREAM_URI:
                // Stream
                String streamName = uri.getPath(); // The Object name is stored as path in the URI
                LOGGER
                    .debug("Creating new StreamLocation: " + protocol.getSchema() + host.getName() + "@" + streamName);
                loc = createLocation(ProtocolType.STREAM_URI, host, streamName);
                break;
            case EXTERNAL_STREAM_URI:
                // External stream
                String streamCompletePath = null;
                try {
                    streamCompletePath = new URI(uri.getPath()).normalize().getPath();
                    if ('/' != streamCompletePath.charAt(0)) {
                        streamCompletePath = new File(uri.getPath()).getCanonicalPath();
                    }
                } catch (URISyntaxException e) {
                    streamCompletePath = new File(uri.getPath()).getCanonicalPath();
                }
                LOGGER.debug("Creating new ExternalStreamLocation: " + protocol.getSchema() + host.getName() + "@"
                    + streamCompletePath);
                loc = createLocation(ProtocolType.EXTERNAL_STREAM_URI, host, streamCompletePath);
                break;
            case PERSISTENT_URI:
                String id = uri.getPath(); // The PSCO Id is stored as path in the URI
                LOGGER.debug("Creating new PersistentLocation: " + id);
                loc = new PersistentLocation(id);
                break;
            case BINDING_URI:
                // Binding Object
                // The Object name is stored as path in the URI
                LOGGER.debug("Creating Binding URI from path");
                BindingObject bo = BindingObject.generate(uri.getPath());
                LOGGER.debug("Creating new BindingObjectLocation: " + protocol.getSchema() + host.getName() + "@" + bo);
                loc = new BindingObjectLocation(host, bo);
                break;
            case ANY_URI:
                LOGGER.debug("Creating new AnyLocation: " + ProtocolType.ANY_URI.getSchema() + host.getName() + "@"
                    + uri.getPath());
                loc = createLocation(ProtocolType.ANY_URI, host, uri.getPath());
                break;
        }
        return loc;
    }

    /**
     * Private Helper method for createLocation.
     * 
     * @param host Resource
     * @param path Path
     * @param protocol Protocol
     * @return Data location object
     */
    private static DataLocation createLocation(ProtocolType protocol, Resource host, String path) {
        SharedDisk disk = host.getSharedDiskFromPath(path);
        if (disk != null) {
            String mountpoint = disk.getMountpoint(host);
            SharedLocation sharLoc = new SharedLocation(protocol, disk, path.substring(mountpoint.length()));
            return sharLoc;
        } else {
            PrivateLocation privLoc = new PrivateLocation(protocol, host, path);
            return privLoc;
        }
    }

    public abstract LocationType getType();

    public abstract ProtocolType getProtocol();

    public abstract List<MultiURI> getURIs();

    public abstract SharedDisk getSharedDisk();

    public abstract List<Resource> getHosts();

    public abstract String getPath();

    public abstract void modifyPath(String path);

    public abstract MultiURI getURIInHost(Resource targetHost);

    public abstract boolean isTarget(DataLocation target);

    public abstract String getLocationKey();

    /**
     * Configures whether the location is used as a checkpoint for future executions.
     * 
     * @param checkpointing {@literal true} if it is used for checkpointing a data value
     */
    public void isCheckpointLocation(boolean checkpointing) {
        this.isCheckpoint = checkpointing;
    }

    /**
     * Returns whether the location is used as a checkpoint for future executions or not.
     * 
     * @return {@literal true} if it is used for checkpointing a data value; {@literal false}, otherwise.
     */
    public boolean isCheckpointing() {
        return this.isCheckpoint;
    }

}
