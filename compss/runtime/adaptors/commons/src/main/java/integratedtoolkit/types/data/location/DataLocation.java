package integratedtoolkit.types.data.location;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.SharedDiskManager;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class DataLocation implements Comparable<DataLocation> {

    /**
     * DataLocation Types
     *
     */
    public enum Type {
        PRIVATE, SHARED, PERSISTENT
    }

    /**
     * Supported Protocols
     * 
     */
    public enum Protocol {
        FILE_URI("file://"), SHARED_URI("shared://"), OBJECT_URI("object://"), PERSISTENT_URI("storage://"), ANY_URI("any://");

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
    protected static final Logger logger = LogManager.getLogger(Loggers.COMM);
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
            logger.warn("WARN: Unrecognised protocol [ " + uri.getSchema() + " ] for createLocation. Switching to "
                    + Protocol.ANY_URI.getSchema());
            protocol = Protocol.ANY_URI;
        }

        DataLocation loc = null;
        switch (protocol) {
            case FILE_URI:
                // Local file
                String canonicalPath = new File(uri.getPath()).getCanonicalPath();
                logger.debug("Creating new FileLocation: " + protocol.getSchema() + host.getName() + "@" + canonicalPath);
                loc = createLocation(Protocol.FILE_URI, host, canonicalPath);
                break;
            case SHARED_URI:
                // Shared file of the form: shared://sharedDisk/path/file
                int splitIndex = uri.getPath().indexOf(File.separator); // First slash occurrence
                String diskName = uri.getPath().substring(0, splitIndex);
                String path = uri.getPath().substring(splitIndex + 1);
                logger.debug("Creating new SharedLocation: " + protocol.getSchema() + "@" + diskName + path);
                loc = new SharedLocation(Protocol.SHARED_URI, diskName, path);
                break;
            case OBJECT_URI:
                // Object
                String objectName = uri.getPath(); // The Object name is stored as path in the URI
                logger.debug("Creating new ObjectLocation: " + protocol.getSchema() + host.getName() + "@" + objectName);
                loc = createLocation(Protocol.OBJECT_URI, host, objectName);
                break;
            case PERSISTENT_URI:
                String id = uri.getPath(); // The PSCO Id is stored as path in the URI
                logger.debug("Creating new PersistentLocation: " + id);
                loc = new PersistentLocation(id);
                break;
            case ANY_URI:
                logger.debug("Creating new AnyLocation: " + Protocol.ANY_URI.getSchema() + host.getName() + "@" + uri.getPath());
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
            return new SharedLocation(protocol, diskName, path.substring(mountpoint.length()));
        } else {
            return new PrivateLocation(protocol, host, path);
        }
    }

    public abstract Type getType();

    public abstract Protocol getProtocol();

    public abstract LinkedList<MultiURI> getURIs();

    public abstract String getSharedDisk();

    public abstract LinkedList<Resource> getHosts();

    public abstract String getPath();

    public abstract MultiURI getURIInHost(Resource targetHost);

    public abstract boolean isTarget(DataLocation target);

    public abstract String getLocationKey();

}
