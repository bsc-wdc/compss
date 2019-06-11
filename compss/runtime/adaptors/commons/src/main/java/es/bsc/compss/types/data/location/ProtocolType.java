package es.bsc.compss.types.data.location;

/**
 * Supported Protocols.
 */
public enum ProtocolType {
    FILE_URI("file://"), // File protocol
    SHARED_URI("shared://"), // Shared protocol
    OBJECT_URI("object://"), // Object protocol
    STREAM_URI("stream://"), // Stream protocol
    EXTERNAL_STREAM_URI("extStream://"), // External Stream protocol
    PERSISTENT_URI("storage://"), // Persistent protocol
    BINDING_URI("binding://"), // Binding protocol
    ANY_URI("any://"); // Other

    private final String schema;


    private ProtocolType(String schema) {
        this.schema = schema;
    }

    public String getSchema() {
        return this.schema;
    }

    /**
     * Get protocol by Schema.
     * 
     * @param schema Scheme
     * @return Protocol related to the schema. Null if there is no protocol bind to the schema
     */
    public static ProtocolType getBySchema(String schema) {
        for (ProtocolType p : ProtocolType.values()) {
            if (p.schema.equals(schema)) {
                return p;
            }
        }

        return null;
    }

}
