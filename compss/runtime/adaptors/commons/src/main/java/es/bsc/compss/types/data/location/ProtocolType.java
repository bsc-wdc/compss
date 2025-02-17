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

/**
 * Supported Protocols.
 */
public enum ProtocolType {

    FILE_URI("file://"), // File protocol
    DIR_URI("dir://"), // Directory protocol
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
