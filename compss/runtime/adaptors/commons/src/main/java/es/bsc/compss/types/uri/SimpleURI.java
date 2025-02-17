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
package es.bsc.compss.types.uri;

import java.io.PrintStream;


public class SimpleURI {

    private static final String SCHEMA_SEPARATOR = "://";
    private static final String HOSTNAME_SEPARATOR = "@";

    private final String schema;
    private final String hostname;
    private final String path;


    /**
     * Simple URI Constructor.
     * 
     * @param scheme URI scheme
     * @param hostname URI hostname
     * @param path URI path
     */
    public SimpleURI(String scheme, String hostname, String path) {
        this.schema = scheme;
        this.hostname = hostname;
        this.path = path;
    }

    /**
     * Simple URI constructor.
     * 
     * @param fullPath URI absolute path.
     */
    public SimpleURI(String fullPath) {
        int parsedIndex = 0;

        // Get schema
        if (fullPath.contains(SCHEMA_SEPARATOR)) {
            int endSchema = fullPath.indexOf(SCHEMA_SEPARATOR);
            this.schema = fullPath.substring(0, endSchema);
            parsedIndex = endSchema + SCHEMA_SEPARATOR.length();
        } else {
            this.schema = "";
        }

        // Get hostname
        if (fullPath.contains(HOSTNAME_SEPARATOR)) {
            int endHostname = fullPath.indexOf(HOSTNAME_SEPARATOR);
            this.hostname = fullPath.substring(parsedIndex, endHostname);
            parsedIndex = endHostname + HOSTNAME_SEPARATOR.length();
        } else {
            this.hostname = "";
        }

        // Get Path
        this.path = fullPath.substring(parsedIndex);
    }

    /**
     * Returns the parsed schema.
     * 
     * @return The parsed schema.
     */
    public String getSchema() {
        String parsedSchema = "";
        if (!this.schema.isEmpty()) {
            if (this.schema.endsWith(SCHEMA_SEPARATOR)) {
                parsedSchema = this.schema;
            } else {
                parsedSchema = this.schema + SCHEMA_SEPARATOR;
            }
        }

        return parsedSchema;
    }

    /**
     * Returns the host name.
     * 
     * @return The URI host name.
     */
    public String getHost() {
        return this.hostname;
    }

    /**
     * Returns the path.
     * 
     * @return The URI path.
     */
    public String getPath() {
        return this.path;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (!this.schema.isEmpty()) {
            sb.append(this.schema).append(SCHEMA_SEPARATOR);
        }

        if (!this.hostname.isEmpty()) {
            sb.append(this.hostname).append(HOSTNAME_SEPARATOR);
        }

        sb.append(this.path);

        return sb.toString();
    }

    /**
     * Extended print version for debugging purposes.
     * 
     * @param ps Print stream to print the debug information
     */
    public void debugPrint(PrintStream ps) {
        ps.println("------------------ URI ----------------------------");
        ps.println("SCHEMA:   " + this.schema);
        ps.println("HOSTNAME: " + this.hostname);
        ps.println("PATH:     " + this.path);
        ps.println("FROM: ");
        for (StackTraceElement ste : Thread.currentThread().getStackTrace()) {
            ps.println(ste);
        }
        ps.println("---------------------------------------------------");
    }

}
