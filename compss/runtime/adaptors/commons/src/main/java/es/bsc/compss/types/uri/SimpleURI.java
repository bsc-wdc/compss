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
package es.bsc.compss.types.uri;

import java.io.PrintStream;


public class SimpleURI {

    private static final String SCHEMA_SEPARATOR = "://";
    private static final String HOSTNAME_SEPARATOR = "@";

    private final String schema;
    private final String hostname;
    private final String path;


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

        // Get hostname
        this.path = fullPath.substring(parsedIndex);
    }

    public String getSchema() {
        return (this.schema.isEmpty() ? "" : this.schema + SCHEMA_SEPARATOR);
    }

    public String getHost() {
        return this.hostname;
    }

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
