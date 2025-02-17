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
package es.bsc.compss.agent.rest.types;

public class RESTResult {

    private String[] locations;


    public RESTResult() {
        this.locations = new String[0];
    }

    public RESTResult(String[] locations) {
        this.locations = locations;
    }

    /**
     * Adds a new location where the result of the parameter can be found.
     *
     * @param location where the result of the parameter can be found.
     */
    public void addLocation(String location) {
        int currentSize = locations.length;
        String[] newLocs = new String[currentSize + 1];
        newLocs[currentSize] = location;
        System.arraycopy(this.locations, 0, newLocs, 0, currentSize);
        this.locations = newLocs;
    }

    public String[] getLocations() {
        return locations;
    }

    public void setLocations(String[] locations) {
        this.locations = locations;
    }

}
