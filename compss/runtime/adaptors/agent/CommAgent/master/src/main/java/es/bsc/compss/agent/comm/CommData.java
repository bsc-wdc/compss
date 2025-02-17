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
package es.bsc.compss.agent.comm;

import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.nio.NIOData;

import java.util.LinkedList;
import java.util.List;


public class CommData extends NIOData {

    private final List<RemoteDataLocation> locations;


    /**
     * Creates a new CommData instance.
     *
     * @param name Data name.
     */
    public CommData(String name) {
        super(name);
        locations = new LinkedList<>();
    }

    /**
     * Adds a new Remote data location to the data.
     * 
     * @param rdl location to be added to the data
     */
    public void addRemoteLocation(RemoteDataLocation rdl) {
        locations.add(rdl);
    }

    /**
     * Returns a list with all the remote location of the data.
     * 
     * @return list of all the remote locations of the data.
     */
    public List<RemoteDataLocation> getRemoteLocations() {
        return locations;
    }
}
