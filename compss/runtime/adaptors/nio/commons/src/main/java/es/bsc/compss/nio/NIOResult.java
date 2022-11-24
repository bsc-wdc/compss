/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.nio;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.LinkedList;


/**
 * Representation of the result of a data parameter for the NIO Adaptor.
 */
public class NIOResult implements Externalizable {

    private Collection<String> locations;


    /**
     * Creates a new NIOResult instance for externalization.
     */
    public NIOResult() {
        this.locations = new LinkedList<>();
    }

    /**
     * Creates a new NIOResult instance.
     * 
     * @param location location where the result is available
     */
    public NIOResult(String location) {
        this.locations = new LinkedList<>();
        if (location != null) {
            this.locations.add(location);
        }
    }

    public void addLocation(String location) {
        this.locations.add(location);
    }

    public Collection<String> getLocations() {
        return locations;
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeObject(locations);
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        locations = (Collection<String>) oi.readObject();
    }

    @Override
    public String toString() {
        return "[LOCATIONS=" + locations + "]";
    }

}
