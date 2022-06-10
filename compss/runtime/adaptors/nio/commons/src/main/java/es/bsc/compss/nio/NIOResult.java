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

import es.bsc.compss.types.annotations.parameter.DataType;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Representation of the reesult of a data parameter for the NIO Adaptor.
 */
public class NIOResult implements Externalizable {

    private DataType type;
    private String location;


    /**
     * Creates a new NIOResult instance for externalization.
     */
    public NIOResult() {
    }

    public NIOResult(DataType type, String location) {
        this.type = type;
        this.location = location;
    }

    public DataType getType() {
        return type;
    }

    public String getLocation() {
        return location;
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeObject(type);
        oo.writeObject(location);
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        type = (DataType) oi.readObject();
        location = (String) oi.readObject();
    }

    @Override
    public String toString() {
        return "[TYPE=" + type + " LOCATION=" + location + "]";
    }

}
