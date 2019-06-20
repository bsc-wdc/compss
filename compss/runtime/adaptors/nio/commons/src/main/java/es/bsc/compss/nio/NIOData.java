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
package es.bsc.compss.nio;

import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.execution.Data;
import es.bsc.compss.types.uri.MultiURI;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;


public class NIOData extends Data<NIOUri> implements Externalizable {

    /**
     * Creates a NIOData for externalization.
     */
    public NIOData() {
        super();
    }

    /**
     * Creates a new NIOData instance.
     * 
     * @param name Data name.
     * @param uri Data URI.
     */
    public NIOData(String name, NIOUri uri) {
        super(name, uri);
    }

    /**
     * Creates a new NIOData instance from the given LogicalData.
     * 
     * @param ld LogicalData containing the data information.
     */
    public NIOData(LogicalData ld) {
        super(ld.getName());

        LinkedList<NIOUri> sources = this.getSources();
        for (MultiURI uri : ld.getURIs()) {
            try {
                Object o = uri.getInternalURI(NIOAgent.ID);
                if (o != null) {
                    sources.add((NIOUri) o);
                }
            } catch (UnstartedNodeException une) {
                // Ignore internal URI.
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

}