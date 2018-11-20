/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.nio.commands;

import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOURI;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.uri.MultiURI;

import es.bsc.compss.types.execution.Data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;


public class NIOData extends Data<NIOURI> implements Externalizable {

    public NIOData() {
        super();
    }

    public NIOData(String name, NIOURI uri) {
        super(name, uri);
    }

    public NIOData(LogicalData ld) {
        super(ld.getName());
        LinkedList<NIOURI> sources = this.getSources();
        for (MultiURI uri : ld.getURIs()) {
            try {
                Object o = uri.getInternalURI(NIOAgent.ID);
                if (o != null) {
                    sources.add((NIOURI) o);
                }
            } catch (UnstartedNodeException une) {
                // Ignore internal URI.
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
    }

}
