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

import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.api.TaskMonitor.CollectionTaskResult;
import es.bsc.compss.api.TaskMonitor.TaskResult;
import es.bsc.compss.types.annotations.parameter.DataType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.List;


/**
 * Extension of the NIOResult class to handle collection types. Basically, a NIOResult plus a list of NIOResult
 * representing the contents of the collection.
 *
 * @see NIOResult
 */
public class NIOResultCollection extends NIOResult implements Externalizable {

    private List<NIOResult> elements;


    /**
     * Create a new NIOResultCollection instance for externalization.
     */
    public NIOResultCollection() {
        // Only executed by externalizable
        super();
    }

    /**
     * Create a new NIOResultCollection instance from a object array following TaskMonitor indexes.
     */
    public NIOResultCollection(CollectionTaskResult param) {
        super(param.getType(), param.getDataLocation());
        this.elements = new ArrayList<>();
        for (TaskResult subParam : param.getSubelements()) {
            if (subParam.getType() == DataType.COLLECTION_T) {
                this.elements.add(new NIOResultCollection((CollectionTaskResult) subParam));
            } else {
                this.elements.add(new NIOResult(subParam.getType(), subParam.getDataLocation()));
            }
        }
    }

    public NIOResultCollection(DataType type, String location, List<NIOResult> elements) {
        super(type, location);
        this.elements = elements;
    }

    public List<NIOResult> getElements() {
        return elements;
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        super.writeExternal(oo);
        oo.writeObject(elements);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        super.readExternal(oi);
        this.elements = (List<NIOResult>) oi.readObject();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("[COLLECTION");
        sb.append(" TYPE=").append(getType());
        sb.append(" LOCATION=").append(getLocation());
        sb.append(" ELEMENTS=[");
        boolean many = false;
        for (NIOResult nr : this.elements) {
            if (many) {
                sb.append(", ");
            }
            sb.append(nr.toString());
            many = true;
        }
        sb.append("]");
        sb.append("]");
        return sb.toString();
    }

}
