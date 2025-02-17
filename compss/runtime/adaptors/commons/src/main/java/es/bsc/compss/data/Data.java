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
package es.bsc.compss.data;

import es.bsc.compss.types.execution.InvocationParamURI;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;


public abstract class Data<T extends InvocationParamURI> implements Externalizable {

    // Name of the data to send
    private String dataMgmtId;
    // Sources list
    private LinkedList<T> sources;


    public Data() {
        this.dataMgmtId = "";
        this.sources = new LinkedList<>();
    }

    public Data(String dataMgmtId) {
        this.dataMgmtId = dataMgmtId;
        this.sources = new LinkedList<>();
    }

    /**
     * Data constructor.
     *
     * @param name Name/identifier
     * @param uri Initial data source URI
     */
    public Data(String name, T uri) {
        this.dataMgmtId = name;
        this.sources = new LinkedList<>();
        sources.add(uri);
    }

    public String getDataMgmtId() {
        return dataMgmtId;
    }

    public void addSource(T uri) {
        sources.add(uri);
    }

    public LinkedList<T> getSources() {
        return sources;
    }

    /**
     * Get first URI.
     *
     * @return
     */
    public T getFirstURI() {
        if (sources != null && !sources.isEmpty()) {
            return sources.getFirst();
        }
        return null;
    }

    // Returns true if the dataMgmtId of the data is the same
    // Returns false otherwise
    public boolean compareTo(Data<T> n) {
        return n.dataMgmtId.compareTo(dataMgmtId) == 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        dataMgmtId = in.readUTF();
        sources = (LinkedList<T>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(dataMgmtId);
        out.writeObject(sources);
    }

    @Override
    public String toString() {
        return dataMgmtId + "@" + sources;
    }
}
