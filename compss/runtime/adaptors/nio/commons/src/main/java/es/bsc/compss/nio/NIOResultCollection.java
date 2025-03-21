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

package es.bsc.compss.nio;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Iterator;
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

    public NIOResultCollection(List<NIOResult> elements) {
        this();
        this.elements = elements;
    }

    public NIOResultCollection(String location, List<NIOResult> elements) {
        super(location);
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

    protected void dumpContent(StringBuilder sb) {
        super.dumpContent(sb);
        sb.append(",\"elements\":[");
        Iterator<NIOResult> itr = this.elements.iterator();
        if (itr.hasNext()) {
            NIOResult nr = itr.next();
            sb.append(nr);
            while (itr.hasNext()) {
                nr = itr.next();
                sb.append("," + nr);
            }
        }
        sb.append("]");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        dumpContent(sb);
        sb.append("}");
        return sb.toString();
    }

}
