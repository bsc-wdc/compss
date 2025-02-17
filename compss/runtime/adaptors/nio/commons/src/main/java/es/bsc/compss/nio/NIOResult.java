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

import es.bsc.compss.types.data.location.ProtocolType;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;


/**
 * Representation of the result of a data parameter for the NIO Adaptor.
 */
public class NIOResult implements Externalizable {

    private Collection<NIOUri> uris;


    /**
     * Creates a new NIOResult instance for externalization.
     */
    public NIOResult() {
        this.uris = new LinkedList<>();
    }

    /**
     * Creates a new NIOResult instance.
     *
     * @param location location that will be set as path in the uri where the result is available
     */
    public NIOResult(String location) {
        this();
        if (location != null) {
            addLocation(location);
        }
    }

    /**
     * Creates a new NIOResult instance.
     *
     * @param uri uri where the result is available
     */
    public NIOResult(NIOUri uri) {
        this();
        if (uri != null) {
            this.uris.add(uri);
        }
    }

    public void addUri(NIOUri uri) {
        this.uris.add(uri);
    }

    public void addLocation(String location) {
        NIOUri uri = new NIOUri(null, location, ProtocolType.ANY_URI);
        this.addUri(uri);
    }

    public Collection<NIOUri> getUris() {
        return uris;
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeObject(uris);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        uris = (Collection<NIOUri>) oi.readObject();
    }

    protected void dumpContent(StringBuilder sb) {
        sb.append("\"uris\":[");
        Iterator<NIOUri> itr = this.uris.iterator();
        if (itr.hasNext()) {
            NIOUri uri = itr.next();
            sb.append(uri);
            while (itr.hasNext()) {
                uri = itr.next();
                sb.append("," + uri);
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
