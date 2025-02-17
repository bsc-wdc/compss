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
package es.bsc.compss.agent.types;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;


/**
 * Container for all the required information for an Agent to use a remote data value.
 */
public class RemoteDataInformation implements Externalizable {

    private String renaming;
    private Collection<RemoteDataLocation> sources = new LinkedList<>();


    public RemoteDataInformation() {
    }

    public RemoteDataInformation(String renaming) {
        this.renaming = renaming;
    }

    public String getRenaming() {
        return this.renaming;
    }

    public void addSource(RemoteDataLocation location) {
        this.sources.add(location);
    }

    public Collection<RemoteDataLocation> getSources() {
        return this.sources;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{" + "\"renaming\":\"" + renaming + "\"");
        sb.append(",\"sources\":[");
        Iterator<RemoteDataLocation> itr = this.sources.iterator();
        if (itr.hasNext()) {
            RemoteDataLocation source = itr.next();
            sb.append(source);
            while (itr.hasNext()) {
                source = itr.next();
                sb.append("," + source);
            }
        }
        sb.append(" ]" + "}");
        return sb.toString();
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeUTF(this.renaming);
        oo.writeObject(this.sources);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        this.renaming = oi.readUTF();
        this.sources = (Collection<RemoteDataLocation>) oi.readObject();
    }

}
