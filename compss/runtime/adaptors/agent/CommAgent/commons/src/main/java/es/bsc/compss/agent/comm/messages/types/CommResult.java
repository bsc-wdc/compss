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
package es.bsc.compss.agent.comm.messages.types;

import es.bsc.compss.agent.types.ApplicationResult;
import es.bsc.compss.agent.types.RemoteDataInformation;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.nio.NIOResult;
import es.bsc.compss.nio.NIOUri;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.jaxb.ResourcesPropertyAdaptorType;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;


public class CommResult extends NIOResult implements ApplicationResult {

    private Collection<RemoteDataLocation> sources = new LinkedList<>();


    public CommResult() {
        super();
    }

    public CommResult(NIOUri uri) {
        super(uri);
    }

    public void setRemoteData(Collection<RemoteDataLocation> sources) {
        this.sources = sources;
    }

    @Override
    public Collection<RemoteDataLocation> getLocations() {
        return this.sources;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(sources);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        sources = (Collection<RemoteDataLocation>) in.readObject();
    }

    protected void dumpContent(StringBuilder sb) {
        sb.append("\"remote_data\":[");
        if (this.sources != null) {
            Iterator<RemoteDataLocation> itr = this.sources.iterator();
            if (itr.hasNext()) {
                RemoteDataLocation loc = itr.next();
                sb.append(loc);
                while (itr.hasNext()) {
                    loc = itr.next();
                    sb.append("," + loc);
                }
            }
        }
        sb.append("],");
        super.dumpContent(sb);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        dumpContent(sb);
        sb.append("}");
        return sb.toString();
    }
}
