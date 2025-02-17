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

import es.bsc.compss.agent.types.ApplicationParameterCollection;
import es.bsc.compss.nio.NIOParam;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;
import java.util.List;


public class CommParamCollection extends CommParam
    implements ApplicationParameterCollection<CommParam>, Externalizable {

    private List<CommParam> collectionParameters;


    /**
     * Create a new NIOParamCollection instance for externalization.
     */
    public CommParamCollection() {
        // Only executed by externalizable
        super();
    }

    /**
     * Create a new NIOParamCollection copying the given NIOParam values.
     *
     * @param p NIOParam to copy.
     */
    public CommParamCollection(CommParam p) {
        super(p);
        // Empty attributes
        this.collectionParameters = new LinkedList<>();
    }

    /**
     * Returns the number of internal parameters of the collection.
     *
     * @return The number of internal parameters of the collection.
     */
    public int getSize() {
        return this.collectionParameters.size();
    }

    /**
     * Returns a list of objects containing the collection parameters.
     *
     * @return A list of objects containing the collection parameters.
     */
    public List<CommParam> getCollectionParameters() {
        return this.collectionParameters;
    }

    /**
     * Adds a new parameter to the collection.
     *
     * @param p Parameter to add.
     */
    public void addParameter(CommParam p) {
        this.collectionParameters.add(p);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.collectionParameters = new LinkedList<>();
        int numParameters = in.readInt();
        for (int i = 0; i < numParameters; ++i) {
            this.collectionParameters.add((CommParam) in.readObject());
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeInt(this.collectionParameters.size());
        for (NIOParam subParam : this.collectionParameters) {
            // Note that this implementation also implicitly supports nesting
            out.writeObject(subParam);
        }
    }

    /**
     * Dumps the internal information into the given StringBuilder.
     *
     * @param sb StringBuilder where to dump the internal information.
     */
    protected void dumpInternalInfo(StringBuilder sb) {
        sb.append("\"elements\":[");
        for (CommParam p : this.collectionParameters) {
            sb.append(p.toString() + ",");
        }
        sb.append("],");
        super.dumpInternalInfo(sb);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("{");
        dumpInternalInfo(sb);
        sb.append("}");
        return sb.toString();
    }

}
